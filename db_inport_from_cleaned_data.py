# -*- coding: utf-8 -*-
"""
ETL script to import CONAGUA-style Excel weather station data into PostgreSQL.
- Reads all Excel files from ./output_cleaned_data/
- Detects the data header row dynamically (row containing a 'fecha' column)
- Enriches station metadata from weather_stations_meta_data.csv
- Inserts all data into PostgreSQL 'manzanillo'
- Logs progress to 'etl_import.log'
"""

import os
import time
import logging
import unicodedata
import re
import pandas as pd
from typing import Dict, Any, cast, Optional, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ==========================================
# CONFIGURATION
# ==========================================
DATA_FOLDER = "./output_cleaned_data"
META_FILE = "./weather_stations_meta_data.csv"

HOST = "localhost"
PORT = 5432
USER = "postgres"
PASSWORD = "super"
NEW_DB_NAME = "manzanillo"

DB_URI = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NEW_DB_NAME}"
LOG_FILE = "etl_import.log"

# ==========================================
# LOGGING SETUP
# ==========================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

logging.info("===========================================")
logging.info("Starting ETL: Import weather data to PostgreSQL")
logging.info("===========================================")

# ==========================================
# NORMALIZATION HELPERS
# ==========================================
_whitespace_re = re.compile(r"\s+")
def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def norm_text(s: str) -> str:
    """Lowercase, remove accents, trim, collapse spaces."""
    s2 = strip_accents(s).lower().strip()
    s2 = _whitespace_re.sub(" ", s2)
    return s2

def norm_columns(cols: List[Any]) -> List[str]:
    out: List[str] = []
    for c in cols:
        name = "" if pd.isna(c) else str(c)
        out.append(norm_text(name))
    return out

# ==========================================
# HEADER / COLUMN DETECTION
# ==========================================
def find_header_row(file_path: str, max_scan_rows: int = 100) -> Optional[int]:
    """Scan the top of the sheet to find a row that looks like a header (contains a 'fecha' column)."""
    try:
        preview = pd.read_excel(file_path, nrows=max_scan_rows, header=None)
    except Exception as e:
        logging.error(f"Error reading preview {file_path}: {e}")
        return None

    # Check each row as potential header
    for i in range(len(preview)):
        row_vals = preview.iloc[i].tolist()
        row_norm = norm_columns(row_vals)
        # Heuristic: a header row contains something that starts with 'fecha'
        if any(col.startswith("fecha") for col in row_norm if isinstance(col, str)):
            return i
    return None

def pick_columns(df: pd.DataFrame) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Return (date_col, precipitation_col, temperature_col) using normalized names.
    """
    original_cols = list(df.columns)
    normalized = norm_columns(original_cols)

    # Map normalized -> original for selection
    norm_to_orig: Dict[str, str] = {}
    for orig, normd in zip(original_cols, normalized):
        norm_to_orig[normd] = orig

    # Date column: any column whose normalized name starts with 'fecha'
    date_norm = next((c for c in normalized if isinstance(c, str) and c.startswith("fecha")), None)
    if not date_norm:
        raise ValueError("No date column found (name starting with 'fecha').")

    # Value columns (optional)
    # Precipitation: contains 'precip'
    precip_norm = next((c for c in normalized if "precip" in c), None)
    # Temperature (max): contains 'tempmax' or 'tmax'
    temp_norm = next((c for c in normalized if "tempmax" in c or re.search(r"\btmax\b", c) is not None), None)

    date_col = norm_to_orig[date_norm]
    precip_col = norm_to_orig[precip_norm] if precip_norm else None
    temp_col = norm_to_orig[temp_norm] if temp_norm else None

    return date_col, precip_col, temp_col

# ==========================================
# HELPER: Load Excel File
# ==========================================
def load_weather_excel(file_path: str) -> pd.DataFrame:
    """
    Detect header row dynamically and parse weather data section.
    Returns a DataFrame with at least a valid date column and any found numeric columns.
    """
    header_row = find_header_row(file_path, max_scan_rows=100)
    if header_row is None:
        logging.warning(f"No header row found in {file_path}. Skipping.")
        return pd.DataFrame()

    # Read with the detected header row
    try:
        df = pd.read_excel(file_path, header=header_row)
    except Exception as e:
        logging.error(f"Error reading {file_path} at header {header_row}: {e}")
        return pd.DataFrame()

    # Log detected header and columns
    logging.info(f"Detected header row {header_row} in {os.path.basename(file_path)}")
    logging.info(f"Columns: {list(df.columns)}")

    try:
        date_col, precip_col, temp_col = pick_columns(df)
    except ValueError as e:
        logging.warning(f"{e} in {file_path}. Skipping.")
        return pd.DataFrame()

    # Build a minimal working frame with standard names
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)

    if precip_col:
        out["precipitation"] = pd.to_numeric(df[precip_col], errors="coerce")
    else:
        out["precipitation"] = None

    if temp_col:
        out["temperature"] = pd.to_numeric(df[temp_col], errors="coerce")
    else:
        out["temperature"] = None

    out = out.dropna(subset=["date"])
    return out

# ==========================================
# TEST DATABASE CONNECTION
# ==========================================
logging.info("Testing database connection...")

try:
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version();")).scalar()
        logging.info(f"Connected successfully to PostgreSQL:\n{version}")
except OperationalError as e:
    logging.error("Could not connect to the database. Please check credentials or network.")
    logging.error(f"Error details: {e}")
    raise SystemExit(1)

# ==========================================
# LOAD METADATA
# ==========================================
logging.info("Loading station metadata...")

meta = pd.read_csv(META_FILE)
meta = meta.rename(columns={
    "clave": "station_code",
    "nombre": "name",
    "estado": "state",
    "municipio": "municipality",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
    "cuenca_de_disponibilidad": "cuenca",
    "region_hidrologica": "region_hidrologica"
})

logging.info(f"Loaded {len(meta)} metadata records.")

# ==========================================
# PROCESS EXCEL FILES
# ==========================================
files = sorted([f for f in os.listdir(DATA_FOLDER) if f.lower().endswith(".xlsx")])

if not files:
    logging.warning("No Excel files found in data folder.")
    raise SystemExit(0)

for file in files:
    start_time = time.time()
    file_path = os.path.join(DATA_FOLDER, file)
    logging.info(f"Processing: {file_path}")

    df = load_weather_excel(file_path)
    if df.empty:
        logging.warning(f"No valid data in {file}")
        continue

    # Station code from filename (cleaned_XXXXXX.xlsx -> XXXXXX)
    station_code = os.path.splitext(file)[0]
    station_code = re.sub(r"^cleaned[_-]?", "", station_code, flags=re.IGNORECASE).strip()

    # Metadata lookup
    meta_row = meta[meta["station_code"].astype(str).str.upper() == station_code.upper()]
    if not meta_row.empty:
        raw_dict = meta_row.to_dict(orient="records")[0]
        station_data: Dict[str, Any] = cast(Dict[str, Any], raw_dict)
    else:
        station_data = {
            "station_code": station_code,
            "name": None, "state": None, "municipality": None,
            "latitude": None, "longitude": None, "altitude": None,
            "cuenca": None, "region_hidrologica": None
        }

    # Upsert station
    with engine.begin() as conn:
        stmt = text("""
            INSERT INTO stations (station_code, name, state, municipality,
                                  latitude, longitude, altitude, cuenca, region_hidrologica)
            VALUES (:station_code, :name, :state, :municipality,
                    :latitude, :longitude, :altitude, :cuenca, :region_hidrologica)
            ON CONFLICT (station_code) DO UPDATE SET
                name = EXCLUDED.name,
                state = EXCLUDED.state,
                municipality = EXCLUDED.municipality,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                altitude = EXCLUDED.altitude,
                cuenca = EXCLUDED.cuenca,
                region_hidrologica = EXCLUDED.region_hidrologica;
        """)
        conn.execute(stmt, station_data)

    # Resolve station id
    with engine.connect() as conn:
        sid_stmt = text("SELECT id FROM stations WHERE station_code = :code")
        station_id = conn.execute(sid_stmt, {"code": station_code}).scalar()

    if not station_id:
        logging.warning(f"Could not find ID for station {station_code}. Skipping.")
        continue

    # Prepare final frame for DB insert (df already has date/precipitation/temperature)
    df_final = pd.DataFrame({
        "station_id": station_id,
        "date": df["date"],
        "precipitation": df["precipitation"],
        "temperature": df["temperature"],
    }).dropna(subset=["date"])

    if df_final.empty:
        logging.warning(f"No valid measurements for station {station_code}.")
        continue

    df_final.to_sql("measurements", engine, if_exists="append", index=False)

    elapsed = time.time() - start_time
    logging.info(
        f"Imported {len(df_final)} records for station {station_code} in {elapsed:.2f} seconds"
    )

logging.info("===========================================")
logging.info("All Excel weather data imported successfully into PostgreSQL.")
logging.info("===========================================")
