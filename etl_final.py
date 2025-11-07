"""
Clean weather-station CSV files from Colima, Mexico.

Features:
- Prompts user for input and output folders.
- Skips first 6 metadata lines.
- Normalizes headers and encoding artifacts (Â, Ã, etc.).
- Adds missing columns as NaN.
- Strips unprintable / emoji characters.
- Writes one cleaned .xlsx per station file (UTF-8 safe for Excel).
"""

from __future__ import annotations
import re
import numpy as np
import pandas as pd
from pathlib import Path
from pandas import DataFrame

# --------------------------------------------------------------------
# CONFIGURATION CONSTANTS
# --------------------------------------------------------------------

# Canonical column order
REQUIRED = [
    "Estacion",
    "Fecha",
    "Precipitacion(mm)",
    "TempMax(°C)",
    "TempMin(°C)",
    "TempAmb(°C)",
    "Evaporacion(mm)",
    "Pres Barometric(g/cm2)",
    "Hum Relativa(%)",
]

# --------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalize text (remove accents, mojibake, invisible chars)."""
    if s is None:
        return ""
    s = str(s)
    reps = {
        "Â": "", "º": "°",
        "Ã¡": "a", "Ã©": "e", "Ã­": "i", "Ã³": "o", "Ãº": "u", "Ã±": "n",
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "²": "2", "Ã": "", "\u200b": ""
    }
    for a, b in reps.items():
        s = s.replace(a, b)
    return s.strip().lower()


HEADER_MAP = {
    "estacion": "Estacion", "estación": "Estacion",
    "fecha": "Fecha",
    "precipitacion(mm)": "Precipitacion(mm)",
    "precipitacion (mm)": "Precipitacion(mm)",
    "precipitaciÃ³n(mm)": "Precipitacion(mm)",
    "tempmax(°c)": "TempMax(°C)", "temperatura maxima(°c)": "TempMax(°C)",
    "tempmin(°c)": "TempMin(°C)", "temperatura minima(°c)": "TempMin(°C)",
    "tempamb(°c)": "TempAmb(°C)", "temperatura media(°c)": "TempAmb(°C)",
    "evaporacion(mm)": "Evaporacion(mm)", "evaporación(mm)": "Evaporacion(mm)",
    "pres barometric(g/cm2)": "Pres Barometric(g/cm2)",
    "pres barometric(g/cm²)": "Pres Barometric(g/cm2)",
    "hum relativa(%)": "Hum Relativa(%)", "humedad relativa(%)": "Hum Relativa(%)",
}


def canonicalize_headers(df: DataFrame) -> DataFrame:
    """Standardize column names to required schema."""
    new_cols = {}
    for c in df.columns:
        key = norm(c)
        key = key.replace("máxima", "maxima").replace("mínima", "minima")
        canon = (
            HEADER_MAP.get(key)
            or ("TempMax(°C)" if ("max" in key and "temp" in key) else
                "TempMin(°C)" if ("min" in key and "temp" in key) else
                "TempAmb(°C)" if (("media" in key or "amb" in key) and "temp" in key) else
                "Precipitacion(mm)" if "precip" in key else
                "Evaporacion(mm)" if "evapora" in key else
                "Hum Relativa(%)" if ("hum" in key and "%" in key) else
                "Pres Barometric(g/cm2)" if ("pres" in key and ("cm2" in key or "cm²" in key or "baro" in key)) else
                "Fecha" if "fecha" in key else
                "Estacion" if ("estacion" in key or "estación" in key) else c)
        )
        new_cols[c] = canon
    return df.rename(columns=new_cols)


def extract_station_code_from_meta(lines: list[str]) -> str | None:
    """Extract 'Clave: XXXX' from the metadata section."""
    for ln in lines[:8]:
        m = re.search(r"Clave:\s*([A-Za-z0-9_-]+)", ln)
        if m:
            return m.group(1).strip()
    return None


def strip_non_ascii(df: pd.DataFrame) -> pd.DataFrame:
    """Remove unprintable / exotic Unicode characters to avoid encoding errors."""
    return df.replace(r"[^\x00-\x7F]+", "", regex=True)

# --------------------------------------------------------------------
# Core Cleaning Logic
# --------------------------------------------------------------------

def clean_file(path: Path) -> DataFrame:
    """Read and clean a single weather-station CSV file."""
    with open(path, "r", encoding="latin-1") as f:
        meta = [next(f).rstrip("\n\r") for _ in range(6)]

    station_code = extract_station_code_from_meta(meta)

    df = pd.read_csv(path, encoding="latin-1", skiprows=6, engine="python")
    df = canonicalize_headers(df)

    # Replace blanks/dashes with NaN
    df = df.replace(r"^\s*$", np.nan, regex=True).replace("-", np.nan)

    # Fill or create Estacion column
    if "Estacion" not in df.columns:
        df["Estacion"] = station_code
    elif station_code:
        df["Estacion"] = df["Estacion"].fillna(station_code).replace("", station_code)

    # Parse dates safely
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date

    # Ensure all required columns exist
    for col in REQUIRED:
        if col not in df.columns:
            df[col] = np.nan

    # Reorder columns
    df = df.reindex(columns=REQUIRED)

    # Clean up stray characters
    df = strip_non_ascii(df)

    return df

# --------------------------------------------------------------------
# Main Driver
# --------------------------------------------------------------------

def main() -> None:
    print("\nWeather Station Cleaner")
    input_path_str = input("Enter the folder path containing raw CSV files: ").strip()
    output_path_str = input("Enter the folder path to save cleaned Excel files: ").strip()

    input_folder = Path(input_path_str)
    output_folder = Path(output_path_str)
    output_folder.mkdir(exist_ok=True)

    if not input_folder.exists():
        print(f"Error: The folder '{input_folder}' does not exist.")
        return

    files = list(input_folder.glob("*.csv"))
    total_files = len(files)

    if total_files == 0:
        print("No CSV files found in the input folder.")
        return

    print(f"\nFound {total_files} CSV files to clean.\n")

    for i, file in enumerate(files, start=1):
        print(f"Processing file {i} of {total_files}: {file.name}")
        try:
            cleaned = clean_file(file)
            out_path = output_folder / f"cleaned_{file.stem}.xlsx"
            cleaned.to_excel(out_path, index=False, engine="openpyxl")
            print(f"Saved: {out_path.name}\n")
        except Exception as e:
            print(f"Error cleaning {file.name}: {e}\n")

    print("All files processed successfully.")

# --------------------------------------------------------------------

if __name__ == "__main__":
    main()
