from __future__ import annotations
import io
import time
import requests
import pandas as pd
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================
BASE_URL = "https://sih.conagua.gob.mx/basedatos/Climas"
CATALOG_URL = f"{BASE_URL}/0_Catalogo_de_estaciones_climatologicas.xls"

MAX_RETRIES = 5
TIMEOUT_SEC = 120

# ==========================================
# HTTP SESSION
# ==========================================
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/vnd.ms-excel,application/octet-stream,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.8,en;q=0.5",
    "Referer": "https://sih.conagua.gob.mx/",
    "Connection": "keep-alive",
})

# ==========================================
# CATALOG FUNCTIONS
# ==========================================
def fetch_catalog() -> pd.DataFrame | None:
    """
    Downloads the main climatological catalog as a DataFrame.
    Adds robust error handling for HTML error responses or legacy Excel formats.
    """
    print(f"Downloading main station catalog from {CATALOG_URL}...")
    try:
        resp = session.get(CATALOG_URL, timeout=TIMEOUT_SEC)
        resp.raise_for_status()

        if "text/html" in resp.headers.get("Content-Type", ""):
            print("Server returned HTML instead of an Excel file (catalog missing or endpoint changed).")
            return None

        try:
            df = pd.read_excel(io.BytesIO(resp.content))
        except Exception:
            print("Pandas had trouble reading the Excel file. Trying with openpyxl engine...")
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")

        df.columns = [str(c).strip() for c in df.columns]
        print("Catalog downloaded and read successfully.")
        return df

    except requests.RequestException as e:
        print(f"FATAL ERROR: Could not download catalog (network issue): {e}")
        return None
    except Exception as e:
        print(f"FATAL ERROR: Could not read catalog: {e}")
        return None


def filter_catalog(df: pd.DataFrame, estado_filters: list[str]) -> list[str]:
    """
    Filters the catalog DataFrame based on user's list of states.
    Returns a list of station keys ('claves').
    """
    try:
        clave_col = next(c for c in df.columns if "clave" in c.lower())
        estado_col = next(c for c in df.columns if "estado" in c.lower())
    except StopIteration:
        print("FATAL ERROR: Could not find 'clave' or 'estado' columns in catalog file.")
        print(f"Available columns: {df.columns.tolist()}")
        return []

    total_stations = len(df)
    print(f"Total stations found in catalog: {total_stations}")

    if not estado_filters:
        print("No filter applied (downloading ALL stations).")
        claves = df[clave_col].astype(str).str.strip().dropna().unique().tolist()
    else:
        df["estado_norm"] = df[estado_col].astype(str).str.strip().str.lower()
        mask = df["estado_norm"].isin(estado_filters)
        filtered_df = df[mask]
        print(f"Found {len(filtered_df)} stations matching filters.")
        claves = filtered_df[clave_col].astype(str).str.strip().dropna().unique().tolist()

    return claves


# ==========================================
# DOWNLOAD FUNCTION
# ==========================================
def download_station_csv(clave: str, out_dir: Path) -> str:
    """
    Downloads the CSV for one station and saves it locally.
    Includes retry and error handling for missing files.
    """
    csv_url = f"{BASE_URL}/{clave}.csv"
    out_path = out_dir / f"{clave}.csv"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(csv_url, timeout=TIMEOUT_SEC)

            if r.status_code == 404:
                return f"Not found: {clave}.csv"

            if "text/html" in r.headers.get("Content-Type", ""):
                return f"{clave}.csv returned HTML page instead of CSV (probably missing)."

            r.raise_for_status()
            with open(out_path, "wb") as f:
                f.write(r.content)
            return f"Saved {out_path.name}"

        except requests.RequestException as e:
            print(f"  > Attempt {attempt} failed: {e}")
            if attempt == MAX_RETRIES:
                return f"Failed {clave} after {MAX_RETRIES} retries."
            time.sleep(2 ** attempt)

        except Exception as e:
            return f"Failed {clave} (File Error): {e}"

    return f"Failed {clave} after {MAX_RETRIES} retries."


# ==========================================
# MAIN
# ==========================================
def main() -> None:
    print("=== CONAGUA Climatological Station Downloader (Enhanced) ===")

    user_folder = input("Output Folder Path: ").strip()
    if not user_folder:
        print("No output folder provided. Exiting.")
        return

    OUT_DIR = Path(user_folder)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    user_input = input("Estados (comma separated or 'all'): ").strip()
    if not user_input:
        print("No input provided. Exiting.")
        return

    if user_input.lower() == "all":
        estado_filters = []
        print("Mode: Download ALL stations")
    else:
        estado_filters = [e.strip().lower() for e in user_input.split(",")]
        print(f"Filtering for: {', '.join(estado_filters)}")

    catalog_df = fetch_catalog()
    if catalog_df is None:
        print("Catalog download failed. Exiting.")
        return

    claves_to_download = filter_catalog(catalog_df, estado_filters)
    if not claves_to_download:
        print("No stations found matching criteria. Exiting.")
        return

    print(f"\nFound {len(claves_to_download)} stations to download.")
    confirm = input("Proceed? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("Cancelled by user.")
        return

    results = []
    for i, clave in enumerate(claves_to_download, 1):
        print(f"--- [{i}/{len(claves_to_download)}] Downloading {clave}.csv ---")
        msg = download_station_csv(clave, OUT_DIR)
        print(" ->", msg)
        results.append(msg)

    ok = [r for r in results if "Saved" in r]
    fail = [r for r in results if "Failed" in r or "Not found" in r]

    print("\n=== DOWNLOAD SUMMARY ===")
    print(f"Successful: {len(ok)}")
    print(f"Failed/Missing: {len(fail)}")
    if fail:
        print("\n--- Failures ---")
        for f in fail:
            print(f)

    print(f"\nAll files saved to: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
