from __future__ import annotations
import io
import time
import random
import requests
import pandas as pd
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================
BASE_URL = "https://sih.conagua.gob.mx/basedatos/Hidros"
CATALOG_URL = f"{BASE_URL}/0_Catalogo%20de%20estaciones%20hidrometricas.xls"

# --- REMOVED ---
# OUT_DIR is now requested from the user in main()
# --- END REMOVED ---

MAX_RETRIES = 3
TIMEOUT_SEC = 60

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.8,en;q=0.5",
    "Referer": "https://sih.conagua.gob.mx/",
    "Connection": "keep-alive",
})

# ==========================================
# CATALOG FUNCTIONS
# ==========================================
def fetch_catalog() -> pd.DataFrame | None:
    """
    Downloads the main Excel catalog file and returns it as a DataFrame.
    """
    print(f"Downloading main station catalog from {CATALOG_URL}...")
    try:
        resp = session.get(CATALOG_URL, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        
        # Use pandas to read the Excel file from memory
        df = pd.read_excel(io.BytesIO(resp.content))
        df.columns = [str(c).strip() for c in df.columns]
        
        print("Catalog downloaded successfully.")
        return df
    except Exception as e:
        print(f"FATAL ERROR: Could not download or read the catalog: {e}")
        return None

def filter_catalog(df: pd.DataFrame, estado_filters: list[str]) -> list[str]:
    """
    Filters the catalog DataFrame based on the user's list of states.
    Returns a list of station keys ('claves').
    """
    try:
        # Find the 'clave' and 'estado' columns
        clave_col = next(c for c in df.columns if "clave" in c.lower())
        estado_col = next(c for c in df.columns if "estado" in c.lower())
    except StopIteration:
        print("FATAL ERROR: Could not find 'clave' or 'estado' columns in catalog file.")
        return []

    total_stations = len(df)
    print(f"Total stations found in catalog: {total_stations}")

    if not estado_filters: # This is the "all" case
        print("No filter applied (downloading all stations).")
        claves = df[clave_col].astype(str).str.strip().dropna().unique().tolist()
    else:
        # Normalize the 'estado' column for reliable matching
        df['estado_norm'] = df[estado_col].astype(str).str.strip().str.lower()
        
        # Create a boolean mask for rows where 'estado_norm' is in our filter list
        mask = df['estado_norm'].isin(estado_filters)
        filtered_df = df[mask]
        
        print(f"Found {len(filtered_df)} stations matching your criteria.")
        claves = filtered_df[clave_col].astype(str).str.strip().dropna().unique().tolist()
    
    return claves

# ==========================================
# DOWNLOAD FUNCTION
# ==========================================
def download_station_csv(clave: str, out_dir: Path) -> str:
    """
    Downloads the raw CSV for a single station and saves it.
    """
    csv_url = f"{BASE_URL}/{clave}.csv"
    out_path = out_dir / f"{clave}.csv" # Save as .csv

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(csv_url, timeout=TIMEOUT_SEC)
            
            if r.status_code == 404:
                return f"Not found: {clave}.csv"
            
            # Check for other errors (like 500, 403)
            r.raise_for_status()
            
            # Write the raw file content (in bytes)
            with open(out_path, 'wb') as f:
                f.write(r.content)
                
            return f"Saved {out_path.name}"

        except requests.RequestException as e:
            print(f"  > Attempt {attempt} failed: {e}")
            if attempt == MAX_RETRIES:
                return f"Failed {clave} (Network Error): {e}"
            time.sleep(2 ** attempt) # Exponential backoff (1s, 2s, 4s)
        except Exception as e:
            return f"Failed {clave} (File Error): {e}"

    return f"Failed {clave} after {MAX_RETRIES} retries"

# ==========================================
# MAIN
# ==========================================
def main() -> None:
    print("=== CONAGUA Hydrometric Station Downloader ===")
    
    # --- NEW: Get output folder from user ---
    print("\nPlease enter the full path for the folder to save files.")
    print(r"(e.g., C:\Users\YourUser\Downloads\CONAGUA_CSV or ./my_csv_folder)")
    user_folder = input("Output Folder: ").strip()

    if not user_folder:
        print("No output folder provided. Exiting.")
        return

    OUT_DIR = Path(user_folder)
    try:
        # Create the directory, including any parent folders
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"FATAL ERROR: Could not create folder at '{OUT_DIR}'.")
        print(f"Error: {e}")
        print("Please check the path and your permissions. Exiting.")
        return
    # --- END NEW ---

    print(f"\nThis script will download raw .csv files to: {OUT_DIR.resolve()}")
    print("\n---")
    print("Please enter the states (Estados) you want to download.")
    print(" - For multiple states, separate with a comma (e.g., Colima, Jalisco)")
    print(" - To download all stations, type: all")
    print("---")
    
    user_input = input("Estados: ").strip()

    if not user_input:
        print("No input provided. Exiting.")
        return

    if user_input.lower() == 'all':
        filter_list = [] # An empty list means "no filter"
        print("\n[Mode: Download ALL stations]")
    else:
        # Get user list, strip whitespace, convert to lowercase
        filter_list = [estado.strip().lower() for estado in user_input.split(',')]
        print(f"\n[Mode: Filtering for states: {', '.join(filter_list)}]")

    # 1. Fetch Catalog
    catalog_df = fetch_catalog()
    if catalog_df is None:
        return

    # 2. Filter Catalog
    claves_to_download = filter_catalog(catalog_df, filter_list)

    if not claves_to_download:
        print("No stations found matching your criteria. Exiting.")
        return

    print(f"\nFound {len(claves_to_download)} stations to download.")
    print("Do you want to proceed? (yes/no)")
    
    confirm = input("> ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("Download cancelled. Exiting.")
        return

    # 3. Download Loop
    print(f"\nStarting download of {len(claves_to_download)} files...")
    results: list[str] = []
    
    # Single-threaded loop
    for i, clave in enumerate(claves_to_download, 1):
        print(f"--- Downloading {i} of {len(claves_to_download)} ---")
        msg = download_station_csv(clave, OUT_DIR) # <-- Pass OUT_DIR
        results.append(msg)
        print(f" -> {msg}")
        

    # 4. Summary
    ok = [r for r in results if r.startswith("Saved")]
    not_ok = [r for r in results if not r.startswith("Saved")]

    print("\n=== DOWNLOAD SUMMARY ===")
    print(f"{len(ok)} successful")
    print(f"{len(not_ok)} failed or missing")
    if not_ok:
        print("\n--- Failures ---")
        for fail_msg in not_ok:
            print(fail_msg)
    print(f"\nAll downloaded files are in: {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()

