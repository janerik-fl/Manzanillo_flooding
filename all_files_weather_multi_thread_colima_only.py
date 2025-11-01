import os
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Base URL for station CSVs
BASE_URL = "https://sih.conagua.gob.mx/basedatos/Climas"
LIST_PATH = "station_colima.txt"     # your uploaded list
DOWNLOAD_DIR = "climas_colima_csv"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Shared session with browser headers
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://sih.conagua.gob.mx/climas.html",
    "Connection": "keep-alive",
})

# --- Read station list ---
def load_station_list(path):
    with open(path, "r", encoding="utf-8") as f:
        stations = [line.strip() for line in f if line.strip()]
    return stations

# --- Download function with retry and backoff ---
def download_station_csv(station, max_retries=5):
    url = f"{BASE_URL}/{station}.csv"
    filename = f"{station}.csv"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=session.headers, timeout=90)
            if r.status_code == 403:
                print(f" 403 Forbidden on attempt {attempt} for {station}")
                time.sleep(2 ** attempt + random.uniform(0, 2))
                continue
            elif r.status_code == 404:
                return f" Not found: {station}.csv"
            r.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(r.content)
            return f" Saved {station}.csv"
        except requests.RequestException as e:
            print(f" Attempt {attempt} failed for {station}: {e}")
            time.sleep(2 ** attempt + random.uniform(0, 2))

    return f" Failed {station} after {max_retries} retries"

# --- Main multi-threaded runner ---
def main(max_workers=8):
    stations = load_station_list(LIST_PATH)
    print(f"Loaded {len(stations)} station names from {LIST_PATH}")
    print(f"Downloading to {DOWNLOAD_DIR} using {max_workers} threads...\n")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_station_csv, st): st for st in stations}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(result)
            time.sleep(random.uniform(0.2, 0.8))  # polite pause

    success = [r for r in results if r.startswith("✅")]
    failed = [r for r in results if r.startswith("❌")]
    print("\n=== Summary ===")
    print(f"{len(success)} successful downloads")
    print(f" {len(failed)} failed downloads")
    print(f"Files saved in: {os.path.abspath(DOWNLOAD_DIR)}")

if __name__ == "__main__":
    main(max_workers=8)
