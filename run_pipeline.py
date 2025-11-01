import subprocess
import sys
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()

steps = [
    (" Step 1/4: Downloading raw CSVs...", "all_files_weather_multi_thread_colima_only.py"),
    (" Step 2/4: Cleaning CSV files into Excel...", "cleaner.py"),
    (" Step 3/4: Creating PostgreSQL database and tables...", "db_creator.py"),
    (" Step 4/4: Importing cleaned Excel data into database...", "db_inport_from_cleaned_data.py"),
]

def run_script(script_name: str):
    """Run a Python script in a subprocess."""
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        print(f" Missing: {script_name}")
        sys.exit(1)
    print(f"Running {script_name} ...")
    result = subprocess.run([sys.executable, str(script_path)], cwd=BASE_DIR)
    if result.returncode != 0:
        print(f" Error while running {script_name}. Exiting.")
        sys.exit(result.returncode)

def main():
    print("==============================================")
    print(" Starting CONAGUA Weather ETL Pipeline")
    print("==============================================\n")

    for message, script in steps:
        print(message)
        print("=" * 60)
        run_script(script)
        print()

    print(" All steps completed successfully!")
    print("Database 'manzanillo' is ready with all station data.")

if __name__ == "__main__":
    main()

