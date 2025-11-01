# 🌦️ Manzanillo Weather ETL

Automated end-to-end pipeline for downloading, cleaning, and storing CONAGUA weather station data from the Colima region (Mexico) into a PostgreSQL database — with nightly automation and GitHub integration.

---

## Project Overview

This project automates:  
1. **Data acquisition** — Multi-threaded download of CONAGUA weather station CSVs.  
2. **Cleaning & normalization** — Conversion of raw CSVs to structured Excel files.  
3. **Database setup** — Automatic PostgreSQL schema creation (`stations` + `measurements`).  
4. **Data import** — Ingests cleaned Excel files into the database.  
5. **Scheduling** — Runs nightly via Windows Task Scheduler (default: 02:00).  
6. **Version control** — Code and config pushed securely to GitHub.

---

##  Folder Structure

```
manzanillo_weather_etl/
│
├── all_files_weather_multi_thread_colima_only.py   # Downloader
├── cleaner.py                                      # CSV → Excel cleaner
├── db_creator.py                                   # Creates DB + schema
├── db_inport_from_cleaned_data.py                  # Imports data to DB
├── weather_stations_meta_data.csv                  # Station metadata
├── run_pipeline.py                                 # Pipeline orchestrator
├── requirements.txt                                # Python dependencies
├── setup.ps1                                       # 🚀 One-click installer
├── run_pipeline.ps1                                # Created automatically for nightly runs
├── logs/                                           # Log files (auto-generated)
├── climas_colima_csv/                              # Raw downloaded CSVs
└── output_cleaned_data/                            # Cleaned Excel files
```

---

## ⚙️ Prerequisites

Before running the setup script, make sure you have:

| Dependency | Description | Install Link |
|-------------|--------------|---------------|
| **Python 3.10+** | Required to run ETL scripts | [python.org/downloads](https://www.python.org/downloads) |
| **PostgreSQL** | Used to store cleaned data | [postgresql.org/download](https://www.postgresql.org/download) |
| **Git** | For version control and repo publishing | [git-scm.com/download/win](https://git-scm.com/download/win) |
| **GitHub CLI (`gh`)** | Secure authentication with GitHub | [cli.github.com](https://cli.github.com/) |

---

## 🪟 Windows Setup (One-Click)

1. **Extract** the project folder to:
   ```
   C:\manzanillo_weather_etl\
   ```

2. **Open PowerShell** (run as your normal user, not admin).

3. Run the setup script:

   ```powershell
   cd C:\manzanillo_weather_etl
   powershell -ExecutionPolicy Bypass -File .\setup.ps1 -RepoUrl "https://github.com/<yourusername>/manzanillo_weather_etl.git"
   ```

💡 On the first run, this will:
- Create virtual environment (`.venv`)
- Install dependencies (`requirements.txt`)
- Create PostgreSQL schema (database: `manzanillo`)
- Register a Windows Task Scheduler job (runs at 02:00)
- Push your code to GitHub securely using your authenticated session

---

## 🔑 GitHub Authentication

This setup uses **GitHub CLI (`gh`)** for secure, token-based authentication.  
If you haven’t logged in before:

```powershell
gh auth login
```

Then choose:
- **GitHub.com**
- **HTTPS**
- **Authenticate with web browser**

This caches your credentials securely in Windows Credential Manager.

---

## ▶️ Running the ETL Manually

If you want to run the full pipeline manually (outside the schedule):

```powershell
cd C:\manzanillo_weather_etl
.\run_pipeline.ps1
```

This will:
- Download new CSVs  
- Clean them  
- Create / update the database  
- Import data into PostgreSQL  
- Log results in `C:\manzanillo_weather_etl\logs\`

---

## 📅 Scheduled Automation

The setup registers a **Windows Task Scheduler job**:

```
Task name:    ManzanilloWeatherETL
Frequency:    Daily
Time:         02:00 AM
User:         (Your Windows account)
Script:       run_pipeline.ps1
```

To verify or change the schedule:
1. Open **Task Scheduler** → “Task Scheduler Library”.
2. Find **ManzanilloWeatherETL**.
3. Right-click → *Properties* → adjust schedule if needed.

---

## 🗃️ PostgreSQL Database Structure

| Table | Purpose |
|--------|----------|
| `stations` | Weather station metadata (name, coordinates, altitude, hydrological region). |
| `measurements` | Time-series measurements (date, precipitation, temperature, etc.). |

**Example query:**

```sql
SELECT s.station_code, s.name, m.date, m.precipitation, m.temperature
FROM measurements m
JOIN stations s ON s.id = m.station_id
ORDER BY s.station_code, m.date;
```

---

## 🧠 Useful Commands

### Check how many stations and records were imported
```sql
SELECT COUNT(*) AS stations FROM stations;
SELECT COUNT(*) AS measurements FROM measurements;
```

### List all stations with data newer than 2024
```sql
SELECT DISTINCT s.station_code, s.name
FROM stations s
JOIN measurements m ON m.station_id = s.id
WHERE m.date >= '2024-01-01';
```

---

## 🪵 Logs

All ETL logs are saved in:
```
C:\manzanillo_weather_etl\logs\
```

Each run creates a file like:
```
etl_2025_11_01_020001.log
```

The 10 most recent logs are kept; older ones are automatically deleted.

---

## 🧾 Updating the Repository

After setup, you can manage your GitHub repo normally:

```bash
git add .
git commit -m "Updated cleaning logic"
git push
```

---

## 🧰 Troubleshooting

| Issue | Possible Fix |
|--------|---------------|
| `psycopg2` not found | Run: `.\.venv\Scripts\pip install psycopg2-binary` |
| PostgreSQL connection failed | Check credentials in `db_creator.py` and ensure PostgreSQL service is running |
| Task doesn’t run | Ensure task runs under your Windows user, not SYSTEM |
| Git push fails | Run `gh auth login` again or verify your GitHub credentials |

---

## 🧩 License

MIT License © 2025 — *Manzanillo Weather ETL Project*

---

### 🌐 Credits

Developed by **Jan Erik Fløde**  
with support for ISO 19156-ready weather data processing.
