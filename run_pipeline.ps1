# run_pipeline.ps1
# Runs the ETL inside a venv, captures a timestamped log, and rotates old logs.

$ErrorActionPreference = "Stop"

# --- Paths ---
$BASE = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $BASE

$VENV = Join-Path $BASE ".venv"
$PY    = Join-Path $VENV "Scripts\python.exe"
$REQ   = Join-Path $BASE "requirements.txt"
$MAIN  = Join-Path $BASE "run_pipeline.py"
$LOGDIR = Join-Path $BASE "logs"
New-Item -ItemType Directory -Force -Path $LOGDIR | Out-Null

# --- Ensure venv exists & deps installed ---
if (-not (Test-Path $PY)) {
    python -m venv $VENV
}
& $PY -m pip install --upgrade pip
& $PY -m pip install -r $REQ

# --- Run ETL with timestamped log ---
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $LOGDIR ("etl_"+$stamp+".log")

# run synchronously and tee output to log
& $PY $MAIN *>> $logFile

# --- Simple log rotation: keep last 10 logs ---
Get-ChildItem -Path $LOGDIR -Filter "etl_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 | Remove-Item -Force
