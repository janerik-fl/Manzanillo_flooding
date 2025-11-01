# -*- coding: utf-8 -*-
"""
Creates a new PostgreSQL database "manzanillo" and initializes schema tables:
  - stations
  - measurements
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ==============================================
# CONFIGURATION
# ==============================================
HOST = "localhost"
PORT = 5432
USER = "postgres"
PASSWORD = "super"
NEW_DB_NAME = "manzanillo"

# Connect first to the default "postgres" database
DB_URI_ADMIN = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"

# ==============================================
# STEP 1: CREATE THE NEW DATABASE
# ==============================================
print(f"Connecting to PostgreSQL server {HOST}...")

try:
    engine = create_engine(DB_URI_ADMIN, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        # Check if the database already exists
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": NEW_DB_NAME},
        ).fetchone()

        if result:
            print(f"Database '{NEW_DB_NAME}' already exists. Skipping creation.")
        else:
            print(f"Creating new database '{NEW_DB_NAME}'...")
            conn.execute(text(f"CREATE DATABASE {NEW_DB_NAME};"))
            print("Database created successfully.")
except OperationalError as e:
    print(" Could not connect to PostgreSQL server.")
    print(f"Error details: {e}")
    exit(1)

# ==============================================
# STEP 2: CREATE TABLES INSIDE THE NEW DATABASE
# ==============================================
DB_URI_NEW = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NEW_DB_NAME}"
engine_new = create_engine(DB_URI_NEW)

SCHEMA_SQL = """
-- Create stations table
CREATE TABLE IF NOT EXISTS stations (
    id SERIAL PRIMARY KEY,
    station_code TEXT UNIQUE NOT NULL,
    name TEXT,
    state TEXT,
    municipality TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude DOUBLE PRECISION,
    cuenca TEXT,
    region_hidrologica TEXT
);

-- Create measurements table
CREATE TABLE IF NOT EXISTS measurements (
    id SERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    precipitation DOUBLE PRECISION,
    temperature DOUBLE PRECISION
);

-- Add helpful indexes
CREATE INDEX IF NOT EXISTS idx_measurements_date ON measurements(date);
CREATE INDEX IF NOT EXISTS idx_measurements_station_id ON measurements(station_id);
"""

print(f"Creating schema in database '{NEW_DB_NAME}'...")

try:
    with engine_new.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
    print(" Schema created successfully in database 'manzanillo'.")
except OperationalError as e:
    print(" Error while creating tables.")
    print(f"Error details: {e}")
    exit(1)

print(" Database and schema setup complete!")
