# etl/load.py
import sqlite3
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "health.db"
DATA_DIR = ROOT / "data_raw"

# Ensure folders
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Connect (creates DB file if missing)
con = sqlite3.connect(DB_PATH)

# ---- Load CSVs directly; create/replace tables
def load_csv(name: str):
    fp = DATA_DIR / f"{name}.csv"
    df = pd.read_csv(fp, dtype=str)
    df.to_sql(name, con, if_exists="replace", index=False)  # create or replace
    print(f"Loaded {len(df):,} rows into {name}")

load_csv("patients")
load_csv("encounters")
load_csv("conditions")

# ---- Add helpful indexes for queries
with con:
    con.execute("CREATE INDEX IF NOT EXISTS ix_encounters_patient ON encounters(patient_id);")
    con.execute("CREATE INDEX IF NOT EXISTS ix_conditions_patient ON conditions(patient_id);")

con.close()
print(f"SQLite database ready at {DB_PATH}")
