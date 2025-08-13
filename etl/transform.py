import sqlite3
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "health.db"

with sqlite3.connect(DB_PATH) as con:
    # ---- Extract raw tables
    patients = pd.read_sql_query("SELECT * FROM patients", con, dtype=str)
    enc = pd.read_sql_query("SELECT * FROM encounters", con, dtype=str)
    conds = pd.read_sql_query("SELECT * FROM conditions", con, dtype=str)

# ---- Clean & coerce types
# Patients
patients["birth_year"] = pd.to_numeric(patients.get("birth_year"), errors="coerce")
patients["sex"] = patients.get("sex", "").str.strip()
patients["state"] = patients.get("state", "").str.strip()
patients = patients.drop_duplicates(subset=["patient_id"]).dropna(subset=["patient_id"])

# Encounters
enc["start_date"] = pd.to_datetime(enc["start_date"], errors="coerce")
enc["encounter_type"] = enc.get("encounter_type", "").str.strip().str.lower()
enc["facility"] = enc.get("facility", "").str.strip()
enc = enc.dropna(subset=["encounter_id", "patient_id", "start_date"]).drop_duplicates(subset=["encounter_id"])

# Conditions
conds["onset_date"] = pd.to_datetime(conds.get("onset_date"), errors="coerce")
conds["condition_code"] = conds.get("condition_code", "").str.strip()
conds["condition_name"] = conds.get("condition_name", "").str.strip()
conds = conds.dropna(subset=["patient_id", "condition_code"]).drop_duplicates()

# ---- Referential integrity: keep only rows with known patients
valid_pids = set(patients["patient_id"])
enc = enc[enc["patient_id"].isin(valid_pids)]
conds = conds[conds["patient_id"].isin(valid_pids)]

# ---- Business rule: 30-day readmission flag (inpatient only)
enc = enc.sort_values(["patient_id", "start_date"]).reset_index(drop=True)
enc["was_readmit"] = False

for pid, grp in enc.groupby("patient_id"):
    g = grp.reset_index()  # keep original index
    for _, row in g.iterrows():
        if row["encounter_type"] != "inpatient":
            continue
        win = g[
            (g["index"] != row["index"]) &
            (g["encounter_type"] == "inpatient") &
            (g["start_date"] > row["start_date"]) &
            (g["start_date"] <= row["start_date"] + pd.Timedelta(days=30))
        ]
        if len(win) > 0:
            enc.loc[row["index"], "was_readmit"] = True

# ---- Write cleaned tables back to SQLite (no visuals)
with sqlite3.connect(DB_PATH) as con:
    # Replace “*_clean” tables atomically
    patients.to_sql("patients_clean", con, if_exists="replace", index=False)
    enc_out = enc.copy()
    enc_out["was_readmit"] = enc_out["was_readmit"].astype(bool)  # store as 0/1 boolean in SQLite
    enc_out.to_sql("encounters_clean", con, if_exists="replace", index=False)
    conds.to_sql("conditions_clean", con, if_exists="replace", index=False)

    # Helpful indexes
    con.execute("CREATE INDEX IF NOT EXISTS ix_patients_clean_pid ON patients_clean(patient_id);")
    con.execute("CREATE INDEX IF NOT EXISTS ix_enc_clean_pid_date ON encounters_clean(patient_id, start_date);")
    con.execute("CREATE INDEX IF NOT EXISTS ix_conds_clean_pid ON conditions_clean(patient_id);")

    # Dim/Fact style convenience views for BI (optional)
    con.execute("DROP VIEW IF EXISTS vw_readmit_month;")
    con.execute("""
        CREATE VIEW vw_readmit_month AS
        WITH fe AS (
          SELECT date(strftime('%Y-%m-01', start_date)) AS month_start,
                 CASE WHEN was_readmit THEN 1.0 ELSE 0.0 END AS readmit
          FROM encounters_clean
        )
        SELECT month_start, AVG(readmit) AS readmit_rate
        FROM fe
        GROUP BY month_start
        ORDER BY month_start;
    """)

    con.execute("DROP VIEW IF EXISTS vw_top_conditions;")
    con.execute("""
        CREATE VIEW vw_top_conditions AS
        SELECT condition_name, COUNT(DISTINCT patient_id) AS patients
        FROM conditions_clean
        GROUP BY condition_name
        ORDER BY patients DESC;
    """)

# ---- Lightweight QA printouts (no plots)
print("=== CLEAN SUMMARY ===")
print(f"patients_clean:   {len(patients):,}")
print(f"encounters_clean: {len(enc):,}  (with was_readmit computed)")
print(f"conditions_clean: {len(conds):,}")
print("Views created: vw_readmit_month, vw_top_conditions")
