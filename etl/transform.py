import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "health.db"
VIZ_DIR = ROOT / "viz"
VIZ_DIR.mkdir(exist_ok=True)

con = sqlite3.connect(DB_PATH)

#Extract Data from database
patients = pd.read_sql_query("SELECT * FROM patients", con, dtype=str)
enc = pd.read_sql_query("SELECT * FROM encounters", con, dtype=str)
conds = pd.read_sql_query("SELECT * FROM conditions", con, dtype=str)

#Parse Dates
enc["start_date"] = pd.to_datetime(enc["start_date"])
conds["onset_date"] = pd.to_datetime(conds["onset_date"], errors="coerce")

# 30 day readmit flag for inpatient encoutners
enc = enc.sort_values(["patient_id", "start_date"]).reset_index(drop=True)
enc["was_readmit"] = False

for pid, grp in enc.groupby("patient_id"):
    grp = grp.reset_index()
    for i, row in grp.iterrows():
        if row["encounter_type"] != "inpatient":
            continue
        later = grp[
            (grp["index"] != row["index"]) &
            (grp["encounter_type"] == "inpatient") &
            (grp["start_date"] > row["start_date"]) &
            (grp["start_date"] <= row["start_date"] + pd.Timedelta(days=30))
        ]
        if len(later) > 0:
            enc.loc[row["index"], "was_readmit"] = True

# New flag 
with con:
    con.execute("DROP TABLE IF EXISTS encounters;")
    enc_to_write = enc.copy()
    # Convert bools to strings for SQLite consistency
    enc_to_write["was_readmit"] = enc_to_write["was_readmit"].astype(str)
    enc_to_write.to_sql("encounters", con, if_exists="replace", index=False)


# ---- Analytics: readmit rate by month
q_readmit = """
WITH fe AS (
  SELECT date(strftime('%Y-%m-01', start_date)) AS month_start,
         was_readmit
  FROM encounters
)
SELECT month_start,
       AVG(CASE WHEN was_readmit='True' THEN 1.0 ELSE 0.0 END) AS readmit_rate
FROM fe
GROUP BY month_start
ORDER BY month_start;
"""
df_readmit = pd.read_sql_query(q_readmit, con)

# ---- Analytics: top conditions by unique patients
q_top = """
SELECT condition_name, COUNT(DISTINCT patient_id) AS patients
FROM conditions
GROUP BY condition_name
ORDER BY patients DESC
LIMIT 10;
"""
df_top = pd.read_sql_query(q_top, con)

con.close()

# ---- Visualize
plt.figure()
plt.plot(df_readmit["month_start"], df_readmit["readmit_rate"], marker="o")
plt.title("Monthly 30-day Readmission Rate")
plt.xlabel("Month")
plt.ylabel("Rate")
plt.tight_layout()
plt.savefig(VIZ_DIR / "readmit_rate.png")
print("Saved viz/readmit_rate.png")

plt.figure()
plt.barh(df_top["condition_name"], df_top["patients"])
plt.title("Top Conditions by Patient Count")
plt.xlabel("Patients")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(VIZ_DIR / "top_conditions.png")
print("Saved viz/top_conditions.png")