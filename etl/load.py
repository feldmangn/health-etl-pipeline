from pathlib import Path
import pandas as pd
import sqlite3

DB_PATH = Path("data/health.db")
con = sqlite3.connect(DB_PATH)

cdi_fp = Path("data_raw/cdi.csv")
if cdi_fp.exists():
    cdi = pd.read_csv(cdi_fp, dtype=str)
    # Optional: normalize column names
    cdi.columns = [c.lower() for c in cdi.columns]
    cdi.to_sql("cdi", con, if_exists="replace", index=False)
    with con:
        con.execute("CREATE INDEX IF NOT EXISTS ix_cdi_loc_year ON cdi(locationabbr, yearstart);")
    print(f"Loaded {len(cdi)} rows into cdi")
else:
    print("No CDI CSV found; skip cdi load.")

con.close()
