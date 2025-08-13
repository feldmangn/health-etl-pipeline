from pathlib import Path
import pandas as pd
import sqlite3

DB_PATH = Path("data/health.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

cdi_fp = Path("data_raw/cdi.csv")
if not cdi_fp.exists():
    print("No CDI CSV found; skip cdi load.")
else:
    # Load CSV
    cdi = pd.read_csv(cdi_fp, dtype=str)
    # Normalize columns
    cdi.columns = [c.strip().lower() for c in cdi.columns]
    # Coerce numerics commonly used in charts
    for col in ["datavalue", "lowconfidencelimit", "highconfidencelimit", "yearstart", "yearend"]:
        if col in cdi.columns:
            cdi[col] = pd.to_numeric(cdi[col], errors="coerce")
    # Trim strings (optional)
    for col in cdi.select_dtypes(include="object").columns:
        cdi[col] = cdi[col].str.strip()

    with sqlite3.connect(DB_PATH) as con:
        cdi.to_sql("cdi", con, if_exists="replace", index=False)
        con.execute("CREATE INDEX IF NOT EXISTS ix_cdi_loc_year ON cdi(locationabbr, yearstart);")
        con.execute("CREATE INDEX IF NOT EXISTS ix_cdi_topic ON cdi(topic);")
        con.execute("CREATE INDEX IF NOT EXISTS ix_cdi_type ON cdi(datavaluetype);")
    print(f"Loaded {len(cdi)} rows into cdi")
