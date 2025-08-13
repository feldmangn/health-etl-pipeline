import os
import requests
import pandas as pd
from pathlib import Path

BASE = "https://data.cdc.gov/resource/hksd-2xuw.json"
OUT = Path("data_raw/cdi.csv")
OUT.parent.mkdir(exist_ok=True)

headers = {}
if "SOCRATA_APP_TOKEN" in os.environ:
    headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

def fetch(params):
    r = requests.get(BASE, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return pd.DataFrame(r.json())

# 1) Pull a recent Maryland slice WITHOUT datavaluetype filter
md_recent = fetch({
    "locationabbr": "MD",
    "$where": "yearstart >= 2020",
    "$limit": 50000
})

if md_recent.empty:
    print("No rows for MD with yearstart >= 2020. Falling back to >= 2018 …")
    md_recent = fetch({
        "locationabbr": "MD",
        "$where": "yearstart >= 2018",
        "$limit": 50000
    })

if md_recent.empty:
    # Last resort: just grab some MD rows to inspect later
    print("Still empty. Writing first 1000 MD rows for inspection.")
    md_any = fetch({"locationabbr": "MD", "$limit": 1000})
    md_any.to_csv(OUT, index=False)
    print(f"Wrote {len(md_any)} CDI rows (unfiltered) → {OUT}")
else:
    # 2) Discover actual datavaluetype values
    types = (md_recent["datavaluetype"]
             .dropna()
             .astype(str)
             .str.strip()
             .value_counts())
    print("Found datavaluetype values:\n", types.to_string())

    # Prefer a type that contains 'crude'
    chosen = None
    for t in types.index:
        if "crude" in t.lower():
            chosen = t
            break
    if chosen is None:
        # Fall back to the most common type
        chosen = types.index[0]
    print(f"Using datavaluetype = {chosen!r}")

    # 3) Pull the final filtered dataset with a tidy projection
    final = fetch({
        "locationabbr": "MD",
        "$where": f"yearstart >= 2020 AND datavaluetype = '{chosen}'",
        "$select": "yearstart,yearend,locationabbr,locationdesc,topic,question,datavaluetype,datavalue,lowconfidencelimit,highconfidencelimit,datavalueunit",
        "$order": "yearstart,topic,question",
        "$limit": 50000
    })

    print(f"Final filtered rows: {len(final)}")
    final.to_csv(OUT, index=False)
    print(f"Wrote {len(final)} CDI rows → {OUT}")
