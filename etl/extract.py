import os, requests, pandas as pd
from pathlib import Path

BASE = "https://data.cdc.gov/resource/hksd-2xuw.json"
OUT = Path("data_raw/cdi.csv"); OUT.parent.mkdir(exist_ok=True)

headers = {}
if "SOCRATA_APP_TOKEN" in os.environ:
    headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

def fetch(params):
    r = requests.get(BASE, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def fetch_paged(base_params, page=50000, max_pages=50):
    """Pull with $limit/$offset pagination."""
    frames, offset = [], 0
    for _ in range(max_pages):
        p = {**base_params, "$limit": page, "$offset": offset}
        df = fetch(p)
        if df.empty:
            break
        frames.append(df)
        if len(df) < page:
            break
        offset += page
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# 1) Pull a recent MD slice (discover values)
md_recent = fetch_paged({
    "locationabbr": "MD",
    "$where": "yearstart >= 2020"
})
if md_recent.empty:
    print("No rows for MD with yearstart >= 2020. Falling back to >= 2018 …")
    md_recent = fetch_paged({
        "locationabbr": "MD",
        "$where": "yearstart >= 2018"
    })

if md_recent.empty:
    print("Still empty. Writing first 1000 MD rows for inspection.")
    md_any = fetch({"locationabbr": "MD", "$limit": 1000})
    md_any.to_csv(OUT, index=False)
    print(f"Wrote {len(md_any)} CDI rows (unfiltered) → {OUT}")
    raise SystemExit(0)

# 2) Inspect datavaluetype and pick one containing 'crude' (else most common)
types = (md_recent["datavaluetype"].dropna().astype(str).str.strip().value_counts())
print("Found datavaluetype values:\n", types.to_string())

chosen = next((t for t in types.index if "crude" in t.lower()), types.index[0])
print(f"Using datavaluetype = {chosen!r}")

# Defensive: escape single quotes for SoQL
chosen_escaped = chosen.replace("'", "''")

# 3) Final filtered pull with a tidy projection
final = fetch_paged({
    "locationabbr": "MD",
    "$where": f"yearstart >= 2020 AND datavaluetype = '{chosen_escaped}'",
    "$select": "yearstart,yearend,locationabbr,locationdesc,topic,question,datavaluetype,datavalue,lowconfidencelimit,highconfidencelimit,datavalueunit",
    "$order": "yearstart,topic,question"
})

# Fallback: if filter too strict, save the broader md_recent slice
if final.empty:
    print("Filtered result empty; saving broader MD recent slice for inspection.")
    md_recent.to_csv(OUT, index=False)
    print(f"Wrote {len(md_recent)} CDI rows (broad slice) → {OUT}")
else:
    print(f"Final filtered rows: {len(final)}")
    final.to_csv(OUT, index=False)
    print(f"Wrote {len(final)} CDI rows → {OUT}")
