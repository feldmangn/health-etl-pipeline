import random, math
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

rng = np.random.default_rng(42)
random.seed(42)

N_PATIENTS = 1200          
MEAN_ENC_PER_PATIENT = 2.2 
START = datetime(2023,1,1)
END   = datetime(2024,12,31)

states = ["MD","VA","DC","PA","DE","WV"]
facilities = ["NIH","JHU","UMMC","GWU","Georgetown"]
enc_types = ["inpatient","outpatient","er"]

# ---- Patients ----
patient_ids = [f"P{str(i).zfill(5)}" for i in range(1, N_PATIENTS+1)]
sexes = rng.choice(["F","M"], size=N_PATIENTS, p=[0.52,0.48])
birth_years = rng.integers(1945, 2005, size=N_PATIENTS)  # ages ~20–80
states_draw = rng.choice(states, size=N_PATIENTS, p=[.38,.22,.12,.18,.06,.04])

patients = pd.DataFrame({
    "patient_id": patient_ids,
    "sex": sexes,
    "birth_year": birth_years,
    "state": states_draw
})

# ---- Encounters ----
def random_date():
    # biased slightly toward 2024 to make charts more interesting
    if rng.random() < 0.6:
        start, end = datetime(2024,1,1), END
    else:
        start, end = START, datetime(2023,12,31)
    delta = end - start
    return start + timedelta(days=int(rng.integers(0, delta.days+1)))

encounters = []
eid = 1
for pid in patient_ids:
    k = max(1, rng.poisson(MEAN_ENC_PER_PATIENT))
    # create a baseline date
    base = random_date()
    # create k encounters around base with some spread
    dates = sorted(base + timedelta(days=int(rng.normal(0, 60))) for _ in range(k))
    for d in dates:
        e_type = rng.choice(enc_types, p=[0.25, 0.6, 0.15])  # mostly outpatient
        facility = random.choice(facilities)
        encounters.append({
            "encounter_id": f"E{str(eid).zfill(6)}",
            "patient_id": pid,
            "start_date": d.date().isoformat(),
            "facility": facility,
            "encounter_type": e_type
        })
        eid += 1

enc = pd.DataFrame(encounters).sort_values(["patient_id","start_date"]).reset_index(drop=True)

# ---- 8% of Patients have to come back ---- #
inpatient_idx = enc.index[enc["encounter_type"] == "inpatient"].tolist()
n_readmit_seeds = math.floor(0.08 * len(inpatient_idx))
seeds = set(rng.choice(inpatient_idx, size=n_readmit_seeds, replace=False))
rows_to_add = []
for i in seeds:
    row = enc.loc[i].copy()
    t = datetime.fromisoformat(row["start_date"])
    follow = t + timedelta(days=int(rng.integers(3, 28)))
    rows_to_add.append({
        "encounter_id": f"E{str(eid).zfill(6)}",
        "patient_id": row["patient_id"],
        "start_date": follow.date().isoformat(),
        "facility": row["facility"],
        "encounter_type": "inpatient"
    })
    eid += 1

if rows_to_add:
    enc = pd.concat([enc, pd.DataFrame(rows_to_add)], ignore_index=True)
    enc = enc.sort_values(["patient_id","start_date"]).reset_index(drop=True)

# ---- Write CSVs ----
Path("data_raw").mkdir(exist_ok=True)
patients.to_csv("data_raw/patients.csv", index=False)
enc.to_csv("data_raw/encounters.csv", index=False)
print(f"Wrote {len(patients)} patients and {len(enc)} encounters to data_raw/")

# ---- Conditions (ICD-10) ----
# A small, realistic set of common chronic conditions
ICD10 = [
    ("E11", "Type 2 diabetes mellitus"),
    ("I10", "Essential (primary) hypertension"),
    ("E78", "Disorders of lipoprotein metabolism (hyperlipidemia)"),
    ("J45", "Asthma"),
    ("F32", "Major depressive disorder, single episode"),
    ("M54", "Dorsalgia (low back pain)"),
    ("N39", "Urinary tract infection, site not specified"),
    ("I25", "Chronic ischemic heart disease"),
    ("K21", "Gastro-esophageal reflux disease"),
    ("G43", "Migraine"),
    ("J44", "Chronic obstructive pulmonary disease"),
]

# Probabilities roughly reflecting prevalence (tweak as you like)
probs = np.array([0.10, 0.30, 0.28, 0.12, 0.12, 0.18, 0.08, 0.09, 0.10, 0.10, 0.07])
probs = probs / probs.sum()

def random_onset_year(byear: int) -> int:
    # Onset between age 18 and 80 (bounded by our END date)
    min_year = max(byear + 18, 1990)
    max_year = min(byear + 80, END.year)
    if min_year > max_year:
        min_year = max_year
    return int(rng.integers(min_year, max_year + 1))

cond_rows = []
for pid, by in zip(patients["patient_id"], patients["birth_year"].astype(int)):
    # each patient gets 0–3 chronic conditions
    n = rng.choice([0, 1, 2, 3], p=[0.25, 0.40, 0.25, 0.10])
    if n == 0:
        continue
    # sample without replacement, biased by probs
    chosen_idx = rng.choice(len(ICD10), size=n, replace=False, p=probs)
    for idx in chosen_idx:
        code, name = ICD10[idx]
        onset_y = random_onset_year(by)
        onset_m = int(rng.integers(1, 13))
        onset_d = int(rng.integers(1, 28))  # keep it simple
        onset_date = f"{onset_y:04d}-{onset_m:02d}-{onset_d:02d}"
        cond_rows.append({
            "patient_id": pid,
            "condition_code": code,
            "condition_name": name,
            "onset_date": onset_date,
        })

conditions = pd.DataFrame(cond_rows)

# ---- Write CSVs (patients/enc already exist above) ----
Path("data_raw").mkdir(exist_ok=True)
patients.to_csv("data_raw/patients.csv", index=False)
enc.to_csv("data_raw/encounters.csv", index=False)
conditions.to_csv("data_raw/conditions.csv", index=False)
print(f"Wrote {len(patients)} patients, {len(enc)} encounters, {len(conditions)} conditions to data_raw/")

