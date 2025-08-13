--- Create base tables ---

DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS conditions;

CREATE TABLE patients (
    patient_id TEXT PRIMARY KEY,
    sex TEXT,
    start_date TEXT,
    facility TEXT,
    encounter_type TEXT
);

CREATE TABLE conditions (
  patient_id     TEXT,
  condition_code TEXT,
  condition_name TEXT,
  onset_date     TEXT
);

CREATE INDEX IF NOT EXISTS ix_encounters_patient ON encounters(patient_id);
CREATE INDEX IF NOT EXISTS ix_conditions_patient ON conditions(patient_id);