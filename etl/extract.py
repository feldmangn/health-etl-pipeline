import sys, subprocess, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

#Check that raw data exists
Path("data_raw").mkdir(exist_ok=True)

print("Generating synthetic CSVs with make_synth_data.py ...")
subprocess.check_call([sys.executable, "make_synth_data.py"])
print("Done! Files in data_raw/:", list(Path("data_raw").glob("*.csv")))