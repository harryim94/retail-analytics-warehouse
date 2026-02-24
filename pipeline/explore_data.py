import os
import pandas as pd

raw_dir = "data/raw"

for f in sorted(os.listdir(RAW_DIR)):
    if f.lower().endwith(".csv"):
        path = os.path.join(raw_dir,f)
        print("\n===",f,"===")
        df = pd.read_csv(path)
        print("shape",df.shape)
        print("columns:",list(df.columns))