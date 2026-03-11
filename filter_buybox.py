#!/usr/bin/env python3
"""
Read merged.csv and drop rows where "Buy Box: Current" or "Buy Box: Stock" is empty.
Writes the result to merged_filtered.csv.
"""

import pandas as pd
from pathlib import Path

CSVS_DIR = Path(__file__).parent / "csvs"
INPUT_FILE = CSVS_DIR / "merged.csv"
OUTPUT_FILE = CSVS_DIR / "merged_filtered.csv"


def main():
    if not INPUT_FILE.exists():
        print("Input file not found:", INPUT_FILE)
        return

    df = pd.read_csv(INPUT_FILE)
    before = len(df)

    # Treat empty strings as missing
    for col in ["Buy Box: Current", "Buy Box: Stock"]:
        df[col] = df[col].replace("", pd.NA)
    df = df.dropna(subset=["Buy Box: Current", "Buy Box: Stock"]).reset_index(drop=True)
    after = len(df)

    df.to_csv(OUTPUT_FILE, index=False)
    print("Filtered CSV written to:", OUTPUT_FILE)
    print(f"Rows: {before} -> {after} (removed {before - after})")


if __name__ == "__main__":
    main()
