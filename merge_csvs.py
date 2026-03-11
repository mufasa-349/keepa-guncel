#!/usr/bin/env python3
"""
Merge all CSV files in the csvs/ folder by ASIN (unique per product).
Outputs a single merged CSV and prints product count and column names.
"""

import pandas as pd
from pathlib import Path

CSVS_DIR = Path(__file__).parent / "csvs"
OUTPUT_FILE = Path(__file__).parent / "csvs" / "merged.csv"


def main():
    csv_files = sorted(CSVS_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in", CSVS_DIR)
        return

    # Exclude output file if it already exists (avoid merging it into itself)
    csv_files = [f for f in csv_files if f.name != "merged.csv"]

    dfs = []
    for f in csv_files:
        df = pd.read_csv(f)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    # One row per ASIN (keep first occurrence)
    merged = merged.drop_duplicates(subset=["ASIN"], keep="first").reset_index(drop=True)

    merged.to_csv(OUTPUT_FILE, index=False)

    print("Merged CSV written to:", OUTPUT_FILE)
    print("Number of products (unique ASINs):", len(merged))
    print("Column names:")
    for i, col in enumerate(merged.columns, 1):
        print(f"  {i}. {col}")


if __name__ == "__main__":
    main()
