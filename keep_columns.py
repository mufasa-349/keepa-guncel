#!/usr/bin/env python3
"""
Read merged_filtered.csv and keep only the specified columns.
Writes the result to merged_columns.csv.
"""

import pandas as pd
from pathlib import Path

CSVS_DIR = Path(__file__).parent / "csvs"
INPUT_FILE = CSVS_DIR / "merged_filtered.csv"
OUTPUT_FILE = CSVS_DIR / "merged_columns.csv"

KEEP_COLUMNS = [
    "Image",
    "Title",
    "Buy Box: Current",
    "Categories: Tree",
    "ASIN",
    "Product Codes: EAN",
    "Brand",
]


def main():
    if not INPUT_FILE.exists():
        print("Input file not found:", INPUT_FILE)
        return

    df = pd.read_csv(INPUT_FILE)
    df = df[[c for c in KEEP_COLUMNS if c in df.columns]]
    df.to_csv(OUTPUT_FILE, index=False)
    print("Written to:", OUTPUT_FILE)
    print("Columns:", list(df.columns))
    print("Rows:", len(df))


if __name__ == "__main__":
    main()
