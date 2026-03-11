#!/usr/bin/env python3
"""
Debug row 113: print original data, what we send to API, and raw API response.
Run: python debug_translate_row113.py
"""

import pandas as pd
from pathlib import Path
import time

from deep_translator import GoogleTranslator

CSVS_DIR = Path(__file__).parent / "csvs"
INPUT_FILE = CSVS_DIR / "merged_columns.csv"
BRAND_PLACEHOLDER = "XBRANDX"


def mask_brand(text, brand):
    if pd.isna(text) or pd.isna(brand) or str(brand).strip() == "":
        return str(text).strip()
    return str(text).strip().replace(str(brand).strip(), BRAND_PLACEHOLDER)


def main():
    df = pd.read_csv(INPUT_FILE)
    i = 112  # row 113 (1-based)
    row = df.iloc[i]

    title_in = row["Title"]
    cat_in = row["Categories: Tree"]
    brand = row["Brand"]

    print("=" * 60)
    print("ORIGINAL (row 113)")
    print("=" * 60)
    print("Brand:", repr(brand))
    print()
    print("Title (DE) – full:")
    print(title_in)
    print("  -> len:", len(str(title_in)))
    print()
    print("Categories (DE) – full:")
    print(cat_in)
    print("  -> len:", len(str(cat_in)))
    print()

    title_masked = mask_brand(title_in, brand)
    cat_masked = mask_brand(cat_in, brand)

    print("=" * 60)
    print("SENT TO API (after mask)")
    print("=" * 60)
    print("Title (masked):")
    print(title_masked[:200] + "..." if len(title_masked) > 200 else title_masked)
    print("  -> len:", len(title_masked))
    print()
    print("Categories (masked):")
    print(cat_masked)
    print("  -> len:", len(cat_masked))
    print()

    translator = GoogleTranslator(source="de", target="tr")

    # SEQUENTIAL (no parallel) to see if API returns different things
    print("=" * 60)
    print("API RESPONSE (raw, before unmask) – sequential calls")
    print("=" * 60)

    print("Calling API for Title...")
    title_raw = translator.translate(title_masked)
    print("  Title raw response:", repr(title_raw))
    print("  -> len:", len(title_raw))
    time.sleep(0.5)

    print("Calling API for Categories...")
    cat_raw = translator.translate(cat_masked)
    print("  Categories raw response:", repr(cat_raw))
    print("  -> len:", len(cat_raw))
    print()

    # After unmask
    title_final = title_raw.replace(BRAND_PLACEHOLDER, str(brand).strip())
    cat_final = cat_raw.replace(BRAND_PLACEHOLDER, str(brand).strip())
    print("=" * 60)
    print("AFTER UNMASK (final)")
    print("=" * 60)
    print("Title (TR):", title_final)
    print("Categories (TR):", cat_final)


if __name__ == "__main__":
    main()
