#!/usr/bin/env python3
"""
Buy Box: Current fiyatını 55 ile çarpıp hemen yanına "TL fiyat" sütununu ekler.
Varsayılan: merged_translated_filtered.csv (üzerine yazılır).
"""

import sys
from pathlib import Path

import pandas as pd

CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged_translated_filtered.csv"
BUY_BOX_COL = "Buy Box: Current"
TL_COL = "TL fiyat"
MULTIPLIER = 55


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else input_path

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    print("Okunuyor:", input_path)
    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)

    if BUY_BOX_COL not in df.columns:
        print(f"'{BUY_BOX_COL}' sütunu bulunamadı.")
        sys.exit(1)

    price = pd.to_numeric(df[BUY_BOX_COL], errors="coerce")
    df[TL_COL] = (price * MULTIPLIER).round(2)

    # Sütunu "Buy Box: Current" hemen yanına taşı (zaten eklenince sonda olur; sırayı düzeltmek için)
    cols = list(df.columns)
    cols.remove(TL_COL)
    idx = cols.index(BUY_BOX_COL) + 1
    cols.insert(idx, TL_COL)
    df = df[cols]

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"'{TL_COL}' = {BUY_BOX_COL} x {MULTIPLIER} eklendi. Yazıldı:", output_path)


if __name__ == "__main__":
    main()
