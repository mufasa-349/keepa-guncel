#!/usr/bin/env python3
"""
Author sütununu siler, sonra Author hariç herhangi bir sütunu boş olan satırları siler
(NaN veya boş string).

Varsayılan:
  brands-1/KeepaExport-2026-04-25-ProductFinder_stripped.csv
    -> brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor.csv
"""

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parent
DEFAULT_INPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped.csv"
DEFAULT_OUTPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor.csv"

AUTHOR_COL = "Author"
EAN_COL = "Product Codes: EAN"


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.is_absolute():
        input_path = BASE_DIR / input_path
    if not output_path.is_absolute():
        output_path = BASE_DIR / output_path

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    n_before = len(df)

    # 1) Author sütununu kaldır (varsa)
    if AUTHOR_COL in df.columns:
        df = df.drop(columns=[AUTHOR_COL])

    # 2) NaN ve boş string (trim sonrası) kontrolü
    #    Not: Product Codes: EAN sütunundaki boşluklar hariç tutulur (EAN boş olabilir).
    check_cols = [c for c in df.columns if c != EAN_COL]
    trimmed = df[check_cols].apply(lambda col: col.astype(str).str.strip())
    mask_any_empty = df[check_cols].isna().any(axis=1) | trimmed.eq("").any(axis=1)

    removed = int(mask_any_empty.sum())
    df_out = df.loc[~mask_any_empty].copy()

    df_out.to_csv(output_path, index=False, encoding="utf-8")

    print("Girdi:", input_path)
    print("Çıktı:", output_path)
    print(f"Satır: {n_before} -> {len(df_out)} (silinen: {removed})")


if __name__ == "__main__":
    main()

