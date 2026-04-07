#!/usr/bin/env python3
"""
guncel1_clean.csv dosyasında "Ürün adı" uzunluğu 15 karakterden kısa olan satırları siler.

Varsayılan:
  guncel1_clean.csv -> guncel1_clean_len15.csv
"""

import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path(__file__).parent / "guncel1_clean.csv"
DEFAULT_OUTPUT = Path(__file__).parent / "guncel1_clean_len15.csv"

COL = "Ürün adı"
MIN_LEN = 15


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    if COL not in df.columns:
        print(f"'{COL}' sütunu yok. Sütunlar: {list(df.columns)}")
        sys.exit(1)

    n_before = len(df)
    lens = df[COL].fillna("").astype(str).str.strip().str.len()
    mask_remove = lens < MIN_LEN
    removed = int(mask_remove.sum())
    df_out = df[~mask_remove].copy()

    df_out.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Önce: {n_before} | Silinen: {removed} | Sonra: {len(df_out)}")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()

