#!/usr/bin/env python3
"""
Türkçe ürün adı (Title (TR)) 20 karakterden kısa olan satırları siler.

Varsayılan:
  brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean.csv
    -> brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean_len20.csv
"""

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parent
DEFAULT_INPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean.csv"
DEFAULT_OUTPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean_len20.csv"

COL = "Title (TR)"
MIN_LEN = 20


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.is_absolute() and not input_path.exists():
        alt = BASE_DIR / input_path
        if alt.exists():
            input_path = alt

    if not output_path.is_absolute() and output_path.parent == Path("."):
        output_path = BASE_DIR / output_path

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
    df_out = df.loc[~mask_remove].copy()

    df_out.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Önce: {n_before} | Silinen: {removed} | Sonra: {len(df_out)}")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()

