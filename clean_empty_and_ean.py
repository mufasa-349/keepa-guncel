#!/usr/bin/env python3
"""
merged_translated_filtered.csv için:
1) Herhangi bir sütunu boş (NaN veya boş string) olan satırları siler.
2) Product Codes: EAN sütununda virgülle ayrılmış birden fazla numara varsa ilkini bırakır, tek EAN kalır.
Çıktı varsayılan olarak aynı dosyaya yazılır (üzerine yazılır).
"""

import sys
from pathlib import Path

import pandas as pd

CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged_translated_filtered.csv"
EAN_COL = "Product Codes: EAN"


def is_empty(val):
    if pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def first_ean_only(val):
    """Virgülle ayrılmış EAN listesinde sadece ilk numarayı döndürür."""
    if pd.isna(val):
        return val
    s = str(val).strip()
    if "," not in s:
        return s
    return s.split(",")[0].strip()


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else input_path

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    print("Okunuyor:", input_path)
    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    n_before = len(df)

    # 1) EAN: virgülle ayrılmışsa ilkini al
    if EAN_COL in df.columns:
        ean_before = (df[EAN_COL].astype(str).str.contains(",", na=False)).sum()
        df[EAN_COL] = df[EAN_COL].apply(first_ean_only)
        print(f"EAN: {ean_before} satırda virgül vardı, ilk numara bırakıldı.")

    # 2) Herhangi bir sütunu boş olan satırları sil
    mask_any_empty = df.apply(
        lambda row: any(is_empty(row[c]) for c in df.columns),
        axis=1,
    )
    n_removed = mask_any_empty.sum()
    df = df[~mask_any_empty].copy()

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Satır: {n_before} -> {len(df)} (silinen: {n_removed})")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()
