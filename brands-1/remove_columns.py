#!/usr/bin/env python3
"""
Keepa CSV'den istenen sütunları çıkarır ve yeni dosyaya kaydeder.

Kaldırılan sütunlar:
  - Sales Rank: Current
  - Reviews: Rating
  - Buy Box: Current
  - Buy Box: Stock
  - Amazon: Current
  - Amazon: Stock
  - New: Current

Varsayılan:
  brands-1/KeepaExport-2026-04-25-ProductFinder.csv
    -> brands-1/KeepaExport-2026-04-25-ProductFinder_stripped.csv
"""

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parent
DEFAULT_INPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder.csv"
DEFAULT_OUTPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped.csv"

DROP_COLS = [
    "Sales Rank: Current",
    "Reviews: Rating",
    "Buy Box: Current",
    "Buy Box: Stock",
    "Amazon: Current",
    "Amazon: Stock",
    "New: Current",
]


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

    existing_drop = [c for c in DROP_COLS if c in df.columns]
    missing = [c for c in DROP_COLS if c not in df.columns]
    if missing:
        print("Uyarı: Bazı sütunlar bulunamadı ve atlandı:", ", ".join(missing))

    df_out = df.drop(columns=existing_drop)
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    print("Girdi:", input_path)
    print("Çıktı:", output_path)
    print(f"Sütun: {len(df.columns)} -> {len(df_out.columns)} (çıkarılan: {len(existing_drop)})")


if __name__ == "__main__":
    main()

