#!/usr/bin/env python3
"""
guncel1.csv dosyasında "Ürün adı" sütunundan parantez içindeki Yenilenmiş (Amazon Renewed) ibaresini kaldırır.

Örnek temizleme:
  "Philips ... (Yenilenmiş)" -> "Philips ..."
  "Philips ... (yenilenmis)" -> "Philips ..."

Varsayılan: guncel1.csv -> guncel1_clean.csv
"""

import re
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path(__file__).parent / "guncel1.csv"
DEFAULT_OUTPUT = Path(__file__).parent / "guncel1_clean.csv"

COL_TITLE_TR = "Ürün adı"

# Parantez veya köşeli parantez içinde geçen Yenilenmiş etiketini sil
_RENEWED_PATTERNS = [
    re.compile(r"\s*[\(\[]\s*yenilenmiş\s*[\)\]]\s*", re.IGNORECASE),
    re.compile(r"\s*[\(\[]\s*yenilenmis\s*[\)\]]\s*", re.IGNORECASE),
    re.compile(r"\s*[\(\[]\s*amazon\s+renewed\s*[\)\]]\s*", re.IGNORECASE),
]


def clean_title(val: object) -> object:
    if pd.isna(val):
        return val
    s = str(val)
    for rx in _RENEWED_PATTERNS:
        s = rx.sub(" ", s)
    # fazla boşlukları toparla
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    if COL_TITLE_TR not in df.columns:
        print(f"'{COL_TITLE_TR}' sütunu yok. Sütunlar: {list(df.columns)}")
        sys.exit(1)

    before = df[COL_TITLE_TR].astype(str)
    df[COL_TITLE_TR] = df[COL_TITLE_TR].apply(clean_title)
    after = df[COL_TITLE_TR].astype(str)

    changed = (before != after).sum()
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Değişen satır: {changed}")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()

