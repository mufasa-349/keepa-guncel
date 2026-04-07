#!/usr/bin/env python3
"""
izgara-body-trimmer-smart-glass_translated_from_excel.csv için:

1) "List Price: Current" örn. "129.98 -C0A" formatından fiyatı alır (C0A vb. silinir),
   55 ile çarpar ve yeni sütuna yazar.
2) "Product Codes: EAN" alanında virgülle ayrılmış birden fazla EAN varsa sadece ilkini bırakır.

Varsayılan çıktı: aynı dosya adı + _fixed.csv (orijinali bozmaz).
"""

import re
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path(__file__).parent / "izgara-body-trimmer-smart-glass_translated_from_excel.csv"
DEFAULT_OUTPUT = Path(__file__).parent / "izgara-body-trimmer-smart-glass_translated_from_excel_fixed.csv"

COL_LIST_PRICE = "List Price: Current"
COL_LIST_PRICE_TRY = "List Price (TL)"
COL_EAN = "Product Codes: EAN"

EUR_TO_TRY = 55


_PRICE_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)")


def parse_price(val) -> float | None:
    """'129.98 -C0A' gibi metinden ilk sayıyı çekip float döndürür."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    m = _PRICE_RE.search(s)
    if not m:
        return None
    num = m.group(1).replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def first_ean_only(val):
    if pd.isna(val):
        return val
    s = str(val).strip()
    if not s:
        return s
    if "," not in s:
        return s
    return s.split(",")[0].strip()


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)

    if COL_LIST_PRICE not in df.columns:
        print(f"'{COL_LIST_PRICE}' sütunu yok.")
        sys.exit(1)

    # EAN çoklu ise sadeleştir
    if COL_EAN in df.columns:
        had_multi = df[COL_EAN].astype(str).str.contains(",", na=False).sum()
        df[COL_EAN] = df[COL_EAN].apply(first_ean_only)
        print(f"EAN: {had_multi} satırda çoklu değer vardı, ilki bırakıldı.")

    # Fiyatı TL'ye çevir
    eur = df[COL_LIST_PRICE].apply(parse_price)
    df[COL_LIST_PRICE_TRY] = (pd.to_numeric(eur, errors="coerce") * EUR_TO_TRY).round(2)

    # Yeni sütunu List Price'ın hemen sağına al
    cols = list(df.columns)
    cols.remove(COL_LIST_PRICE_TRY)
    idx = cols.index(COL_LIST_PRICE) + 1
    cols.insert(idx, COL_LIST_PRICE_TRY)
    df = df[cols]

    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()

