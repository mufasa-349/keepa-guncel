#!/usr/bin/env python3
"""
Türkçe başlığı hatalı satırları siler:
1) Title (TR) ilk kelimesi Brand ile aynı olmayan satırlar (ürün adı değil kategori/yanlış çeviri)
2) Title (TR) çok kısa olanlar (örn. sadece "Crucial" gibi tek kelime)
Kalan satırlar yeni CSV'ye yazılır (varsayılan: aynı isim + _filtered).
"""

import sys
import re
from pathlib import Path

import pandas as pd

CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged_translated.csv"
DEFAULT_OUTPUT = CSVS_DIR / "merged_translated_filtered.csv"

# Title (TR) en az bu kadar karakter olmalı (sadece "Crucial" gibi tek kelime elenir)
MIN_TITLE_CHARS = 20
# Veya en az bu kadar kelime (boşlukla ayrılmış)
MIN_TITLE_WORDS = 2


def first_word(text):
    """Metnin ilk kelimesini döndürür (boşluk/punctuation ile ayrılmış)."""
    if pd.isna(text):
        return ""
    s = str(text).strip()
    if not s:
        return ""
    # İlk token: boşluk veya yaygın ayraçlara göre böl
    m = re.match(r"^([^\s›\-,;]+)", s)
    return m.group(1).strip() if m else ""


def word_count(text):
    """Boşlukla ayrılmış kelime sayısı."""
    if pd.isna(text):
        return 0
    return len(str(text).strip().split())


def normalize_for_compare(s):
    """Brand / ilk kelime karşılaştırması için küçük harf, boşluksuz."""
    if pd.isna(s):
        return ""
    return str(s).strip().lower().replace(" ", "")


def should_remove(row, col_title_tr, col_brand):
    """
    True = bu satır silinsin.
    Silme: 1) İlk kelime != Brand  2) Title (TR) çok kısa
    """
    title_tr = row.get(col_title_tr)
    brand = row.get(col_brand)

    # Çok kısa başlık (sadece "Crucial" vb.)
    if pd.isna(title_tr) or str(title_tr).strip() == "":
        return True
    s = str(title_tr).strip()
    if len(s) < MIN_TITLE_CHARS or word_count(s) < MIN_TITLE_WORDS:
        return True

    # Brand yoksa ilk kelime kontrolü yapma (sadece kısalık kontrolü yaptık)
    if pd.isna(brand) or str(brand).strip() == "":
        return False

    first = first_word(title_tr)
    if not first:
        return True
    # İlk kelime ile marka eşleşmeli (büyük/küçük harf ve boşluk farkı yok)
    if normalize_for_compare(first) != normalize_for_compare(brand):
        return True

    return False


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    print("Okunuyor:", input_path)
    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    n_before = len(df)

    if "Title (TR)" not in df.columns:
        print("'Title (TR)' sütunu yok.")
        sys.exit(1)
    col_brand = "Brand" if "Brand" in df.columns else None
    if not col_brand:
        print("Uyarı: 'Brand' sütunu yok; sadece kısa başlık filtresi uygulanacak.")

    mask_remove = df.apply(
        lambda row: should_remove(row, "Title (TR)", col_brand or ""),
        axis=1,
    )
    n_removed = mask_remove.sum()
    df_out = df[~mask_remove].copy()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    print(f"Önce: {n_before} satır")
    print(f"Silinen: {n_removed} satır")
    print(f"Sonra: {len(df_out)} satır")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()
