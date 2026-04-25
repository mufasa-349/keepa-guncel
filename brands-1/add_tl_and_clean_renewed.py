#!/usr/bin/env python3
"""
Çıktı dosyasında 2 işlem yapar:

1) "List Price: Current" değerini 55 ile çarpıp hemen yanına "TL fiyat" sütunu ekler.
2) Ürün adlarında (öncelik: "Title (TR)", yoksa "Title") geçen renewed/yenilenmiş ibarelerini
   sadece başlıktan siler (ürün normal adı kalsın).

Varsayılan:
  brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr.csv
    -> brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean.csv
"""

import re
import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parent
DEFAULT_INPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr.csv"
DEFAULT_OUTPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr_clean.csv"

COL_PRICE = "List Price: Current"
COL_TL = "TL fiyat"
COL_TITLE = "Title"
COL_TITLE_TR = "Title (TR)"

MULTIPLIER = 55

_PRICE_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)")

# Parantez / köşeli parantez / tire vb. varyasyonları da yakalasın diye esnek tuttuk
_RENEWED_PATTERNS = [
    re.compile(r"\s*[\(\[]\s*amazon\s+renewed\s*[\)\]]\s*", re.IGNORECASE),
    re.compile(r"\s*[\(\[]\s*renewed\s*[\)\]]\s*", re.IGNORECASE),
    re.compile(r"\s*[\(\[]\s*yenilenmiş\s*[\)\]]\s*", re.IGNORECASE),
    re.compile(r"\s*[\(\[]\s*yenilenmis\s*[\)\]]\s*", re.IGNORECASE),
    # Parantezsiz geçen basit ekler (son/baş gibi)
    re.compile(r"(\s+|^)(amazon\s+renewed|renewed|yenilenmiş|yenilenmis)(\s+|$)", re.IGNORECASE),
]


def parse_price(val) -> float | None:
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


def clean_renewed(text: object) -> object:
    if pd.isna(text):
        return text
    s = str(text)
    for rx in _RENEWED_PATTERNS:
        s = rx.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # Eğer başta/sonda kalan '-' gibi ayraçlar olduysa toparla
    s = re.sub(r"^\s*[-–—]\s*", "", s).strip()
    s = re.sub(r"\s*[-–—]\s*$", "", s).strip()
    return s


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    # Kullanıcı argümanını önce olduğu gibi dene (CWD'ye göre). Bulunamazsa brands-1 altına bak.
    if not input_path.is_absolute() and not input_path.exists():
        alt = BASE_DIR / input_path
        if alt.exists():
            input_path = alt

    # Çıktı yolu: mutlak değilse olduğu gibi bırak (CWD'ye göre yazsın).
    # Eğer kullanıcı "out.csv" gibi kısa isim verirse ve script brands-1 içinde çalışıyorsa
    # yine brands-1/out.csv oluşsun diye, parent yoksa brands-1 altına yaz.
    if not output_path.is_absolute() and output_path.parent == Path("."):
        output_path = BASE_DIR / output_path

    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)

    if COL_PRICE not in df.columns:
        print(f"'{COL_PRICE}' sütunu yok.")
        sys.exit(1)

    # 1) TL fiyat ekle
    eur = df[COL_PRICE].apply(parse_price)
    df[COL_TL] = (pd.to_numeric(eur, errors="coerce") * MULTIPLIER).round(2)

    # Sütunu fiyatın hemen yanına taşı
    cols = list(df.columns)
    cols.remove(COL_TL)
    idx = cols.index(COL_PRICE) + 1
    cols.insert(idx, COL_TL)
    df = df[cols]

    # 2) Renewed / Yenilenmiş temizliği
    title_col = COL_TITLE_TR if COL_TITLE_TR in df.columns else (COL_TITLE if COL_TITLE in df.columns else None)
    if title_col:
        before = df[title_col].fillna("").astype(str)
        df[title_col] = df[title_col].apply(clean_renewed)
        after = df[title_col].fillna("").astype(str)
        changed = int((before != after).sum())
        print(f"Başlık temizliği ({title_col}): değişen satır = {changed}")
    else:
        print("Uyarı: Title (TR) / Title sütunu bulunamadı, başlık temizliği atlandı.")

    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Yazıldı:", output_path)


if __name__ == "__main__":
    main()

