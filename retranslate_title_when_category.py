#!/usr/bin/env python3
"""
Title (TR) sütununda ürün adı yerine kategori adı yazan satırları bulur;
Almanca Title'ı tekrar Türkçe'ye çevirip Title (TR) ve Title (TR) success günceller.
Girdi: merged_translated.csv (veya belirtilen CSV). Çıktı: aynı dosya güncellenir.
"""

import sys
import time
from pathlib import Path

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("Kurulum: pip install deep-translator")
    raise

CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged_translated.csv"
SAVE_EVERY = 25
DELAY_SECONDS = 0.25
TRANSLATE_TIMEOUT = 5
MAX_CHARS = 5000
CHUNK_SIZE = 2500
TITLE_SEP = ", "
TITLE_COMMA_SPLIT_MIN_LEN = 1500
BRAND_PLACEHOLDER = "XBRANDX"
RATE_LIMIT_WAIT = 60
MAX_RETRIES = 4


def is_rate_limit_error(e):
    if e is None:
        return False
    msg = str(e).lower()
    return "429" in msg or "rate" in msg or "limit" in msg or "too many" in msg or "quota" in msg or "blocked" in msg


def _translate_one(translator, text):
    def _do():
        return translator.translate(text)

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=TRANSLATE_TIMEOUT), True, None
        except (FuturesTimeoutError, Exception) as e:
            return "", False, e


def translate_with_retry(translator, text):
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    for attempt in range(MAX_RETRIES):
        result, ok, err = _translate_one(translator, text)
        if ok:
            return result, True
        if err and is_rate_limit_error(err) and attempt < MAX_RETRIES - 1:
            print(f"\n[Rate limit] {RATE_LIMIT_WAIT} saniye bekleniyor (deneme {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(RATE_LIMIT_WAIT)
            continue
        return "", False
    return "", False


def truncate_at_word_boundary(text, max_len):
    s = str(text).strip()
    if len(s) <= max_len:
        return s
    cut = s[:max_len]
    last_space = cut.rfind(" ")
    if last_space == -1:
        return s[:max_len]
    return cut[:last_space]


def mask_brand(text, brand):
    if pd.isna(text) or pd.isna(brand) or str(brand).strip() == "":
        return str(text).strip()
    return str(text).strip().replace(str(brand).strip(), BRAND_PLACEHOLDER)


def unmask_brand(text, brand):
    if pd.isna(text) or pd.isna(brand) or str(brand).strip() == "":
        return str(text) if not pd.isna(text) else ""
    return str(text).replace(BRAND_PLACEHOLDER, str(brand).strip())


def translate_title_by_parts(translator, text):
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    s = truncate_at_word_boundary(str(text).strip(), MAX_CHARS)
    if len(s) <= TITLE_COMMA_SPLIT_MIN_LEN or TITLE_SEP not in s:
        return translate_with_retry(translator, s)
    parts = [p.strip() for p in s.split(TITLE_SEP) if p.strip()]
    if not parts:
        return "", True
    results = []
    for part in parts:
        out, ok = translate_with_retry(translator, part)
        results.append(out if out else part)
        time.sleep(0.15)
    return TITLE_SEP.join(results), all(r != "" for r in results)


def title_tr_equals_category(row):
    """Title (TR) ile Categories: Tree (TR) aynı veya çok benzer mi (kategori yanlışlıkla başlık olmuş)."""
    title_tr = row.get("Title (TR)")
    cat_tr = row.get("Categories: Tree (TR)")
    if pd.isna(title_tr) or pd.isna(cat_tr):
        return False
    a, b = str(title_tr).strip(), str(cat_tr).strip()
    if a == b:
        return True
    # Biri diğerinin başında da olabilir (kategori kısaltılmış yazılmış)
    if a.startswith(b) or b.startswith(a):
        return True
    return False


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_INPUT
    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    print("Okunuyor:", input_path)
    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    if "Title (TR)" not in df.columns or "Categories: Tree (TR)" not in df.columns:
        print("Gerekli sütunlar yok: Title (TR), Categories: Tree (TR)")
        sys.exit(1)

    # Title (TR) == kategori olan satırların indeksleri
    mask = df.apply(title_tr_equals_category, axis=1)
    indices = df.index[mask].tolist()
    total = len(indices)
    if total == 0:
        print("Title (TR) = kategori olan satır bulunamadı. Çıkılıyor.")
        return

    print(f"Yeniden çevrilecek satır sayısı: {total}\n")
    translator = GoogleTranslator(source="de", target="tr")
    brand_col = "Brand" if "Brand" in df.columns else None

    for k, i in enumerate(indices, 1):
        title_de = df.at[i, "Title"]
        brand = df.at[i, brand_col] if brand_col else ""
        title_masked = mask_brand(title_de, brand)
        title_tr_new, ok = translate_title_by_parts(translator, title_masked)
        title_tr_new = unmask_brand(title_tr_new, brand)

        df.at[i, "Title (TR)"] = title_tr_new
        df.at[i, "Title (TR) success"] = ok

        print(f"[{k}/{total}] Satır {i + 1} | DE: {str(title_de)[:60]}...")
        print(f"         TR: {title_tr_new[:60]}{'...' if len(title_tr_new) > 60 else ''} [{'OK' if ok else 'FAIL'}]")
        time.sleep(DELAY_SECONDS)

        if k % SAVE_EVERY == 0:
            df.to_csv(input_path, index=False, encoding="utf-8")
            print(f">>> Ara kayıt: {input_path}")

    df.to_csv(input_path, index=False, encoding="utf-8")
    print(f"\nTamamlandı. {total} satır güncellendi. Dosya: {input_path}")


if __name__ == "__main__":
    main()
