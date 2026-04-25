#!/usr/bin/env python3
"""
brands-1/KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor.csv dosyasında
Title ve Categories: Tree sütunlarını Türkçeye çevirip iki yeni sütun yazar:
  - Title (TR)
  - Categories: Tree (TR)

Notlar:
- Çeviri sıralı yapılır (karışma olmasın diye).
- --start N ile N. satırdan devam edebilirsiniz (1 = ilk veri satırı).
- Her SAVE_EVERY satırda ara kayıt alır.
"""

import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import pandas as pd

try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("Kurulum: pip install deep-translator")
    raise


BASE_DIR = Path(__file__).parent
DEFAULT_INPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor.csv"
DEFAULT_OUTPUT = BASE_DIR / "KeepaExport-2026-04-25-ProductFinder_stripped_nonempty_noauthor_tr.csv"

COL_TITLE = "Title"
COL_CAT = "Categories: Tree"
COL_TITLE_TR = "Title (TR)"
COL_CAT_TR = "Categories: Tree (TR)"

SAVE_EVERY = 50
DELAY_SECONDS = 0.25
TRANSLATE_TIMEOUT = 5
RATE_LIMIT_WAIT = 60
MAX_RETRIES = 4
MAX_CHARS = 5000


def _short(s: object, n: int = 110) -> str:
    """Terminal için kısaltılmış metin."""
    if pd.isna(s):
        return ""
    out = str(s).replace("\n", " ").strip()
    return out if len(out) <= n else out[:n] + "..."


def is_rate_limit_error(e) -> bool:
    if e is None:
        return False
    msg = str(e).lower()
    return (
        "429" in msg
        or "rate" in msg
        or "limit" in msg
        or "too many" in msg
        or "quota" in msg
        or "blocked" in msg
    )


def _translate_one(translator, text: str):
    def _do():
        return translator.translate(text)

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=TRANSLATE_TIMEOUT), True, None
        except (FuturesTimeoutError, Exception) as e:
            return "", False, e


def translate_with_retry(translator, text: object):
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    s = str(text).strip()[:MAX_CHARS]
    for attempt in range(MAX_RETRIES):
        out, ok, err = _translate_one(translator, s)
        if ok:
            return out, True
        if err and is_rate_limit_error(err) and attempt < MAX_RETRIES - 1:
            print(f"\n[Rate limit] {RATE_LIMIT_WAIT} saniye bekleniyor (deneme {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(RATE_LIMIT_WAIT)
            continue
        return "", False
    return "", False


def parse_args():
    raw = sys.argv[1:]
    start_row = 1
    i = 0
    while i < len(raw):
        if raw[i] in ("--start", "--from", "-s") and i + 1 < len(raw):
            start_row = max(1, int(raw[i + 1]))
            raw = raw[:i] + raw[i + 2 :]
            continue
        i += 1

    input_path = Path(raw[0]) if len(raw) >= 1 else DEFAULT_INPUT
    output_path = Path(raw[1]) if len(raw) >= 2 else DEFAULT_OUTPUT

    if not input_path.is_absolute():
        input_path = BASE_DIR / input_path
    if not output_path.is_absolute():
        output_path = BASE_DIR / output_path

    # Devam modunda çıktı verilmediyse üzerine yazmak daha pratik
    if start_row > 1 and len(raw) < 2:
        output_path = input_path

    return input_path, output_path, start_row


def main():
    input_path, output_path, start_row = parse_args()
    if not input_path.exists():
        print("Dosya bulunamadı:", input_path)
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8", quoting=1)
    n = len(df)
    if start_row > n:
        print(f"--start {start_row} dosyadaki satır sayısından ({n}) büyük.")
        sys.exit(1)

    for col in (COL_TITLE, COL_CAT):
        if col not in df.columns:
            print(f"Gerekli sütun yok: {col}")
            sys.exit(1)

    if COL_TITLE_TR not in df.columns:
        df[COL_TITLE_TR] = ""
    if COL_CAT_TR not in df.columns:
        df[COL_CAT_TR] = ""

    if start_row > 1:
        print(f"Kaldığı yerden devam: satır {start_row}–{n}. Çıktı: {output_path}\n")

    translator = GoogleTranslator(source="de", target="tr")

    for i in range(start_row - 1, n):
        row_num = i + 1

        # Eğer zaten doluysa atla (resume-friendly)
        if str(df.at[i, COL_TITLE_TR]).strip() and str(df.at[i, COL_CAT_TR]).strip():
            continue

        title_de = df.at[i, COL_TITLE]
        cat_de = df.at[i, COL_CAT]

        title_tr, ok1 = translate_with_retry(translator, title_de)
        time.sleep(DELAY_SECONDS)
        cat_tr, ok2 = translate_with_retry(translator, cat_de)
        time.sleep(DELAY_SECONDS)

        df.at[i, COL_TITLE_TR] = title_tr
        df.at[i, COL_CAT_TR] = cat_tr

        print(f"--- {row_num}/{n} ---")
        print(f"  Title (DE): {_short(title_de)}")
        print(f"  Title (TR): {_short(title_tr)} [{'OK' if ok1 else 'FAIL'}]")
        print(f"  Categories (DE): {_short(cat_de)}")
        print(f"  Categories (TR): {_short(cat_tr)} [{'OK' if ok2 else 'FAIL'}]")

        if row_num % SAVE_EVERY == 0:
            df.to_csv(output_path, index=False, encoding="utf-8")
            print(f">>> Saved {row_num}/{n}")

    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Done. Written to:", output_path)


if __name__ == "__main__":
    main()

