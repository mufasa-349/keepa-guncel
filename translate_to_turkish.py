#!/usr/bin/env python3
"""
Translate Title and Categories: Tree from German to Turkish.
Reads merged_columns.csv, adds Title (TR) and Categories: Tree (TR), saves every 50 rows.
Rate limit: on 429/quota, waits RATE_LIMIT_WAIT s and retries (no crash).
Output: merged_translated.csv
"""

import sys
import pandas as pd
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("Install: pip install deep-translator")
    raise

CSVS_DIR = Path(__file__).parent / "csvs"
INPUT_FILE = CSVS_DIR / "merged_columns.csv"
OUTPUT_FILE = CSVS_DIR / "merged_translated.csv"
SAVE_EVERY = 50
DELAY_SECONDS = 0.2  # lower = faster; increase if you hit rate limits
TRANSLATE_TIMEOUT = 3
MAX_CHARS = 5000  # API limit
CHUNK_SIZE = 2500  # translate in chunks to avoid API returning only first word (e.g. "Hayati")
# Titles: only split by comma when long (API sometimes returns first token only for long titles)
TITLE_SEP = ", "
TITLE_COMMA_SPLIT_MIN_LEN = 1500  # above this length use comma-split; below = single fast call
BRAND_PLACEHOLDER = "XBRANDX"  # mask brand so it isn't translated; restore after

# Rate limit safe switch: wait and retry instead of crashing
RATE_LIMIT_WAIT = 60  # seconds to wait when rate limited (429)
MAX_RETRIES = 4  # max attempts per request when rate limited


def is_rate_limit_error(e):
    """True if exception looks like API rate limit (429, quota, too many requests)."""
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


def _translate_one(translator, text):
    """Run one translate call with timeout. Returns (result, success, error)."""
    def _do():
        return translator.translate(text)

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=TRANSLATE_TIMEOUT), True, None
        except (FuturesTimeoutError, Exception) as e:
            return "", False, e


def translate_with_retry(translator, text):
    """Translate with rate-limit handling: wait and retry instead of crashing. Returns (result, success)."""
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
    """Return text up to max_len chars, cutting at last space so no word is split."""
    s = str(text).strip()
    if len(s) <= max_len:
        return s
    cut = s[:max_len]
    last_space = cut.rfind(" ")
    if last_space == -1:
        return s[:max_len]
    return cut[:last_space]


def split_into_chunks(text, max_len=CHUNK_SIZE):
    """Split text into chunks at word boundaries (no word cut in half)."""
    s = str(text).strip()
    if len(s) <= max_len:
        return [s] if s else []
    chunks = []
    while s:
        cut = truncate_at_word_boundary(s, max_len)
        chunks.append(cut)
        s = s[len(cut) :].lstrip()
    return chunks


def mask_brand(text, brand):
    """Replace brand name with placeholder so API doesn't translate it (avoids 'Crucial' -> 'Hayati' only)."""
    if pd.isna(text) or pd.isna(brand) or str(brand).strip() == "":
        return str(text).strip()
    return str(text).strip().replace(str(brand).strip(), BRAND_PLACEHOLDER)


def unmask_brand(text, brand):
    """Restore brand name in translated text."""
    if pd.isna(text) or pd.isna(brand) or str(brand).strip() == "":
        return str(text) if not pd.isna(text) else ""
    return str(text).replace(BRAND_PLACEHOLDER, str(brand).strip())


def translate_with_timeout(translator, text, chunked=True):
    """Returns (translated_text, success). Uses 3s timeout + rate-limit retry. If chunked, splits long text."""
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    text = str(text).strip()
    text = truncate_at_word_boundary(text, MAX_CHARS)

    if not chunked or len(text) <= CHUNK_SIZE:
        return translate_with_retry(translator, text)
    # Chunk and translate each part, then join (each part uses rate-limit retry)
    chunks = split_into_chunks(text, CHUNK_SIZE)
    results = []
    for chunk in chunks:
        part, _ = translate_with_retry(translator, chunk)
        results.append(part)
        time.sleep(0.1)
    joined = " ".join(results)
    ok = len(results) == len(chunks) and all(r != "" for r in results)
    return joined, ok


def translate_title_by_parts(translator, text):
    """
    Short titles: single API call (fast). Long titles: split by comma and translate each part
    (API sometimes returns only first token for long title).
    """
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    s = str(text).strip()
    s = truncate_at_word_boundary(s, MAX_CHARS)
    # Fast path: short title = one call
    if len(s) <= TITLE_COMMA_SPLIT_MIN_LEN or TITLE_SEP not in s:
        return translate_with_timeout(translator, s, chunked=False)
    # Long title: comma-split to avoid API returning only first token
    parts = [p.strip() for p in s.split(TITLE_SEP) if p.strip()]
    if not parts:
        return "", True
    results = []
    for part in parts:
        out, ok = translate_with_timeout(translator, part, chunked=False)
        results.append(out if out else part)
        time.sleep(0.15)
    joined = TITLE_SEP.join(results)
    ok = all(r != "" for r in results)
    return joined, ok


def main():
    # Optional: test a single row, e.g. python translate_to_turkish.py 113
    test_row = None
    if len(sys.argv) > 1:
        try:
            test_row = int(sys.argv[1])  # 1-based row number (e.g. 113)
        except ValueError:
            pass

    if not INPUT_FILE.exists():
        print("Input file not found:", INPUT_FILE)
        return

    df = pd.read_csv(INPUT_FILE)
    n = len(df)
    if test_row is not None:
        if test_row < 1 or test_row > n:
            print(f"Row must be 1..{n}, got {test_row}")
            return
        print(f"Test mode: only row {test_row}\n")
    translator = GoogleTranslator(source="de", target="tr")

    if "Title (TR)" not in df.columns:
        df["Title (TR)"] = ""
    if "Categories: Tree (TR)" not in df.columns:
        df["Categories: Tree (TR)"] = ""
    if "Title (TR) success" not in df.columns:
        df["Title (TR) success"] = False
    if "Categories: Tree (TR) success" not in df.columns:
        df["Categories: Tree (TR) success"] = False

    indices = [test_row - 1] if test_row is not None else range(n)
    for i in indices:
        row_num = i + 1
        title_in = df.at[i, "Title"]
        cat_in = df.at[i, "Categories: Tree"]
        brand = df.at[i, "Brand"] if "Brand" in df.columns else ""

        # Mask brand so it won't be translated (avoids "Crucial" -> "Hayati" only)
        title_masked = mask_brand(title_in, brand)
        cat_masked = mask_brand(cat_in, brand)

        # Title and Categories in parallel (fast). Title uses comma-split only when long.
        with ThreadPoolExecutor(max_workers=2) as ex:
            title_fut = ex.submit(translate_title_by_parts, translator, title_masked)
            cat_fut = ex.submit(translate_with_timeout, translator, cat_masked)
            title_out, title_ok = title_fut.result()
            cat_out, cat_ok = cat_fut.result()
        time.sleep(DELAY_SECONDS)

        # Restore brand name in output
        title_out = unmask_brand(title_out, brand)
        cat_out = unmask_brand(cat_out, brand)

        df.at[i, "Title (TR)"] = title_out
        df.at[i, "Title (TR) success"] = title_ok
        df.at[i, "Categories: Tree (TR)"] = cat_out
        df.at[i, "Categories: Tree (TR) success"] = cat_ok
        time.sleep(DELAY_SECONDS)

        # Progress to terminal: input and output for every row
        print(f"--- Row {row_num}/{n} ---")
        print(f"  Title (DE): {str(title_in)[:80]}{'...' if len(str(title_in)) > 80 else ''}")
        print(f"  Title (TR): {title_out[:80]}{'...' if len(title_out) > 80 else ''} [{'OK' if title_ok else 'FAIL'}]")
        print(f"  Categories (DE): {str(cat_in)[:80]}{'...' if len(str(cat_in)) > 80 else ''}")
        print(f"  Categories (TR): {cat_out[:80]}{'...' if len(cat_out) > 80 else ''} [{'OK' if cat_ok else 'FAIL'}]")

        if (row_num) % SAVE_EVERY == 0:
            df.to_csv(OUTPUT_FILE, index=False)
            print(f">>> Saved {row_num}/{n} products")

    if test_row is None:
        df.to_csv(OUTPUT_FILE, index=False)
        print("Done. Written to:", OUTPUT_FILE)
        print("Rows:", len(df))
    else:
        print("\n(Test run: output not saved to CSV)")


if __name__ == "__main__":
    main()
