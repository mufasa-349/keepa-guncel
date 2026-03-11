#!/usr/bin/env python3
"""
Translate Title and Categories: Tree from German to Turkish.
Reads merged_columns.csv, adds Title (TR) and Categories: Tree (TR), saves every 50 rows.
Prints input/output per row, records success per translation, 3s timeout per call.
Output: merged_translated.csv
"""

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
DELAY_SECONDS = 0.5
TRANSLATE_TIMEOUT = 3


def translate_with_timeout(translator, text):
    """Returns (translated_text, success). Uses 3s timeout."""
    if pd.isna(text) or str(text).strip() == "":
        return "", True
    text = str(text).strip()

    def _do_translate():
        return translator.translate(text)

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do_translate)
        try:
            result = fut.result(timeout=TRANSLATE_TIMEOUT)
            return result, True
        except (FuturesTimeoutError, Exception):
            return "", False


def main():
    if not INPUT_FILE.exists():
        print("Input file not found:", INPUT_FILE)
        return

    df = pd.read_csv(INPUT_FILE)
    n = len(df)
    translator = GoogleTranslator(source="de", target="tr")

    if "Title (TR)" not in df.columns:
        df["Title (TR)"] = ""
    if "Categories: Tree (TR)" not in df.columns:
        df["Categories: Tree (TR)"] = ""
    if "Title (TR) success" not in df.columns:
        df["Title (TR) success"] = False
    if "Categories: Tree (TR) success" not in df.columns:
        df["Categories: Tree (TR) success"] = False

    for i in range(n):
        row_num = i + 1
        title_in = df.at[i, "Title"]
        cat_in = df.at[i, "Categories: Tree"]

        # Title
        title_out, title_ok = translate_with_timeout(translator, title_in)
        df.at[i, "Title (TR)"] = title_out
        df.at[i, "Title (TR) success"] = title_ok
        time.sleep(DELAY_SECONDS)

        # Categories: Tree
        cat_out, cat_ok = translate_with_timeout(translator, cat_in)
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

    df.to_csv(OUTPUT_FILE, index=False)
    print("Done. Written to:", OUTPUT_FILE)
    print("Rows:", len(df))


if __name__ == "__main__":
    main()
