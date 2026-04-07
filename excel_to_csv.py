#!/usr/bin/env python3
"""
Excel (.xlsx) dosyasını CSV'ye çevirir.
Varsayılan: izgara-body-trimmer-smart-glass_translated.xlsx -> izgara-body-trimmer-smart-glass_translated_from_excel.csv
"""

import sys
from pathlib import Path

import pandas as pd

try:
    import openpyxl  # noqa: F401
except ImportError:
    print("Excel okumak için openpyxl gerekli. Kurulum: pip install openpyxl")
    sys.exit(1)

DEFAULT_INPUT = Path(__file__).parent / "izgara-body-trimmer-smart-glass_translated.xlsx"
DEFAULT_OUTPUT = Path(__file__).parent / "izgara-body-trimmer-smart-glass_translated_from_excel.csv"


def excel_to_csv(input_path: Path, output_path: Path | None = None, sheet_name: str | int = 0) -> Path:
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Excel bulunamadı: {input_path}")

    if output_path is None:
        output_path = input_path.with_suffix(".csv")
    else:
        output_path = Path(output_path)

    df = pd.read_excel(input_path, sheet_name=sheet_name, engine="openpyxl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    return output_path


def main():
    if len(sys.argv) >= 2:
        input_file = Path(sys.argv[1])
        output_file = Path(sys.argv[2]) if len(sys.argv) >= 3 else None
    else:
        input_file = DEFAULT_INPUT
        output_file = DEFAULT_OUTPUT

    try:
        out = excel_to_csv(input_file, output_file)
        print(f"Tamamlandı: {out}")
    except Exception as e:
        print(f"Hata: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()