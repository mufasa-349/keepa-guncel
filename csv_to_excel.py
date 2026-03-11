#!/usr/bin/env python3
"""
CSV dosyasını Excel (.xlsx) formatına dönüştürür.
Encoding, tırnak içi virgüller ve özel karakterler sorunsuz işlenir.
"""

import sys
from pathlib import Path

import pandas as pd

try:
    import openpyxl  # noqa: F401
except ImportError:
    print("Excel yazmak için openpyxl gerekli. Kurulum: pip install openpyxl")
    sys.exit(1)

# Varsayılan klasör ve dosya
CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged.csv"
DEFAULT_OUTPUT = CSVS_DIR / "merged.xlsx"

# Excel satır limiti (Excel max ~1.048.576 satır)
EXCEL_MAX_ROWS = 1_048_576


def csv_to_excel(
    input_path: Path,
    output_path: Path | None = None,
    encoding: str = "utf-8",
    delimiter: str = ",",
    sheet_name: str = "Sheet1",
) -> Path:
    """
    CSV dosyasını okuyup Excel'e yazar.

    Args:
        input_path: CSV dosya yolu
        output_path: Çıktı Excel yolu (None ise input adına .xlsx eklenir)
        encoding: CSV encoding (utf-8, utf-8-sig, latin-1 vb.)
        delimiter: CSV ayırıcı (varsayılan virgül)
        sheet_name: Excel sayfa adı

    Returns:
        Yazılan Excel dosyasının yolu
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"CSV bulunamadı: {input_path}")

    if output_path is None:
        output_path = input_path.with_suffix(".xlsx")
    else:
        output_path = Path(output_path)

    # Encoding denemeleri (önce verilen, sonra yaygın olanlar)
    encodings_to_try = [encoding, "utf-8", "utf-8-sig", "latin-1", "cp1252"]
    df = None
    last_error = None

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(
                input_path,
                encoding=enc,
                delimiter=delimiter,
                quoting=1,  # QUOTE_MINIMAL - tırnak içi virgülleri doğru parse eder
                on_bad_lines="warn",  # Hatalı satırları atlayıp uyarı verir (pandas >= 1.3)
            )
            break
        except Exception as e:
            last_error = e
            continue

    if df is None:
        raise ValueError(f"CSV okunamadı (denenen encoding'ler: {encodings_to_try}). Son hata: {last_error}")

    # Excel satır limiti kontrolü (başlık + veri)
    if len(df) + 1 > EXCEL_MAX_ROWS:
        print(f"Uyarı: {len(df)} satır var; Excel en fazla {EXCEL_MAX_ROWS} satır destekler. İlk {EXCEL_MAX_ROWS - 1} satır yazılacak.")
        df = df.head(EXCEL_MAX_ROWS - 1)

    # NaN'ları boş string yapmıyoruz; Excel zaten boş gösterir. Gerekirse: df = df.fillna("")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_excel(
        output_path,
        index=False,
        sheet_name=sheet_name,
        engine="openpyxl",
    )

    return output_path


def main():
    if len(sys.argv) >= 2:
        input_file = Path(sys.argv[1])
        output_file = Path(sys.argv[2]) if len(sys.argv) >= 3 else None
    else:
        input_file = DEFAULT_INPUT
        output_file = DEFAULT_OUTPUT

    try:
        out = csv_to_excel(input_file, output_file)
        print(f"Tamamlandı: {out}")
    except FileNotFoundError as e:
        print(f"Hata: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Hata: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
