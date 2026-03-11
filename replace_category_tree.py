#!/usr/bin/env python3
"""
"Categories: Tree" (Almanca) ile eşleşen satırlarda sadece "Categories: Tree (TR)" güncellenir.
Eşleşme: Almanca metin verilen ifadeyle BAŞLIYORSA (önek eşleşmesi) güncellenir; en uzun eşleşme kullanılır.
Almanca sütun değiştirilmez. CSV veya Excel giriş/çıkış desteklenir.
"""

import sys
from pathlib import Path

import pandas as pd

try:
    import openpyxl  # noqa: F401
except ImportError:
    openpyxl = None

CSVS_DIR = Path(__file__).parent / "csvs"
DEFAULT_INPUT = CSVS_DIR / "merged_translated.csv"
DEFAULT_OUTPUT = CSVS_DIR / "merged_translated.csv"  # üzerine yazılır; farklı isim istersen değiştir

# Almanca (önek) -> Türkçe kategori ağacı. Satır "Almanca" ile BAŞLIYORSA TR atanır; en uzun eşleşme geçerli.
CATEGORY_DE_TO_TR = {
    # Önek eşleşmeleri (ilk kısım eşleşenler; alt kategoriler dahil)
    "Computer & Zubehör › Computer-Zubehör › Audio & Video Zubehör": "Bilgisayarlar ve Aksesuarlar › Bilgisayar Aksesuarları › Ses ve Video Aksesuarları",
    "Computer & Zubehör › Computer-Zubehör › Monitor-Zubehör": "Bilgisayarlar ve Aksesuarlar › Bilgisayar Aksesuarları › Monitör Aksesuarları",
    "Computer & Zubehör › Datenspeicher › Externe Datenspeicher": "Bilgisayarlar ve Aksesuarlar › Veri Depolama › Harici Veri Depolama",
    "Computer & Zubehör › Datenspeicher › Interner Speicher": "Bilgisayarlar ve Aksesuarlar › Veri Depolama › Dahili Depolama",
    "Computer & Zubehör › Komponenten & Ersatzteile": "Bilgisayarlar ve Aksesuarlar › Bileşenler ve Yedek Parçalar",
    # Tam yol eşleşmeleri (Speicherkarten vb.)
    "Computer & Zubehör › Datenspeicher › Externe Datenspeicher › Speicherkarten › Micro SD": "Bilgisayarlar ve Aksesuarlar › Veri Depolama › Harici Veri Depolama › Hafıza Kartları › Micro SD",
    "Computer & Zubehör › Datenspeicher › Externe Datenspeicher › Speicherkarten › SecureDigital-Cards": "Bilgisayarlar ve Aksesuarlar › Veri Depolama › Harici Veri Depolama › Hafıza Kartları › Dijital Kartlar",
    # Datenspeicher - USB & Festplatten
    "Computer & Zubehör › Datenspeicher › Externe Datenspeicher › USB-Sticks": "Bilgisayarlar ve Aksesuarlar › Veri depolama › Harici veri depolama › USB Diskler",
    "Computer & Zubehör › Datenspeicher › Externe Datenspeicher › Externe Festplatten": "Bilgisayarlar ve Aksesuarlar › Veri Depolama › Harici Veri Depolama › Harici Sabit Sürücüler",
    # Baumarkt - Elektroinstallation
    "Baumarkt › Elektroinstallation › Schalter & Dimmer › Dimmschalter": "Ev & Yaşam › Elektrik tesisatı › Anahtarlar ve dimmerler › Kısma anahtarları",
    "Baumarkt › Elektroinstallation › Schalter & Dimmer › Lichtschalter": "Ev & Yaşam › Elektrik tesisatı › Anahtarlar ve dimmerler › Işık anahtarları",
    "Baumarkt › Elektroinstallation › Steckdosen & Zubehör › Smart & Ferngesteuerte Stecker": "Ev & Yaşam › Elektrik tesisatı › Prizler ve aksesuarlar › Akıllı ve uzaktan kumandalı prizler",
    "Baumarkt › Sicherheitstechnik › Überwachungstechnik › Videoüberwachungstechnik › Überwachungskameras": "Hırdavat mağazası › Güvenlik teknolojisi › Gözetim teknolojisi › Video gözetim teknolojisi › Gözetim kameraları",
    # Beleuchtung - Außen
    "Beleuchtung › Außenbeleuchtung › Außenwandleuchten": "Aydınlatma › Dış Mekan Aydınlatması › Dış Mekan Duvar Lambaları",
    "Beleuchtung › Außenbeleuchtung › Terrassen- & Verandabeleuchtung › Hängelampen": "Aydınlatma › Dış Mekan Aydınlatması › Bahçe ve Veranda Aydınlatması › Sarkıt Lambalar",
    "Beleuchtung › Außenbeleuchtung › Wegeleuchten": "Aydınlatma › Dış aydınlatma › Yol ışıkları",
    # Beleuchtung - Bad
    "Beleuchtung › Bad-Beleuchtung › Einbauleuchten": "Aydınlatma › Banyo Aydınlatması › Gömme Aydınlatmalar",
    # Beleuchtung - Innen (Decken, Hänge, Schrank, Spezial, Spot, Tisch, Wand)
    "Beleuchtung › Innenbeleuchtung › Deckenbeleuchtung › Deckenleuchten": "Aydınlatma › İç aydınlatma › Tavan aydınlatması › Tavan lambaları",
    "Beleuchtung › Innenbeleuchtung › Deckenbeleuchtung › Hänge- & Pendelleuchten": "Aydınlatma › İç aydınlatma › Tavan aydınlatması › Asma ve sarkıt lambalar",
    "Beleuchtung › Innenbeleuchtung › Schrank Unterbauleuchten": "Aydınlatma › İç Mekan Aydınlatması › Dolap Altı Aydınlatmaları",
    "Beleuchtung › Innenbeleuchtung › Spezial- & Stimmungsbeleuchtung › LED Streifen": "Aydınlatma › İç aydınlatma › Özel Aydınlatma › LED şeritler",
    "Beleuchtung › Innenbeleuchtung › Spezial- & Stimmungsbeleuchtung › Stimmungslichter": "Aydınlatma › İç aydınlatma › Özel ve Mod aydınlatması › Mod ışıkları",
    "Beleuchtung › Innenbeleuchtung › Spotleuchten & Spotbalken › Deckenspots": "Aydınlatma › İç aydınlatma › Spot ışıklar ve spot çubuklar › Tavan spotları",
    "Beleuchtung › Innenbeleuchtung › Spotleuchten & Spotbalken › Schienen- & Seilsysteme": "Aydınlatma › İç Mekan Aydınlatması › Spot Işıklar ve Spot Işık Çubukları › Raylı ve Kablolu Sistemler",
    "Beleuchtung › Innenbeleuchtung › Spotleuchten & Spotbalken › Schienen- & Seilsysteme › Komplett-Kits": "Aydınlatma › İç Mekan Aydınlatması › Spot Işıklar ve Işık Barları › Raylı ve Kablolu Sistemler › Komple Setler",
    "Beleuchtung › Innenbeleuchtung › Spotleuchten & Spotbalken › Schienen- & Seilsysteme › Laufbahnen & Schienen": "Aydınlatma › İç Mekan Aydınlatması › Spot Işıklar ve Spot Işık Çubukları › Ray ve Kablo Sistemleri › Raylar ve Ray Sistemleri",
    "Beleuchtung › Innenbeleuchtung › Spotleuchten & Spotbalken › Wandspots": "Aydınlatma › İç aydınlatma › Spot ışıklar ve spot çubuklar › Duvar spotları",
    "Beleuchtung › Innenbeleuchtung › Tisch- & Stehleuchten › Standleuchten & Deckenfluter": "Aydınlatma › İç aydınlatma › Masa ve zemin lambaları › Zemin lambaları ve tavan projektörleri",
    "Beleuchtung › Innenbeleuchtung › Tisch- & Stehleuchten › Tischlampen": "Aydınlatma › İç aydınlatma › Masa ve zemin lambaları › Masa lambaları",
    "Beleuchtung › Innenbeleuchtung › Wandbeleuchtung › Wandleuchten": "Aydınlatma › İç Mekan Aydınlatması › Duvar Aydınlatması › Duvar Lambaları",
    # Beleuchtung - Leuchtmittel & Lichterketten
    "Beleuchtung › Leuchtmittel › LED Lampen": "Aydınlatma › Ampuller › LED Lambalar",
    "Beleuchtung › Leuchtmittel": "Aydınlatma › Ampuller",
    "Beleuchtung › Leuchtmittel › WLAN-Lampen": "Aydınlatma › Ampuller › WiFi lambaları",
    "Beleuchtung › Lichterketten › Außen & Innen": "Aydınlatma › Peri ışıkları › Dış ve İç Mekan",
    "Beleuchtung › Lichtschläuche": "Aydınlatma › Işık tüpleri",
    # Computer - Mäuse, Monitore
    "Computer & Zubehör › Mäuse, Tastaturen & Eingabegeräte": "Bilgisayarlar ve Aksesuarlar › Fareler, Klavyeler ve Giriş Cihazları › Klavyeler",
    "Computer & Zubehör › Monitore": "Bilgisayarlar ve Aksesuarlar › Monitörler",
    # Elektronik & Foto
    "Elektronik & Foto › Auto- & Fahrzeugelektronik › Auto-Elektronik › TV & Video › Autokameras": "Elektronik ve Fotoğraf › Araç ve Araç Elektroniği › Araç Elektroniği › TV ve Video › Araç Kameraları",
    "Elektronik & Foto › Fernseher & Heimkino › Blu-ray-Player & -Rekorder › Blu-ray-Player": "Elektronik ve Fotoğraf › TV'ler ve Ev Sineması › Blu-ray Oynatıcılar ve Kayıt Cihazları › Blu-ray Oynatıcılar",
    "Elektronik & Foto › Fernseher & Heimkino › Blu-ray-Player & -Rekorder › Blu-ray-Rekorder": "Elektronik ve Fotoğraf › Televizyonlar ve Ev Sineması › Blu-ray Oynatıcılar ve Kayıt Cihazları › Blu-ray Kayıt Cihazları",
    "Elektronik & Foto › Fernseher & Heimkino › DVD-Player & -Rekorder › DVD-Player": "Elektronik ve Fotoğraf › Televizyonlar ve Ev Sineması › DVD Oynatıcılar ve Kayıt Cihazları › DVD Oynatıcılar",
    "Elektronik & Foto › Fernseher & Heimkino › Fernseher": "Elektronik ve Fotoğraf › TV'ler ve Ev Sineması › TV'ler",
    "Elektronik & Foto › Fernseher & Heimkino": "Elektronik ve Fotoğraf › Televizyon ve Ev Sineması",
    # Games
    "Games › Plattformen › PC › Zubehör › Gaming-Headsets": "Oyunlar › Platformlar › PC › Aksesuarlar › Oyun kulaklıkları",
    "Games › Plattformen › PC › Zubehör": "Oyunlar › Platformlar › PC › Aksesuarlar",
    # Küche, Haushalt & Wohnen
    "Küche, Haushalt & Wohnen › Küche, Kochen & Backen › Kochen": "Mutfak, Ev ve Yaşam › Mutfak, Yemek Pişirme ve Pişirme › Yemek Pişirme",
    "Küche, Haushalt & Wohnen › Möbel › Arbeitszimmer": "Mutfak, Ev ve Yaşam › Mobilya › Ev Ofisi",
}


def replace_category_trees(df: pd.DataFrame) -> pd.DataFrame:
    """
    "Categories: Tree" (Almanca) verilen öneklerle BAŞLIYORSA "Categories: Tree (TR)" güncellenir.
    En uzun eşleşen önek kullanılır. Almanca sütun değiştirilmez.
    """
    col_tree = "Categories: Tree"
    col_tree_tr = "Categories: Tree (TR)"

    if col_tree not in df.columns:
        print(f"Uyarı: '{col_tree}' sütunu bulunamadı. Sütunlar: {list(df.columns)}")
        return df
    if col_tree_tr not in df.columns:
        df[col_tree_tr] = pd.NA
        print(f"'{col_tree_tr}' sütunu yoktu, eklendi.")

    # En uzun önek önce denensin diye uzunluğa göre azalan sıra
    sorted_mappings = sorted(
        [(k.strip(), v) for k, v in CATEGORY_DE_TO_TR.items()],
        key=lambda x: len(x[0]),
        reverse=True,
    )

    updated = 0
    vals = df[col_tree].astype(str).str.strip()
    for de_prefix, tr_val in sorted_mappings:
        mask = vals.str.startswith(de_prefix)
        count = mask.sum()
        if count == 0:
            continue
        df.loc[mask, col_tree_tr] = tr_val
        updated += count
        disp = de_prefix[:55] + "..." if len(de_prefix) > 55 else de_prefix
        print(f"  '{disp}' -> {count} satır (TR güncellendi).")
        # Eşleşen satırları bir daha değiştirme (en uzun eşleşme kazandı)
        vals = vals.where(~mask, pd.NA)

    if updated == 0:
        print("Eşleşen satır bulunamadı.")
    else:
        print(f"Toplam {updated} satır güncellendi.")
    return df


def read_table(path: Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        encodings = ["utf-8", "utf-8-sig", "latin-1"]
        for enc in encodings:
            try:
                return pd.read_csv(path, encoding=enc, quoting=1)
            except Exception:
                continue
        return pd.read_csv(path, encoding="utf-8", errors="replace", quoting=1)
    if suffix in (".xlsx", ".xls"):
        if openpyxl is None and suffix == ".xlsx":
            raise ImportError("Excel okumak için openpyxl gerekli: pip install openpyxl")
        return pd.read_excel(path, engine="openpyxl" if suffix == ".xlsx" else None)
    raise ValueError(f"Desteklenen format: .csv, .xlsx. Verilen: {path}")


def write_table(df: pd.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False, encoding="utf-8")
    elif suffix in (".xlsx", ".xls"):
        if openpyxl is None:
            raise ImportError("Excel yazmak için openpyxl gerekli: pip install openpyxl")
        df.to_excel(path, index=False, engine="openpyxl")
    else:
        raise ValueError(f"Desteklenen format: .csv, .xlsx. Verilen: {path}")


def main():
    if len(sys.argv) >= 2:
        input_path = Path(sys.argv[1])
        output_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else input_path
    else:
        input_path = DEFAULT_INPUT
        output_path = DEFAULT_OUTPUT

    if not input_path.exists():
        print(f"Hata: Dosya bulunamadı: {input_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Okunuyor: {input_path}")
    df = read_table(input_path)
    print(f"Satır sayısı: {len(df)}")

    df = replace_category_trees(df)

    print(f"Yazılıyor: {output_path}")
    write_table(df, output_path)
    print("Tamamlandı.")


if __name__ == "__main__":
    main()
