import os
import time
import shutil
import atexit
import pandas as pd
from datetime import datetime
from pathlib import Path
from utils.konvertierer import konvertiere_erste_seite_zu_base64, extrahiere_text_aus_pdf
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from ocr.ocr_fallback import gpt_abfrage_ocr_text
from inhaltsextraktion.gpt_datenabfrage import gpt_abfrage_inhalt
from parsing.csv_parser import parse_csv_in_dataframe
from validierung.plausibilitaet import plausibilitaet_pruefen
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei
from daten.verarbeitungspfade import input_folder, nicht_rechnung_folder, archiv_folder, problemordner, output_excel
from vorfilter import pdf_hat_nutzbaren_text

# Konfiguration
FLUSH_INTERVAL = 250
BATCH_SIZE = 1000
MAX_KATEGORIEN = set()
TOCHTERFIRMEN = ["wähler", "kuhlmann", "mudcon", "datacon", "seier", "geidel", "bhk"]

# Statistik
gesamt_start = time.time()
anzahl_text = 0
anzahl_ocr = 0
probleme = 0
nicht_rechnungen = 0
dauer_text = 0.0
dauer_ocr = 0.0
alle_dfs = []

# atexit-Backup
def speichere_backup():
    if alle_dfs:
        backup = pd.concat(alle_dfs, ignore_index=True)
        backup.to_excel(output_excel.parent / "backup_abbruch.xlsx", index=False)
atexit.register(speichere_backup)

def erkenne_zugehoerigkeit(text):
    lower = text.lower()
    for firma in TOCHTERFIRMEN:
        if firma in lower:
            return firma
    return "unbekannt"

def hauptprozess():
    verarbeitete = lade_verarbeitete_liste()
    pdf_files = list(input_folder.glob("*.pdf"))
    print(f"📂 {len(pdf_files)} Dateien gefunden.")
    for index, pdf_path in enumerate(pdf_files[:BATCH_SIZE], 1):
        start = time.time()
        dateiname = pdf_path.name
        print(f"➡️ {index}/{len(pdf_files)}: {dateiname}")
        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            continue
        ist_lesbar = pdf_hat_nutzbaren_text(pdf_path)
        if not ist_lesbar:
            b64 = konvertiere_erste_seite_zu_base64(pdf_path)
            if not b64:
                shutil.move(pdf_path, problemordner / f"unlesbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                probleme += 1
                continue
            klassifikation = gpt_klassifikation(image_b64=b64)
            text = gpt_abfrage_ocr_text(b64)
            dauer = time.time() - start
            verfahren = "gpt-ocr"
            anzahl_ocr += 1
            dauer_ocr += dauer
        else:
            klassifikation = gpt_klassifikation(text_path=pdf_path)
            text = extrahiere_text_aus_pdf(pdf_path)
            dauer = time.time() - start
            verfahren = "text"
            anzahl_text += 1
            dauer_text += dauer
        if klassifikation != "rechnung":
            shutil.move(pdf_path, nicht_rechnung_folder / f"{klassifikation}_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            nicht_rechnungen += 1
            continue
        antwort = gpt_abfrage_inhalt(text=text)
        if antwort.strip().lower().startswith("fehler"):
            shutil.move(pdf_path, problemordner / f"GPT_Fehler_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        df = parse_csv_in_dataframe(antwort, dateiname)
        if df is None or df.empty:
            shutil.move(pdf_path, problemordner / f"Tabelle_unbrauchbar_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        df["Dokumententyp"] = klassifikation
        df["Klassifikation_vor_Plausibilitaet"] = klassifikation
        df["Verfahren"] = verfahren
        df["Verarbeitung_Dauer"] = round(dauer, 2)
        df["Zugehörigkeit"] = erkenne_zugehoerigkeit(text)
        df = plausibilitaet_pruefen(df)
        alle_dfs.append(df)
        shutil.move(pdf_path, archiv_folder / dateiname)
        speichere_verarbeitete_datei(dateiname)
        if index % FLUSH_INTERVAL == 0:
            flush = pd.concat(alle_dfs, ignore_index=True)
            flush.to_excel(output_excel.parent / f"artikelpositionen_ki_batch_{index}.xlsx", index=False)
            alle_dfs.clear()
    if alle_dfs:
        timestamped = output_excel.parent / f"artikelpositionen_ki_batch_final.xlsx"
        pd.concat(alle_dfs, ignore_index=True).to_excel(timestamped, index=False)

    # 🔁 Merge aller Batchdateien & Kategoriebildung
    merge_and_enrich(output_excel.parent)

    # Statistik
    gesamt_dauer = time.time() - gesamt_start
    gesamt = anzahl_text + anzahl_ocr
    print(f"🏁 Verarbeitung beendet: {gesamt} Dateien in {gesamt_dauer:.1f}s")
    if gesamt:
        print(f"📝 Ø/Datei: {gesamt_dauer/gesamt:.2f}s")
        print(f"📄 Textbasiert: {anzahl_text} ({anzahl_text/gesamt:.1%}), Ø {dauer_text/max(1,anzahl_text):.2f}s")
        print(f"🧠 GPT-OCR: {anzahl_ocr} ({anzahl_ocr/gesamt:.1%}), Ø {dauer_ocr/max(1,anzahl_ocr):.2f}s")
        print(f"❌ Nicht-Rechnungen: {nicht_rechnungen} ({nicht_rechnungen/gesamt:.1%})")
        print(f"⚠️ Probleme: {probleme} ({probleme/gesamt:.1%})")

def merge_and_enrich(ordner):
    batches = list(ordner.glob("artikelpositionen_ki_batch_*.xlsx"))
    if not batches:
        print("⚠️ Keine Batchdateien zum Zusammenführen gefunden.")
        return
    frames = [pd.read_excel(f) for f in batches]
    merged = pd.concat(frames, ignore_index=True)
    if "Kategorie" not in merged.columns:
        merged["Kategorie"] = merged["Artikelbezeichnung"].apply(lambda x: rate_kategorie(x))
    if "Unterkategorie" not in merged.columns:
        merged["Unterkategorie"] = merged["Artikelbezeichnung"].apply(lambda x: rate_unterkategorie(x))
    merged.to_excel(ordner / f"artikelpositionen_ki_GESAMT_{datetime.now():%Y%m%d_%H%M}.xlsx", index=False)

def rate_kategorie(bezeichnung):
    text = str(bezeichnung).lower()
    if "bagger" in text or "radlader" in text:
        return "Maschine"
    if "bitumen" in text or "asphalt" in text:
        return "Material"
    return "unbekannt"

def rate_unterkategorie(bezeichnung):
    text = str(bezeichnung).lower()
    if "miete" in text:
        return "Miete"
    if "kauf" in text:
        return "Kauf"
    return "unbekannt"

if __name__ == "__main__":
    hauptprozess()