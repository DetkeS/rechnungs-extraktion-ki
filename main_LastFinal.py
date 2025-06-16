from pathlib import Path
import pandas as pd
from datetime import datetime
import atexit
import shutil
import time
import glob

from vorfilter import pdf_hat_nutzbaren_text
from utils.konvertierer import konvertiere_erste_seite_zu_base64, extrahiere_text_aus_pdf
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from ocr.ocr_fallback import gpt_abfrage_ocr_text
from inhaltsextraktion.gpt_datenabfrage import gpt_abfrage_inhalt
from parsing.csv_parser import parse_csv_in_dataframe
from validierung.plausibilitaet import plausibilitaet_pruefen
from daten.verarbeitungspfade import input_folder, nicht_rechnung_folder, archiv_folder, problemordner, output_excel
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei

# Konfiguration
FLUSH_INTERVAL = 50
BATCH_LIMIT = 1000
BATCH_PREFIX = f"artikelpositionen_ki_batch_{datetime.now():%Y%m%d_%H%M}"
flush_counter = 0
alle_dfs = []


def schreibe_zwischenstand():
    global flush_counter, alle_dfs
    if alle_dfs:
        df_tmp = pd.concat(alle_dfs, ignore_index=True)
        flush_file = output_excel.parent / f"{BATCH_PREFIX}_{str(flush_counter).zfill(3)}.xlsx"
        df_tmp.to_excel(flush_file, index=False)
        print(f"📏 Zwischenspeicherung nach {len(df_tmp)} Dateien: {flush_file.name}")
        alle_dfs.clear()
        flush_counter += 1


def zusammenfassen_zu_gesamtdatei():
    folder = output_excel.parent
    gesamt_dfs = []
    batch_files = sorted(folder.glob(f"{BATCH_PREFIX}_*.xlsx"))
    for file in batch_files:
        try:
            df = pd.read_excel(file)
            gesamt_dfs.append(df)
        except Exception as e:
            print(f"⚠️ Fehler beim Lesen von {file.name}: {e}")
    if gesamt_dfs:
        gesamt_df = pd.concat(gesamt_dfs, ignore_index=True)
        gesamt_path = folder / f"{BATCH_PREFIX}_GESAMT.xlsx"
        gesamt_df.to_excel(gesamt_path, index=False)
        print(f"📊 Gesamtausgabe gespeichert: {gesamt_path.name}")
    else:
        print("⚠️ Keine gültigen Batch-Dateien gefunden zur Zusammenführung.")


atexit.register(schreibe_zwischenstand)


def hauptprozess():
    print("🔍 Starte Verarbeitung mit Zwischenspeicherung und Batch-Limit...")
    verarbeitete = lade_verarbeitete_liste()
    pdf_files = list(input_folder.glob("*.pdf"))
    print(f"📂 {len(pdf_files)} Dateien gefunden.")
    verarbeitet_aktuell = 0

    for index, pdf_path in enumerate(pdf_files, 1):
        if verarbeitet_aktuell >= BATCH_LIMIT:
            print(f"⏹️ Batch-Limit von {BATCH_LIMIT} erreicht. Skript beendet sich automatisch.")
            break

        start = time.time()
        dateiname = pdf_path.name
        print(f"➔️ {index}/{len(pdf_files)}: {dateiname}")

        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            continue

        try:
            ist_lesbar = pdf_hat_nutzbaren_text(pdf_path)
            print(f"🔍 Textlayer vorhanden: {'JA' if ist_lesbar else 'NEIN'}")

            if not ist_lesbar:
                b64 = konvertiere_erste_seite_zu_base64(pdf_path)
                if not b64:
                    shutil.move(pdf_path, problemordner / f"unlesbar_{dateiname}")
                    speichere_verarbeitete_datei(dateiname)
                    print("⚠️ Kein OCR möglich → verschoben.\n")
                    continue
                klassifikation = gpt_klassifikation(image_b64=b64)
                text = gpt_abfrage_ocr_text(b64)
            else:
                klassifikation = gpt_klassifikation(text_path=pdf_path)
                text = extrahiere_text_aus_pdf(pdf_path)

            print(f"📄 Dokumenttyp: {klassifikation}")

            if klassifikation != "rechnung":
                ziel = nicht_rechnung_folder / f"{klassifikation}_{dateiname}"
                shutil.move(pdf_path, ziel)
                speichere_verarbeitete_datei(dateiname)
                print(f"❌ Nicht-Rechnung → verschoben nach: {ziel.name}\n")
                continue

            antwort = gpt_abfrage_inhalt(text=text)
            if antwort.strip().lower().startswith("fehler"):
                shutil.move(pdf_path, problemordner / f"GPT_Fehler_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                print("⚠️ GPT-Inhaltsextraktion fehlgeschlagen → Problemrechnungen\n")
                continue

            df = parse_csv_in_dataframe(antwort, dateiname)
            if df is None or df.empty:
                shutil.move(pdf_path, problemordner / f"Tabelle_unbrauchbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                print("⚠️ Tabelle leer oder fehlerhaft → Problemrechnungen\n")
                continue

            df["Dokumententyp"] = klassifikation
            df = plausibilitaet_pruefen(df)
            alle_dfs.append(df)
            verarbeitet_aktuell += 1

            if verarbeitet_aktuell % FLUSH_INTERVAL == 0:
                schreibe_zwischenstand()

            shutil.move(pdf_path, archiv_folder / dateiname)
            speichere_verarbeitete_datei(dateiname)
            dauer = time.time() - start
            print(f"✅ Fertig in {dauer:.1f}s\n")

        except Exception as e:
            print(f"💥 Fehler bei {dateiname}: {e}")
            continue

    if alle_dfs:
        schreibe_zwischenstand()
    zusammenfassen_zu_gesamtdatei()
    print("🌟 Verarbeitung abgeschlossen.")


if __name__ == "__main__":
    hauptprozess()
