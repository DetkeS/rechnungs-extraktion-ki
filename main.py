from pathlib import Path
from vorfilter import pdf_hat_nutzbaren_text as pdf_hat_text
from utils.konvertierer import konvertiere_erste_seite_zu_base64, extrahiere_text_aus_pdf
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from ocr.ocr_fallback import gpt_abfrage_ocr_text
from inhaltsextraktion.gpt_datenabfrage import gpt_abfrage_inhalt
from parsing.csv_parser import parse_csv_in_dataframe
from validierung.plausibilitaet import plausibilitaet_pruefen
from daten.verarbeitungspfade import input_folder, nicht_rechnung_folder, archiv_folder, problemordner, output_excel
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei
import shutil
import pandas as pd
import time
from datetime import datetime

def hauptprozess():
    print("🔍 Starte Verarbeitung mit Vorfilter + Klassifikation + Extraktion...")
    verarbeitete = lade_verarbeitete_liste()
    pdf_files = list(input_folder.glob("*.pdf"))
    alle_dfs = []
    print(f"📂 {len(pdf_files)} Dateien gefunden.")

    for index, pdf_path in enumerate(pdf_files, 1):
        start = time.time()
        dateiname = pdf_path.name
        print(f"➡️ {index}/{len(pdf_files)}: {dateiname}")

        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            continue

        ist_lesbar = pdf_hat_text(pdf_path)
        print(f"🔍 Textlayer vorhanden: {'JA' if ist_lesbar else 'NEIN'}")

        if not ist_lesbar:
            b64 = konvertiere_erste_seite_zu_base64(pdf_path)
            if not b64:
                shutil.move(pdf_path, problemordner / f"unlesbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                print("⚠️ Kein OCR möglich → verschoben.")
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

        shutil.move(pdf_path, archiv_folder / dateiname)
        speichere_verarbeitete_datei(dateiname)
        dauer = time.time() - start
        print(f"✅ Fertig in {dauer:.1f}s")

    if alle_dfs:
        final_df = pd.concat(alle_dfs, ignore_index=True)
        timestamped_output = output_excel.parent / f"{datetime.now().strftime('%Y%m%d_%H%M')}_artikelpositionen_ki.xlsx"
        final_df.to_excel(timestamped_output, index=False)
        print(f"✅ Excel gespeichert: {timestamped_output}")
    else:
        print("⚠️ Keine gültigen Rechnungen verarbeitet.")

if __name__ == "__main__":
    hauptprozess()