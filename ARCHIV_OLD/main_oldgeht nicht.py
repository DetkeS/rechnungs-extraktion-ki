from daten.verarbeitungspfade import input_folder, archiv_folder, nicht_rechnung_folder, problemordner, output_excel
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei
from utils.konvertierer import extrahiere_text_aus_pdf, konvertiere_erste_seite_zu_base64
from ocr.ocr_fallback import gpt_abfrage_ocr_text
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from inhaltsextraktion.gpt_datenabfrage import gpt_abfrage_inhalt
from parsing.csv_parser import parse_csv_in_dataframe
from validierung.plausibilitaet import plausibilitaet_pruefen
import shutil
import time
import pandas as pd
from datetime import datetime

def hauptprozess():
    print("🔄 Starte KI-Extraktion...")
    verarbeitete = lade_verarbeitete_liste()
    alle_dfs = []
    pdf_files = list(input_folder.glob("*.pdf"))
    total_files = len(pdf_files)
    print(f"📂 {total_files} PDF-Dateien gefunden.\n")
    start_gesamt = time.time()

    for index, dateipfad in enumerate(pdf_files, start=1):
        start_einzel = time.time()
        dateiname = dateipfad.name
        print(f"➡️ Datei {index}/{total_files}: {dateiname}")
        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.\n")
            continue

        text = extrahiere_text_aus_pdf(dateipfad)
        if not text or len(text) < 100:
            print("⚠️ Text zu kurz – wechsle zu OCR-Analyse")
            b64 = konvertiere_erste_seite_zu_base64(dateipfad)
            if not b64:
                shutil.move(dateipfad, problemordner / f"Kein_OCR_möglich_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                continue
            text = gpt_abfrage_ocr_text(b64)
            if not text.strip():
                shutil.move(dateipfad, problemordner / f"OCR_liefert_keinen_Text_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                continue

        klassifikation = gpt_klassifikation(text)
        print(f"🔎 Dokumententyp: {klassifikation}")

        if klassifikation != "rechnung":
            ziel = nicht_rechnung_folder / f"{klassifikation}_{dateiname}"
            shutil.move(dateipfad, ziel)
            speichere_verarbeitete_datei(dateiname)
            print(f"📥 Kein Rechnungstyp. Verschoben: {ziel.name}\n")
            continue

        antwort = gpt_abfrage_inhalt(text=text)
        if antwort.strip().lower().startswith("fehler"):
            shutil.move(dateipfad, problemordner / f"GPT_Fehler_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            continue

        df = parse_csv_in_dataframe(antwort, dateiname)
        if df is None or df.empty:
            shutil.move(dateipfad, problemordner / f"Tabelle_unbrauchbar_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            print("⚠️ Tabelle leer oder fehlerhaft. → Problemrechnungen\n")
            continue

        df["Dokumententyp"] = klassifikation
        df = plausibilitaet_pruefen(df)
        alle_dfs.append(df)

        shutil.move(dateipfad, archiv_folder / dateiname)
        speichere_verarbeitete_datei(dateiname)

        dauer = time.time() - start_einzel
        verbleibend = dauer * (total_files - index)
        print(f"✅ Fertig in {dauer:.1f}s | Verbleibend ~{verbleibend/60:.1f}min\n")

    if alle_dfs:
        final_df = pd.concat(alle_dfs, ignore_index=True)
        timestamped_output = output_excel.parent / f"{datetime.now().strftime('%Y%m%d_%H%M')}_artikelpositionen_ki.xlsx"
        final_df.to_excel(timestamped_output, index=False)
        print(f"✅ Excel gespeichert: {timestamped_output}")
    else:
        print("⚠️ Keine gültigen Rechnungen verarbeitet.")

if __name__ == "__main__":
    hauptprozess()