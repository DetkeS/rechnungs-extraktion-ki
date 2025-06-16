from pathlib import Path
from vorfilter import pdf_hat_text
from utils.konvertierer import konvertiere_erste_seite_zu_base64
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from daten.verarbeitungspfade import input_folder, nicht_rechnung_folder, archiv_folder, problemordner
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei
import shutil
import time

def hauptprozess_v2():
    print("🔍 Starte Vorfilterung + Klassifikation...")
    verarbeitete = lade_verarbeitete_liste()
    pdf_files = list(input_folder.glob("*.pdf"))
    print(f"📂 {len(pdf_files)} Dateien gefunden.")

    for index, pdf_path in enumerate(pdf_files, 1):
        dateiname = pdf_path.name
        print(f"➡️ {index}/{len(pdf_files)}: {dateiname}")

        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            continue

        ist_lesbar = pdf_hat_text(pdf_path)
        print(f"🔍 Lesbarkeit (Textlayer): {'JA' if ist_lesbar else 'NEIN'}")

        if not ist_lesbar:
            b64 = konvertiere_erste_seite_zu_base64(pdf_path)
            if not b64:
                shutil.move(pdf_path, problemordner / f"unlesbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                print("⚠️ Kein OCR möglich → verschoben.")
                continue
            klassifikation = gpt_klassifikation(image_b64=b64)
        else:
            klassifikation = gpt_klassifikation(text_path=pdf_path)

        print(f"📄 Dokumenttyp: {klassifikation}")

        if klassifikation != "rechnung":
            ziel = nicht_rechnung_folder / f"{klassifikation}_{dateiname}"
            shutil.move(pdf_path, ziel)
            speichere_verarbeitete_datei(dateiname)
            print(f"❌ Nicht-Rechnung → verschoben nach: {ziel.name}")
            continue

        # Rechnung erkannt → belassen zur Weiterverarbeitung
        archiv_folder.mkdir(parents=True, exist_ok=True)
        shutil.move(pdf_path, archiv_folder / dateiname)
        speichere_verarbeitete_datei(dateiname)
        print("✅ Rechnung → archiviert.")

if __name__ == "__main__":
    hauptprozess_v2()