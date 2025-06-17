import os
import time
import shutil
import atexit
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import openai

from utils.konvertierer import konvertiere_erste_seite_zu_base64, extrahiere_text_aus_pdf
from klassifikation.dokument_klassifizieren import gpt_klassifikation
from ocr.ocr_fallback import gpt_abfrage_ocr_text
from inhaltsextraktion.gpt_datenabfrage import gpt_abfrage_inhalt
from parsing.csv_parser import parse_csv_in_dataframe
from validierung.plausibilitaet import plausibilitaet_pruefen
from daten.dateiverwaltung import lade_verarbeitete_liste, speichere_verarbeitete_datei
from daten.verarbeitungspfade import input_folder, nicht_rechnung_folder, archiv_folder, problemordner,bereits_verarbeitet_ordner,output_excel
from vorfilter import pdf_hat_nutzbaren_text

# Logging in Konsole + Datei gleichzeitig
class DualLogger:
    def __init__(self, logfile_path):
        self.terminal = sys.__stdout__      # das ist die Konsole
        self.log = open(logfile_path, "w", encoding="utf-8")  # das ist die Datei

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()
        
# Konfiguration
FLUSH_INTERVAL = 100
BATCH_SIZE = 1000

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
print("💾 Backup-Funktion registriert")

# Logging einschalten
logdatei = output_excel.parent / f"verarbeitung_log_{datetime.now():%Y%m%d_%H%M}.txt"
sys.stdout = DualLogger(logdatei)
print(f"💾 Logging aktiv: {logdatei.name}")


def erkenne_zugehoerigkeit(text):
    lower = text.lower()
    for firma in TOCHTERFIRMEN:
        if firma in lower:
            return firma
    return "unbekannt"

def hauptprozess():
    global anzahl_text, anzahl_ocr, probleme, nicht_rechnungen, dauer_text, dauer_ocr, alle_dfs
    verarbeitete = lade_verarbeitete_liste()
    print("🔍 Starte Verarbeitung mit Zwischenspeicherung und Batch-Limit...")
    pdf_files = list(input_folder.glob("*.pdf"))
    print(f"📂 {len(pdf_files)} Dateien gefunden.")
    for index, pdf_path in enumerate(pdf_files[:BATCH_SIZE], 1):
        start = time.time()
        dateiname = pdf_path.name
        print(f"➞️ {index}/{len(pdf_files)}: {dateiname}")
        print("🛂 Starte Vorprüfung der Datei")
        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            shutil.move(pdf_path, bereits_verarbeitet_ordner / dateiname)
            speichere_verarbeitete_datei(dateiname)
            continue
        ist_lesbar = pdf_hat_nutzbaren_text(pdf_path)
        print(f"🔍 Textlayer vorhanden: {'JA' if ist_lesbar else 'NEIN'}")
        if not ist_lesbar:
            b64 = konvertiere_erste_seite_zu_base64(pdf_path)
            if not b64:
                print("⚠️ Kein OCR möglich → verschoben.")
                shutil.move(pdf_path, problemordner / f"unlesbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                probleme += 1
                continue
            klassifikation = gpt_klassifikation(image_b64=b64)
            print("🔠 Starte GPT-Klassifikation auf Bildbasis (OCR)")
            text = gpt_abfrage_ocr_text(b64)
            dauer = time.time() - start
            print(f"✅ Fertig in {dauer:.1f}s")
            verfahren = "gpt-ocr"
            anzahl_ocr += 1
            dauer_ocr += dauer
        else:
            klassifikation = gpt_klassifikation(text_path=pdf_path)
            print("🔠 Starte GPT-Klassifikation auf Textbasis")
            text = extrahiere_text_aus_pdf(pdf_path)
            dauer = time.time() - start
            print(f"✅ Fertig in {dauer:.1f}s")
            verfahren = "text"
            anzahl_text += 1
            dauer_text += dauer
        if klassifikation != "rechnung":
            print(f"📄 Dokumenttyp: {klassifikation}")
            shutil.move(pdf_path, nicht_rechnung_folder / f"{klassifikation}_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            nicht_rechnungen += 1
            print(f"❌ Nicht-Rechnung → verschoben nach: {klassifikation}_{dateiname}")
            continue
        print("📤 Sende Text zur GPT-Inhaltsextraktion …")
        antwort = gpt_abfrage_inhalt(text=text)
        if antwort.strip().lower().startswith("fehler"):
            print("⚠️ GPT-Inhaltsextraktion fehlgeschlagen → Problemrechnungen")
            shutil.move(pdf_path, problemordner / f"GPT_Fehler_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        print("✅ Starte Plausibilitätsprüfung der Tabelle …")
        df = parse_csv_in_dataframe(antwort, dateiname)
        if df is None or df.empty:
            print("⚠️ Tabelle leer oder fehlerhaft → Problemrechnungen")
            shutil.move(pdf_path, problemordner / f"Tabelle_unbrauchbar_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        df["Dokumententyp"] = klassifikation
        df["Klassifikation_vor_Plausibilitaet"] = klassifikation
        df["Verfahren"] = verfahren
        df["Verarbeitung_Dauer"] = round(dauer, 2)
        df["Zugehörigkeit"] = erkenne_zugehoerigkeit(text)
        alle_dfs.append(df)
        shutil.move(pdf_path, archiv_folder / dateiname)
        speichere_verarbeitete_datei(dateiname)
        if index % FLUSH_INTERVAL == 0:
            flush = pd.concat(alle_dfs, ignore_index=True)
            flush.to_excel(output_excel.parent / f"artikelpositionen_ki_batch_{index}.xlsx", index=False)
            print(f"📏 Zwischenspeicherung nach {len(flush)} Dateien: artikelpositionen_ki_batch_{index}.xlsx")
            alle_dfs.clear()
    if alle_dfs:
        timestamped = output_excel.parent / f"artikelpositionen_ki_batch_final.xlsx"
        pd.concat(alle_dfs, ignore_index=True).to_excel(timestamped, index=False)

    merge_and_enrich(output_excel.parent)

    gesamt_dauer = time.time() - gesamt_start
    gesamt = anzahl_text + anzahl_ocr
    print(f"🌟 Verarbeitung beendet: {gesamt} Dateien in {gesamt_dauer:.1f}s")
    if gesamt:
        print(f"📜 Ø/Datei: {gesamt_dauer/gesamt:.2f}s")
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

    # 🧼 Einheitenharmonisierung + Logging   
    merged = harmonisiere_daten_mit_mapping(merged, mapping_path="mein_mapping.xlsx")

    # 🔢 Zahlenbereinigung mit Rohwerten
    merged = bereinige_zahlen(merged)

    # 🧠 Kategorisierung über alle Artikelbezeichnungen global
    merged, logeintraege = kategorisiere_artikel_global(merged)

    # 📁 Speichern der Gesamtausgabe
    gesamt_path = ordner / f"artikelpositionen_ki_GESAMT_{datetime.now():%Y%m%d_%H%M}.xlsx"
    merged.to_excel(gesamt_path, index=False)
    print(f"📊 Gesamtausgabe gespeichert: {gesamt_path.name}")

    # 📝 Speichern des Kategorielogs (nur wenn etwas kategorisiert wurde)
    if logeintraege:
        df_log = pd.DataFrame(logeintraege)
        df_log.to_excel(ordner / f"kategorielog_neu_{datetime.now():%Y%m%d_%H%M}.xlsx", index=False)
        print("✅ Kategorien wurden global via GPT erzeugt und geloggt.")

def kategorisiere_artikel_global(df):
    print("🧠 GPT: Kategorisiere Artikel global mit Log-Wiederverwendung …")

    artikel = df["Artikelbezeichnung"].dropna().unique()
    artikel_clean = pd.Series(artikel).astype(str).str.strip().str.lower()

    # Lade alte Logs
    vorhandene_logs = sorted(Path(output_excel.parent).glob("kategorielog_*.xlsx"))
    treffer_alt = pd.DataFrame()

    for log_path in reversed(vorhandene_logs):
        try:
            log_df = pd.read_excel(log_path)
            log_df["clean"] = log_df["Artikelbezeichnung"].astype(str).str.strip().str.lower()
            log_df = log_df[["Artikelbezeichnung", "Kategorie", "Unterkategorie", "clean"]].drop_duplicates()
            treffer_alt = pd.concat([treffer_alt, log_df], ignore_index=True)
        except Exception as e:
            print(f"⚠️ Fehler beim Lesen eines Logs: {e}")
            continue

    # Entferne schlechte Kategorien
    treffer_alt = treffer_alt[
        ~treffer_alt["Kategorie"].str.lower().isin(["sonstiges", "fehler", "unbekannt"])
        & ~treffer_alt["Unterkategorie"].str.lower().isin(["unklar", "unbekannt"])
    ]

    # Trenne bekannte und neue Begriffe
    artikel_set = pd.DataFrame({"Artikelbezeichnung": artikel, "clean": artikel_clean})
    reuse_df = artikel_set.merge(treffer_alt, on="clean", how="inner").drop_duplicates("Artikelbezeichnung")
    gpt_df = artikel_set[~artikel_set["Artikelbezeichnung"].isin(reuse_df["Artikelbezeichnung"])]

    print(f"🔁 Wiederverwendete Kategorien: {len(reuse_df)}")
    print(f"🧠 Neue GPT-Kategorisierung für: {len(gpt_df)}")

    logeintraege = []

    # GPT-Kategorisierung für neue Begriffe
    if not gpt_df.empty:
        prompt = (
            "Ordne den folgenden Artikeln je eine passende Haupt- und Unterkategorie zu.\n"
            "Gib nur die CSV-Zeilen mit folgenden Spalten zurück:\n"
            "Artikelbezeichnung;Hauptkategorie;Unterkategorie\n\n" +
            "\n".join(gpt_df["Artikelbezeichnung"])
        )
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            antwort = response['choices'][0]['message']['content'].strip()
            lines = [line for line in antwort.splitlines() if ";" in line]
            cat_gpt = pd.read_csv(StringIO("\n".join(lines)), sep=";", engine="python", on_bad_lines="skip")
            cat_gpt["Herkunft"] = "gpt"
        except Exception as e:
            print(f"⚠️ Fehler bei GPT-Kategorisierung: {e}")
            cat_gpt = pd.DataFrame(columns=["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"])
    else:
        cat_gpt = pd.DataFrame(columns=["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"])

    # Ergänze Herkunft zu reuse-Daten
    if not reuse_df.empty:
        reuse_df["Herkunft"] = "reuse"
        reuse_df.rename(columns={"Kategorie": "Hauptkategorie"}, inplace=True)

    # Kombiniere alles
    gesamt_kat = pd.concat([reuse_df[["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"]], cat_gpt], ignore_index=True)
    df = df.merge(gesamt_kat, on="Artikelbezeichnung", how="left")
    df.rename(columns={"Hauptkategorie": "Kategorie"}, inplace=True)

    # Baue Log
    for _, row in gesamt_kat.iterrows():
        logeintraege.append({
            "Artikelbezeichnung": row["Artikelbezeichnung"],
            "Kategorie": row["Kategorie"],
            "Unterkategorie": row["Unterkategorie"],
            "Herkunft": row["Herkunft"],
            "Zeitpunkt": datetime.now()
        })

    return df, logeintraege

def harmonisiere_daten_mit_mapping(df, mapping_path=None):
    print("🔧 Harmonisiere Einheiten & Artikelbezeichnungen mit Mapping + Logging ...")

    default_map = {
        "t": "Tonne", "t.": "Tonne", "T": "Tonne",
        "kg": "Kilogramm",
        "St": "Stück", "St.": "Stück", "st": "Stück",
        "m": "Meter", "m.": "Meter",
        "l": "Liter", "L": "Liter",
        "psch": "Pauschale", "pauschal": "Pauschale"
    }

    mapping_dict = default_map.copy()
    if mapping_path and Path(mapping_path).exists():
        try:
            df_map = pd.read_excel(mapping_path)
            for _, row in df_map.iterrows():
                roh = str(row["Einheit_roh"]).strip()
                norm = str(row["Einheit_normiert"]).strip()
                if roh and norm:
                    mapping_dict[roh] = norm
            print(f"📄 Mapping-Datei geladen: {mapping_path}")
        except Exception as e:
            print(f"⚠️ Fehler beim Laden des Mappings: {e}")

    unbekannte = set()

    def reinige_einheit(e):
        e_clean = str(e).strip().replace(".", "")
        normiert = mapping_dict.get(e_clean)
        if not normiert:
            unbekannte.add(e_clean)
            return e_clean
        return normiert

    def reinige_bezeichnung(text):
        if not isinstance(text, str):
            return text
        return text.strip().replace("  ", " ")

    if "Einheit" in df.columns:
        df["Einheit"] = df["Einheit"].apply(reinige_einheit)
    if "Artikelbezeichnung" in df.columns:
        df["Artikelbezeichnung"] = df["Artikelbezeichnung"].apply(reinige_bezeichnung)

    if unbekannte:
        log_df = pd.DataFrame(sorted(unbekannte), columns=["Einheit_roh"])
        log_df["Einheit_normiert"] = ""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_path = output_excel.parent / f"einheiten_log_{timestamp}.xlsx"
        log_df.to_excel(log_path, index=False)
        print(f"📝 {len(unbekannte)} unbekannte Einheiten gespeichert in: {log_path}")

    return df
    
def korrigiere_zahl_mit_gpt(wert):
    prompt = f"""
    Korrigiere folgende fehlerhafte Zahl so, dass sie maschinenlesbar als float verwendet werden kann:
    Gib nur die Zahl im Format 1234.56 zurück (kein Eurozeichen, kein Text).
    Beispiel: '4.473.39' → '4473.39'
    Wert: {wert}
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        result = response.choices[0].message.content.strip()
        return float(result)
    except:
        return None

def bereinige_zahlen(df):
    print("🧠 Formatiere und korrigiere Zahlen …")
    for spalte in ["Menge", "Einzelpreis", "Gesamtpreis"]:
        if spalte in df.columns:
            df[f"{spalte}_roh"] = df[spalte]
            df[spalte] = df[spalte].astype(str) \
                                   .str.replace("€", "", regex=False) \
                                   .str.replace(",", ".", regex=False) \
                                   .str.replace(" ", "", regex=False) \
                                   .str.strip()

            def umwandeln_oder_gpt(wert):
                try:
                    if isinstance(wert, str) and wert.count('.') > 1:
                        raise ValueError()
                    return float(wert)
                except:
                    return korrigiere_zahl_mit_gpt(wert)

            df[spalte] = df[spalte].apply(umwandeln_oder_gpt)
    return df
 
if __name__ == "__main__":
    hauptprozess()