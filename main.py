# ==========================================
# 📦 INITIALISIERUNG UND KONFIGURATION
# ==========================================
import os
import time
import shutil
import atexit
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import openai
import fitz  # PyMuPDF
from io import StringIO
import pandas as pd  # Falls nicht ohnehin schon importiert

#neu vom zurückführen der dateien
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
from base64 import b64encode
from io import BytesIO
basisverzeichnis = Path(__file__).resolve().parent
zeitstempel = datetime.now().strftime('%Y%m%d_%H%M')

input_folder = basisverzeichnis / "zu_verarbeiten"
archiv_folder = basisverzeichnis / f"{zeitstempel}_verarbeitet"
nicht_rechnung_folder = basisverzeichnis / f"{zeitstempel}_nicht_rechnung"
problemordner = basisverzeichnis / f"{zeitstempel}_problemrechnungen"
bereits_verarbeitet_ordner = basisverzeichnis / f"{zeitstempel}_bereits_verarbeitet"
output_excel = basisverzeichnis / "artikelpositionen_ki.xlsx"
protokoll_excel = basisverzeichnis / "verarbeitete_dateien.xlsx"
protokoll_excel = Path("verarbeitete_dateien.xlsx")

TOCHTERFIRMEN = ["Wähler", "Kuhlmann", "BHK", "Mudcon","Seier"] #Bekannte Firmen für die Rechnungen verarbeitet werden. 
VERARBEITUNGSFEHLER = []  # zentrale Fehlerliste für alle Fehlermeldungen



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
FLUSH_INTERVAL = 20
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


# Ordner sicherstellen
for pfad in [
    input_folder, archiv_folder, nicht_rechnung_folder,
    problemordner, bereits_verarbeitet_ordner
]:
    pfad.mkdir(parents=True, exist_ok=True)



# ==========================================
# 🛠️ HILFSFUNKTIONEN & WERKZEUGE
# ==========================================

# Logging einschalten
logdatei = output_excel.parent / f"verarbeitung_log_{datetime.now():%Y%m%d_%H%M}.txt"
sys.stdout = DualLogger(logdatei)
print(f"💾 Logging aktiv: {logdatei.name}")

# Abbrüche verhindern -Robuste Verarbeitung
def sicher_ausführen(funktion, name, *args, **kwargs):
    try:
        return funktion(*args, **kwargs)
    except Exception as e:
        fehlermeldung = f"❌ Fehler in '{name}': {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return None
    
def zeige_next_steps_übersicht(batch_ordner):
    print("\n\n📋 NÄCHSTE SCHRITTE (vor dem nächsten Lauf):\n")
    print("1️⃣  🔁 Verschiebe oder lösche die verarbeiteten Batch-Dateien:")
    for f in Path(batch_ordner).glob("artikelpositionen_ki_batch_*.xlsx"):
        print(f"    - {f.name}")
    print("    📎 Sonst werden sie beim nächsten Lauf erneut verarbeitet!\n")

    print("2️⃣  📄 Öffne die Datei `mein_mapping.xlsx` und ergänze neue Einheiten.")
    print("    ➤ Alternativ: prüfe `einheiten_log_<timestamp>.xlsx` auf neue Rohwerte.\n")

    print("3️⃣  🧠 Prüfe den Kategorielog (`kategorielog_neu_*.xlsx`), falls Kategorien nicht korrekt erkannt wurden.\n")

    print("4️⃣  ✅ Wenn alles geprüft und gepflegt ist, kannst du das Skript erneut starten.\n")

    print("💡 Tipp: Erstelle ggf. ein Backup des Gesamt-Exports:")
    print("    - `artikelpositionen_ki_GESAMT_<timestamp>.xlsx`\n")
   
    if VERARBEITUNGSFEHLER:
        print("⚠️ Achtung: Während der Verarbeitung sind Fehler aufgetreten!")
        print("   ➤ Prüfe das Fehlerprotokoll (fehlerprotokoll.txt)")
        print("   ➤ Häufige Ursachen:")
        print("      - ❌ Kategorie konnte nicht mit GPT ermittelt werden")
        print("      - ❌ Batchdatei defekt (Datei verschoben)")
        print("      - ❌ Mapping oder Spalten fehlen")
        print("   ➤ Was tun:")
        print("      1. Öffne die Datei `mein_mapping.xlsx` und ergänze fehlende Einheiten.")
        print("      2. Prüfe die verschobenen Dateien im Ordner `fehlerhafte_batches`.")
        print("      3. Starte das Skript erneut, wenn alles geprüft wurde.\n")

def speichere_verarbeitete_datei(dateiname):
    try:
        df = pd.DataFrame([[dateiname, datetime.now().strftime("%Y-%m-%d %H:%M")]],
                          columns=["Dateiname", "Verarbeitet am"])
        if protokoll_excel.exists():
            bestehend = pd.read_excel(protokoll_excel)
            df = pd.concat([bestehend, df], ignore_index=True)
        df.to_excel(protokoll_excel, index=False)
    except Exception as e:
        fehlermeldung = f"Fehler beim Speichern in Protokolldatei ({dateiname}): {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)




# ==========================================
# 📑 VORVERARBEITUNG & PDF-EXTRAKTION
# ==========================================

def extrahiere_text_aus_pdf(pfad):
    try:
        reader = PdfReader(pfad)
        return "\n".join([page.extract_text() or "" for page in reader.pages]).strip()
    except Exception as e:
        VERARBEITUNGSFEHLER.append(f"PDF-Text konnte nicht extrahiert werden ({pfad}): {e}")
        return ""

def konvertiere_erste_seite_zu_base64(pfad):
    try:
        image = convert_from_path(pfad, first_page=1, last_page=1)[0]
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return b64encode(buffer.getvalue()).decode("utf-8")
    except Exception as e:
        VERARBEITUNGSFEHLER.append(f"Base64-Bild von erster PDF-Seite fehlgeschlagen ({pfad}): {e}")
        return None
        
def pdf_hat_nutzbaren_text(pdf_path, min_verwertbar=80):
    try:
        doc = fitz.open(pdf_path)
        text = "".join([page.get_text() for page in doc])
        return len(text.strip()) >= min_verwertbar and not text.startswith("VL<?LHH")
    except Exception as e:
        fehlermeldung = f"Fehler beim PDF-Vorfilter für {pdf_path}: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return False

# ==========================================
# 🧠 GPT-FUNKTIONEN (OCR, KLASSIFIKATION, INHALT)
# ==========================================

def gpt_klassifikation(text_path=None, image_b64=None):
    if image_b64:
        prompt = (
            "Du erhältst ein Bild eines Dokuments.\n"
            "Bitte klassifiziere den Dokumenttyp als einen der folgenden Begriffe:\n"
            "- rechnung\n- mahnung\n- anschreiben\n- email\n- gutschrift\n"
            "- zahlungserinnerung\n- behördlich\n- sonstiges\n\n"
            "Antworte ausschließlich mit einem dieser Begriffe."
        )
        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}}
            ]
        }]
    elif text_path:
        try:
            doc = fitz.open(text_path)
            extracted_text = "\n".join([page.get_text() for page in doc])
        except Exception as e:
            fehlermeldung = f"❌ Fehler beim Öffnen/Lesen von PDF ({text_path}): {e}"
            print(fehlermeldung)
            VERARBEITUNGSFEHLER.append(fehlermeldung)
            return "unlesbar"

        prompt = (
            "Analysiere den folgenden Text und gib exakt einen Dokumenttyp zurück:\n"
            "- rechnung\n- mahnung\n- anschreiben\n- email\n- gutschrift\n"
            "- zahlungserinnerung\n- behördlich\n- sonstiges\n\n"
            "Wichtig: Wenn unklar, schätze.\n\n"
            f"{extracted_text[:3000]}"
        )
        messages = [{"role": "user", "content": prompt}]
    else:
        fehlermeldung = "gpt_klassifikation: Kein text_path oder image_b64 übergeben."
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return "fehler"

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        fehlermeldung = f"Fehler bei Klassifikation durch GPT: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return "unbekannt"

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
    except Exception as e:
        VERARBEITUNGSFEHLER.append(f"GPT-Zahlenkorrektur fehlgeschlagen für '{wert}': {e}")
        return None

def gpt_abfrage_ocr_text(b64_image):
    prompt = (
        "Analysiere das folgende Bild einer Rechnung.\n"
        "Auch wenn die Darstellung undeutlich ist, versuche so viel strukturierten Text wie möglich zu extrahieren.\n"
        "Ignoriere Layout-Fehler, Trennzeichen oder Formatierung – gib nur den vermutlichen reinen Rechnungstext wieder."
    )

    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_image}"}}
        ]
    }]

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        fehlermeldung = f"Fehler bei GPT-OCR: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return ""

def gpt_abfrage_inhalt(text=None, b64_image=None):
    prompt = (
        "Extrahiere alle Artikelpositionen aus dieser Rechnung in folgender CSV-Struktur:\n"
        "Artikelbezeichnung;Menge;Einheit;Einzelpreis;Gesamtpreis;Lieferant;Rechnungsdatum;Rechnungsempfänger;Rechnungsnummer\n"
        "Gib ausschließlich die Tabelle als CSV mit Semikolon-Trennung zurück.\n"
        "WICHTIG: Gib KEINE fiktiven Daten an. Wenn keine Daten enthalten sind, gib eine leere Tabelle zurück.\n"
        "Extrahiere den Rechnungsempfänger aus dem Adressfeld des Dokuments, an den das Schreiben adressiert wurde."
    )

    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]

    if b64_image:
        messages[0]["content"].append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_image}"}})
    elif text:
        messages[0]["content"].append({"type": "text", "text": text[:4000]})

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        fehlermeldung = f"Fehler bei GPT-Inhaltsextraktion: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return ""

def kategorisiere_artikel_global(df):
    print("🧠 GPT: Kategorisiere Artikel global mit Log-Wiederverwendung …")

    artikel = df["Artikelbezeichnung"].dropna().unique()
    artikel_clean = pd.Series(artikel).astype(str).str.strip().str.lower()

    # Lade alte Logs
    vorhandene_logs = sorted(Path(output_excel.parent).glob("kategorielog_*.xlsx"))
    treffer_alt = pd.DataFrame()

    for log_path in reversed(vorhandene_logs):
        # Kategorielogs gegen Fehler absichern - robuste Varienate (18.06.25 SD)
        try:
            log_df = pd.read_excel(log_path)

            required_cols = {"Artikelbezeichnung", "Kategorie", "Unterkategorie"}
            if not required_cols.issubset(log_df.columns):
                print(f"⚠️ Log-Datei übersprungen (fehlende Spalten): {log_path.name}")
                continue

            log_df["clean"] = log_df["Artikelbezeichnung"].astype(str).str.strip().str.lower()
            log_df = log_df[["Artikelbezeichnung", "Kategorie", "Unterkategorie", "clean"]].drop_duplicates()
            treffer_alt = pd.concat([treffer_alt, log_df], ignore_index=True)
        except Exception as e:
            print(f"⚠️ Fehler beim Verarbeiten eines Logs ({log_path.name}): {e}")
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
            fehlermeldung = f"GPT-Fehler bei Kategorisierung: {e}"
            print(fehlermeldung)
            VERARBEITUNGSFEHLER.append(fehlermeldung)
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


# ==========================================
# 🔢 DATENPARSING UND TRANSFORMATION
# ==========================================

def parse_csv_in_dataframe(csv_text, dateiname):
    lieferanten_liste = ["Matthäi", "Eurovia", "Bauzentrum", "Remondis", "Kuhlmann", "BHK"]

    if "```" in csv_text:
        csv_text = csv_text.replace("```csv", "").replace("```", "").strip()

    lines = [line for line in csv_text.splitlines() if line.strip()]
    if not lines or ";" not in lines[0]:
        fehlermeldung = f"CSV-Parsing fehlgeschlagen: Kein valider CSV-Header in {dateiname}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return None

    if not lines[0].lower().startswith("artikelbezeichnung"):
        lines.insert(0, "Artikelbezeichnung;Menge;Einheit;Einzelpreis;Gesamtpreis;Lieferant;Rechnungsdatum;Rechnungsempfänger")

    try:
        df = pd.read_csv(StringIO("\n".join(lines)), sep=";", engine="python", on_bad_lines="skip")
        df["Dateiname"] = dateiname
        df["Lieferant_unbekannt"] = ~df["Lieferant"].apply(lambda x: any(l.lower() in str(x).lower() for l in lieferanten_liste))
        return df
    except Exception as e:
        fehlermeldung = f"Fehler beim Parsen von GPT-CSV ({dateiname}): {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return None

def plausibilitaet_pruefen(df):
    def bewerte_zeile(row):
        if not isinstance(row["Artikelbezeichnung"], str) or row["Artikelbezeichnung"].strip() == "":
            return ("Unplausibel", "Leere Artikelbezeichnung")
        if str(row["Menge"]).lower() in ["", "none", "nan"] or str(row["Gesamtpreis"]).lower() in ["", "none", "nan"]:
            return ("Unplausibel", "Fehlende Menge oder Gesamtpreis")
        if len(str(row["Artikelbezeichnung"])) < 4:
            return ("Verdächtig", "Sehr kurze Bezeichnung")
        return ("OK", "")

    try:
        status, bemerkung = zip(*df.apply(bewerte_zeile, axis=1))
        df["Plausibilitaet_Status"] = status
        df["Plausibilitaet_Bemerkung"] = bemerkung
    except Exception as e:
        fehlermeldung = f"Plausibilitätsprüfung fehlgeschlagen: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
    return df

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

    # Mapping-Datei automatisch erzeugen, falls sie fehlt
    if mapping_path and not Path(mapping_path).exists():
        print(f"📄 Mapping-Datei nicht gefunden – leeres Template wird angelegt: {mapping_path}")
        pd.DataFrame(columns=["Einheit_roh", "Einheit_normiert"]).to_excel(mapping_path, index=False)

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
 

# ==========================================
# 🧮 KATEGORISIERUNG & HARMONISIERUNG
# ==========================================

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

    # Mapping-Datei automatisch erzeugen, falls sie fehlt
    if mapping_path and not Path(mapping_path).exists():
        print(f"📄 Mapping-Datei nicht gefunden – leeres Template wird angelegt: {mapping_path}")
        pd.DataFrame(columns=["Einheit_roh", "Einheit_normiert"]).to_excel(mapping_path, index=False)

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
    
def kategorisiere_artikel_global(df):
    print("🧠 GPT: Kategorisiere Artikel global mit Log-Wiederverwendung …")

    artikel = df["Artikelbezeichnung"].dropna().unique()
    artikel_clean = pd.Series(artikel).astype(str).str.strip().str.lower()

    # Lade alte Logs
    vorhandene_logs = sorted(Path(output_excel.parent).glob("kategorielog_*.xlsx"))
    treffer_alt = pd.DataFrame()

    for log_path in reversed(vorhandene_logs):
        # Kategorielogs gegen Fehler absichern - robuste Varienate (18.06.25 SD)
        try:
            log_df = pd.read_excel(log_path)

            required_cols = {"Artikelbezeichnung", "Kategorie", "Unterkategorie"}
            if not required_cols.issubset(log_df.columns):
                print(f"⚠️ Log-Datei übersprungen (fehlende Spalten): {log_path.name}")
                continue

            log_df["clean"] = log_df["Artikelbezeichnung"].astype(str).str.strip().str.lower()
            log_df = log_df[["Artikelbezeichnung", "Kategorie", "Unterkategorie", "clean"]].drop_duplicates()
            treffer_alt = pd.concat([treffer_alt, log_df], ignore_index=True)
        except Exception as e:
            print(f"⚠️ Fehler beim Verarbeiten eines Logs ({log_path.name}): {e}")
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
            fehlermeldung = f"GPT-Fehler bei Kategorisierung: {e}"
            print(fehlermeldung)
            VERARBEITUNGSFEHLER.append(fehlermeldung)
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

def erkenne_zugehoerigkeit(text):
    lower = text.lower()
    for firma in TOCHTERFIRMEN:
        if firma in lower:
            return firma
    return "unbekannt"

# ==========================================
# 🧾 HAUPTVERARBEITUNG & ZUSAMMENFÜHRUNG
# ==========================================

def lade_verarbeitete_liste():
    if protokoll_excel.exists():
        try:
            return pd.read_excel(protokoll_excel)["Dateiname"].tolist()
        except Exception as e:
            fehlermeldung = f"Fehler beim Laden der Liste verarbeiteter Dateien: {e}"
            print(fehlermeldung)
            VERARBEITUNGSFEHLER.append(fehlermeldung)
            return []
    return []

def merge_and_enrich(ordner):
    global abbrechen
    abbrechen = False
    batches = list(ordner.glob("artikelpositionen_ki_batch_*.xlsx"))
    if not batches:
        print("⚠️ Keine Batchdateien zum Zusammenführen gefunden.")
        return

    frames = [pd.read_excel(f) for f in batches]
    merged = pd.concat(frames, ignore_index=True)

    # 🧼 Einheitenharmonisierung + Logging   
    merged = sicher_ausführen(harmonisiere_daten_mit_mapping, "Einheitenharmonisierung", merged, mapping_path="mein_mapping.xlsx")

    # 🔢 Zahlenbereinigung mit Rohwerten
    merged = sicher_ausführen(bereinige_zahlen, "Zahlenbereinigung", merged)

    # 🧠 Kategorisierung über alle Artikelbezeichnungen global
    ergebnis = sicher_ausführen(kategorisiere_artikel_global, "Kategorisierung", merged)
    if ergebnis is None:
        VERARBEITUNGSFEHLER.append("❌ Kategorisierung schlug fehl. Verarbeitung abgebrochen.")
        global abbrechen
        abbrechen = True
        return
    merged, logeintraege = ergebnis

    # 📁 Speichern der Gesamtausgabe
    gesamt_path = ordner / f"artikelpositionen_ki_GESAMT_{datetime.now():%Y%m%d_%H%M}.xlsx"
    merged.to_excel(gesamt_path, index=False)
    print(f"📊 Gesamtausgabe gespeichert: {gesamt_path.name}")

    # 📝 Speichern des Kategorielogs (nur wenn etwas kategorisiert wurde)
    if logeintraege:
        df_log = pd.DataFrame(logeintraege)
        df_log.to_excel(ordner / f"kategorielog_neu_{datetime.now():%Y%m%d_%H%M}.xlsx", index=False)
        print("✅ Kategorien wurden global via GPT erzeugt und geloggt.")
    
    # 📋 Hinweise für den Anwender
    zeige_next_steps_übersicht(ordner)    

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

# ==========================================
# 🚀 SKRIPTSTART
# ==========================================

if __name__ == "__main__":
    try:
        hauptprozess()
    except Exception as e:
        print("\n❌ Unerwarteter Abbruch im Hauptprozess:")
        print(f"   {e}")
        VERARBEITUNGSFEHLER.append(f"Hauptprozess-Abbruch: {e}")
    finally:
        print("\n🧾 Zusammenfassung nach Skriptlauf:")
        if VERARBEITUNGSFEHLER:
            print("⚠️ Es sind folgende Fehler aufgetreten:")
            for fehler in VERARBEITUNGSFEHLER:
                print(f" - {fehler}")
            print("\n📋 Bitte behebe die Fehler und starte das Skript erneut.")

            # Log schreiben
            with open("fehlerprotokoll.txt", "w", encoding="utf-8") as f:
                for eintrag in VERARBEITUNGSFEHLER:
                    f.write(f"{eintrag}\n")
        else:
            print("✅ Keine kritischen Fehler. Der Lauf war erfolgreich.")
        if 'abbrechen' in globals() and abbrechen:
            print("\n❌ Kritischer Fehler erkannt: Verarbeitung wurde bewusst abgebrochen.")
            print("📋 Bitte Ursache beheben (siehe fehlerprotokoll.txt), dann erneut starten.")
