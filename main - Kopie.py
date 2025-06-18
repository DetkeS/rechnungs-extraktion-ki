# ==========================================
# 📦 INITIALISIERUNG UND KONFIGURATION
# ==========================================
# 🔁 System & Dateioperationen
import os              # z. B. für Umgebungsvariablen oder Dateinamen prüfen
import sys             # zur Umleitung von stdout für Logging
import shutil          # zum Verschieben von Dateien
import atexit          # für automatische Sicherung bei Abbruch
import time            # für Statistik-Ausgaben (z. B. Gesamtdauer)
import traceback       # ➕ Für vollständige Fehlermeldungen mit Traceback

# 📊 Datenverarbeitung
import pandas as pd    # Tabellenverarbeitung für CSV, XLSX
from io import StringIO  # um Text als Dateiobjekt zu behandeln (z. B. für CSV-PARSING)

# 🗂 Pfad- und Zeitsteuerung
from datetime import datetime  # für Zeitstempel in Dateinamen
from pathlib import Path       # Plattformunabhängige Pfaddefinitionen

# 📄 PDF-Verarbeitung (Text- und Bildextraktion)
import fitz            # PyMuPDF – extrahiert Text aus PDFs
from pdf2image import convert_from_path  # erzeugt Bilder aus PDF-Seiten

# 📦 Bildverarbeitung
from base64 import b64encode  # für GPT-Bilder als base64 (z. B. erste Seite einer PDF)
from io import BytesIO        # für temporären Bildspeicher (PNG in base64)

# 🧠 OpenAI API
import openai                  # GPT-Modelle aufrufen (z. B. für Klassifikation, OCR, Kategorisierung)
from dotenv import load_dotenv  # .env-Dateien lesen für sichere API-Key-Verwaltung
from openai import OpenAI  # ✅ neue Client-API für openai>=1.0

# 🔐 API-Key aus .env-Datei laden (nicht im Code sichtbar speichern)
# 🔄 Lade Umgebungsvariablen aus .env-Datei (muss im Hauptverzeichnis liegen)
load_dotenv()
# 📌 API-Key laden und an OpenAI-Client übergeben (für SDK ≥ 1.0)
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("❌ Kritischer Fehler: OPENAI_API_KEY nicht gesetzt.")
    print("ℹ️  Bitte prüfe deine .env-Datei oder setze den API-Key manuell.")
    print("📋 Skript wird aus Sicherheitsgründen abgebrochen.")
    try:
        fehlerlog_datei = output_excel.parent / f"{zeitstempel}_fehlerprotokoll.txt"
        with open(fehlerlog_datei, "a", encoding="utf-8") as f:
            f.write("❌ Kein OpenAI API-Key gefunden – kritischer Abbruch.\n")
    except Exception:
        pass
    sys.exit(1)

# 🧠 GPT-Client initialisieren
client = OpenAI(api_key=api_key)

# 📁 Pfade & Dateinamen (werden beim Start automatisch erstellt)
basisverzeichnis = Path(__file__).resolve().parent            # Hauptverzeichnis der Skriptdatei
zeitstempel = datetime.now().strftime('%Y%m%d_%H%M')          # Zeitstempel für Archiv-Ordner

input_folder = basisverzeichnis / "zu_verarbeiten"            # Eingang für neue Rechnungen
archiv_folder = basisverzeichnis / f"{zeitstempel}_verarbeitet"          # erfolgreich verarbeitet
nicht_rechnung_folder = basisverzeichnis / f"{zeitstempel}_nicht_rechnung"  # z. B. Angebote, Werbung etc.
problemordner = basisverzeichnis / f"{zeitstempel}_problemrechnungen"    # unklare/fehlerhafte Fälle
bereits_verarbeitet_ordner = basisverzeichnis / f"{zeitstempel}_bereits_verarbeitet"  # Duplikate
output_excel = basisverzeichnis / "artikelpositionen_ki.xlsx" # Haupt-Ausgabedatei
protokoll_excel = basisverzeichnis / "verarbeitete_dateien.xlsx"  # Logbuch über bereits verarbeitete Dateien

# 🏢 Bekannte Einheiten oder Firmen
TOCHTERFIRMEN = ["Wähler", "Kuhlmann", "BHK", "Mudcon", "Seier"]  # Für Zuordnungen von Rechnungsempfängern

# 🛑 Zentrale Fehlerliste für Laufzeitfehler
VERARBEITUNGSFEHLER = []


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


# ==========================================
# 🛠️ HILFSFUNKTIONEN & WERKZEUGE
# ==========================================

# Logging einschalten
logdatei = output_excel.parent / f"{zeitstempel}_verarbeitung_log.txt"
sys.stdout = DualLogger(logdatei)
print(f"💾 Logging aktiv: {logdatei.name}")

# Abbrüche verhindern -Robuste Verarbeitung
def sicher_ausführen(funktion, name, *args, **kwargs):
    try:
        return funktion(*args, **kwargs)
    except Exception:
        tb = traceback.format_exc()  # 🔍 Hole kompletten Fehler inklusive Zeile
        fehlermeldung = f"\n❌ Fehler in '{name}':\n{tb}"
        print(fehlermeldung)  # ✅ Ausgabe in GUI (Terminal)
        VERARBEITUNGSFEHLER.append(fehlermeldung)  # ✅ Logging fürs Fehlerprotokoll
        return None
        
# 🔁 Robuster Datei-Move: erstellt Zielordner nur bei Bedarf
def move_with_folder(src_path, target_folder, target_filename):
    target_folder.mkdir(parents=True, exist_ok=True)
    shutil.move(src_path, target_folder / target_filename)
  
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
        doc = fitz.open(pfad)
        return "\n".join([page.get_text() for page in doc]).strip()
    except Exception as e:
        fehlermeldung = f"PDF-Text konnte nicht extrahiert werden ({pfad}): {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
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
        return len(text.strip()) >= 50 and not text.startswith("VL<?LHH")
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
            "Du erhältst ein Bild eines Geschäftsdokuments (z. B. Rechnung, Gutschrift, Anschreiben, Mahnung).\n"
            "Bitte klassifiziere das Dokument eindeutig anhand typischer Begriffe oder Layoutstruktur.\n\n"
            "Typen zur Auswahl:\n"
            "- rechnung (z. B. 'Rechnung', 'Rechnungsnummer', 'Zahlbetrag', 'USt.')\n"
            "- gutschrift (z. B. 'Gutschrift', 'Rechnungskorrektur', 'Erstattung')\n"
            "- mahnung (z. B. 'Mahnung', 'letzte Erinnerung')\n"
            "- zahlungserinnerung\n"
            "- anschreiben\n"
            "- email\n"
            "- behördlich\n"
            "- sonstiges\n\n"
            "Achte besonders auf Begriffe oben rechts oder in der Kopfzeile.\n"
            "Antworte ausschließlich mit **einem** dieser Begriffe."
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
            "Analysiere den folgenden extrahierten Text eines Geschäftsdokuments und gib **genau einen** der folgenden Dokumenttypen zurück:\n\n"
            "- rechnung (z. B. mit 'Rechnung', 'Rechnungsnummer', 'USt.', 'Zahlbetrag')\n"
            "- gutschrift (z. B. mit 'Gutschrift', 'Rechnungskorrektur', 'Erstattung')\n"
            "- mahnung\n- zahlungserinnerung\n- anschreiben\n- email\n- behördlich\n- sonstiges\n\n"
            "Wenn der Text mehrere Begriffe enthält, wähle den eindeutigsten und plausibelsten Typ.\n"
            "Antwort nur mit dem Begriff.\n\n"
            f"{extracted_text[:3000]}"
        )
        messages = [{"role": "user", "content": prompt}]
    else:
        fehlermeldung = "gpt_klassifikation: Kein text_path oder image_b64 übergeben."
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return "fehler"

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
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
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
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
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
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
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        fehlermeldung = f"Fehler bei GPT-Inhaltsextraktion: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return ""

def kategorisiere_artikel_global(df):
    print("🧠 Starte globale Artikel-Kategorisierung mit GPT und Log-Wiederverwendung …")

    artikel = df["Artikelbezeichnung"].dropna().unique()
    artikel_clean = pd.Series(artikel).astype(str).str.strip().str.lower()
    artikel_set = pd.DataFrame({"Artikelbezeichnung": artikel, "clean": artikel_clean})

    # 🗂 Kategorielogs laden
    vorhandene_logs = sorted(Path(output_excel.parent).glob("kategorielog_*.xlsx"))
    treffer_alt = pd.DataFrame()
    for log_path in reversed(vorhandene_logs):
        try:
            log_df = pd.read_excel(log_path)
            if {"Artikelbezeichnung", "Kategorie", "Unterkategorie"}.issubset(log_df.columns):
                log_df["clean"] = log_df["Artikelbezeichnung"].astype(str).str.strip().str.lower()
                log_df = log_df[["Artikelbezeichnung", "Kategorie", "Unterkategorie", "clean"]].drop_duplicates()
                treffer_alt = pd.concat([treffer_alt, log_df], ignore_index=True)
            else:
                print(f"⚠️ Unvollständiger Log – wird ignoriert: {log_path.name}")
        except Exception as e:
            print(f"⚠️ Fehler beim Lesen eines Logs ({log_path.name}): {e}")

    if treffer_alt.empty or not {"Kategorie", "Unterkategorie", "clean"}.issubset(treffer_alt.columns):
        print("ℹ️ Keine brauchbaren Kategorielogs – GPT wird vollständig verwendet.")
        reuse_df = pd.DataFrame(columns=["Artikelbezeichnung", "Kategorie", "Unterkategorie", "Herkunft"])
    else:
        try:
            treffer_alt = treffer_alt[
                ~treffer_alt["Kategorie"].str.lower().isin(["sonstiges", "fehler", "unbekannt"]) &
                ~treffer_alt["Unterkategorie"].str.lower().isin(["unklar", "unbekannt"])
            ]
            reuse_df = artikel_set.merge(treffer_alt, on="clean", how="inner").drop_duplicates("Artikelbezeichnung")
            reuse_df["Herkunft"] = "reuse"
        except Exception as e:
            print(f"⚠️ Wiederverwendung fehlgeschlagen: {e}")
            reuse_df = pd.DataFrame(columns=["Artikelbezeichnung", "Kategorie", "Unterkategorie", "Herkunft"])

    # 🧠 GPT für neue Begriffe
    bekannte = reuse_df["Artikelbezeichnung"] if not reuse_df.empty else []
    gpt_df = artikel_set[~artikel_set["Artikelbezeichnung"].isin(bekannte)]
    logeintraege = []

    if not gpt_df.empty:
        prompt = (
            "Ordne den folgenden Artikeln je eine passende Haupt- und Unterkategorie zu.\n"
            "Gib nur die CSV-Zeilen mit folgenden Spalten zurück:\n"
            "Artikelbezeichnung;Hauptkategorie;Unterkategorie\n\n" +
            "\n".join(gpt_df["Artikelbezeichnung"])
        )
        try:
           # ✅ NEU – Kompatibel mit openai>=1.0.0 (Client-Instanz verwenden)
            client = OpenAI()
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            antwort = response['choices'][0]['message']['content'].strip()
            lines = [line for line in antwort.splitlines() if ";" in line]
            cat_gpt = pd.read_csv(StringIO("\n".join(lines)), sep=";", engine="python", on_bad_lines="skip")
            cat_gpt["Herkunft"] = "gpt"
        except Exception as e:
            fehlermeldung = f"GPT-Kategorisierung fehlgeschlagen: {e}"
            print(f"⚠️ {fehlermeldung}")
            VERARBEITUNGSFEHLER.append(fehlermeldung)
            cat_gpt = pd.DataFrame(columns=["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"])
    else:
        cat_gpt = pd.DataFrame(columns=["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"])

   # 🧩 Vereinheitlichung & Zusammenführung
    if not reuse_df.empty and "Kategorie" in reuse_df.columns:
        reuse_df = reuse_df.rename(columns={"Kategorie": "Hauptkategorie"})
    elif not reuse_df.empty:
        fehlermeldung = "⚠️ 'Kategorie'-Spalte fehlt in reuse_df – keine Umbenennung möglich"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)

    # ✅ Spaltenprüfung vor concat
    gewuenschte_spalten = ["Artikelbezeichnung", "Hauptkategorie", "Unterkategorie", "Herkunft"]
    fehlen = [col for col in reuse_df.columns if col not in gewuenschte_spalten]

    if fehlen:
        fehlermeldung = f"⚠️ Spalten fehlen in reuse_df: {fehlen} – es wird ein leeres DataFrame verwendet"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        reuse_df = pd.DataFrame(columns=gewuenschte_spalten)

    gesamt_kat = pd.concat(
        [reuse_df[gewuenschte_spalten], cat_gpt],
        ignore_index=True
    )
    # 🧬 Merge in Originaldaten
    df = df.merge(gesamt_kat, on="Artikelbezeichnung", how="left")
    if "Hauptkategorie" in df.columns:
        df.rename(columns={"Hauptkategorie": "Kategorie"}, inplace=True)
    else:
        fehlermeldung = "❌ Spalte 'Hauptkategorie' fehlt – Merge fehlgeschlagen."
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        raise ValueError(fehlermeldung)

    if "Kategorie" not in df.columns:
        raise ValueError("Spalte 'Kategorie' fehlt – Kategorisierung unvollständig")

    # 📝 Log-Einträge vorbereiten
    for _, row in gesamt_kat.iterrows():
        logeintraege.append({
            "Artikelbezeichnung": row["Artikelbezeichnung"],
            "Kategorie": row["Hauptkategorie"],
            "Unterkategorie": row["Unterkategorie"],
            "Herkunft": row["Herkunft"],
            "Zeitpunkt": datetime.now()
        })

    if not logeintraege:
        print("⚠️ Keine neuen Kategorisierungen vorgenommen – kein Kategorielog gespeichert.")

    return df, logeintraege

   
# ==========================================
# 🔢 DATENPARSING UND TRANSFORMATION
# ==========================================

def parse_csv_in_dataframe(csv_text, dateiname):
    if not csv_text or not isinstance(csv_text, str):
        fehlermeldung = f"❌ CSV-Parsing fehlgeschlagen: Keine oder ungültige Antwort für {dateiname}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return None

    # Frühfilter: Ist überhaupt ein Semikolon vorhanden?
    if ";" not in csv_text:
        fehlermeldung = f"❌ CSV-Parsing abgebrochen: Antwort enthält keine tabellarischen Daten (kein ';') für {dateiname}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return None

    # Entferne GPT-Formatierung wie ```csv ... ```
    if "```" in csv_text:
        csv_text = csv_text.replace("```csv", "").replace("```", "").strip()

    # Zeilen aufbereiten
    lines = [line.strip() for line in csv_text.splitlines() if line.strip()]

    # Prüfen ob erste Zeile Header ist
    header_ist_korrekt = lines and "artikelbezeichnung" in lines[0].lower()

    if not header_ist_korrekt:
        print(f"⚠️ Kein valider Header erkannt – Ersetze durch Standard-Header in {dateiname}")
        standard_header = "Artikelbezeichnung;Menge;Einheit;Einzelpreis;Gesamtpreis;Lieferant;Rechnungsdatum;Rechnungsempfänger;Rechnungsnummer"
        lines.insert(0, standard_header)

    try:
        df = pd.read_csv(StringIO("\n".join(lines)), sep=";", engine="python", on_bad_lines="skip")
        if df.empty or "Artikelbezeichnung" not in df.columns:
            raise ValueError("Leeres oder ungültiges DataFrame")
        df["Dateiname"] = dateiname
        return df
    except Exception as e:
        fehlermeldung = f"❌ Fehler beim Parsen von GPT-CSV ({dateiname}): {e}"
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
        log_path = output_excel.parent / f"{zeitstempel}_einheiten_log.xlsx"
        log_df.to_excel(log_path, index=False)
        print(f"📝 {len(unbekannte)} unbekannte Einheiten gespeichert in: {log_path}")

    return df
  
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
        #global abbrechen überflüssig da oben schon? 18.06.25 SD
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
    print("")
    print("═" * 60)  # 🔽 Visuelle Trennung vor erster Datei
    for index, pdf_path in enumerate(pdf_files[:BATCH_SIZE], 1):
        start = time.time()
        dateiname = pdf_path.name
        print(f"➞️ {index}/{len(pdf_files)}: {dateiname}")
        print("🛂 Starte Vorprüfung der Datei")
        if dateiname in verarbeitete:
            print("⏭️ Bereits verarbeitet.")
            print("")  # ✅ NEU: Leerzeile vor continue
            move_with_folder(pdf_path, bereits_verarbeitet_ordner, dateiname)
            speichere_verarbeitete_datei(dateiname)
            continue
        ist_lesbar = pdf_hat_nutzbaren_text(pdf_path)
        print(f"🔍 Textlayer vorhanden: {'JA' if ist_lesbar else 'NEIN'}")
        if not ist_lesbar:
            b64 = konvertiere_erste_seite_zu_base64(pdf_path)
            if not b64:
                print("⚠️ Kein OCR möglich → verschoben.")
                print("")  # ✅ NEU: Leerzeile vor continue
                move_with_folder(pdf_path, problemordner, f"unlesbar_{dateiname}")
                speichere_verarbeitete_datei(dateiname)
                probleme += 1
                continue
            klassifikation = gpt_klassifikation(image_b64=b64)
            print("🔠 Starte GPT-Klassifikation auf Bildbasis (OCR)")
            text = gpt_abfrage_ocr_text(b64)
            dauer = time.time() - start
            print(f"✅ Fertig in {dauer:.1f}s")
            print("")  # ➕ neue Leerzeile nach OCR/Textverarbeitung
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
            print("")
            move_with_folder(pdf_path, nicht_rechnung_folder, f"{klassifikation}_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            nicht_rechnungen += 1
            print(f"❌ Nicht-Rechnung → verschoben nach: {klassifikation}_{dateiname}")
            print("")
            continue
        print("📤 Sende Text zur GPT-Inhaltsextraktion …")
        antwort = gpt_abfrage_inhalt(text=text)
        if antwort.strip().lower().startswith("fehler"):
            print("⚠️ GPT-Inhaltsextraktion fehlgeschlagen → Problemrechnungen")
            print("")
            move_with_folder(pdf_path, problemordner, f"GPT_Fehler_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        print("✅ Starte Plausibilitätsprüfung der Tabelle …")
        print("")
        df = parse_csv_in_dataframe(antwort, dateiname)
        if df is None or df.empty:
            print("⚠️ Tabelle leer oder fehlerhaft → Problemrechnungen")
            print("")
            move_with_folder(pdf_path, problemordner, f"Tabelle_unbrauchbar_{dateiname}")
            speichere_verarbeitete_datei(dateiname)
            probleme += 1
            continue
        df["Dokumententyp"] = klassifikation
        df["Klassifikation_vor_Plausibilitaet"] = klassifikation
        df["Verfahren"] = verfahren
        df["Verarbeitung_Dauer"] = round(dauer, 2)
        df["Zugehörigkeit"] = erkenne_zugehoerigkeit(text)
        alle_dfs.append(df)
        move_with_folder(pdf_path, archiv_folder, dateiname)
        speichere_verarbeitete_datei(dateiname)
        if index % FLUSH_INTERVAL == 0:
            flush = pd.concat(alle_dfs, ignore_index=True)
            flush.to_excel(output_excel.parent / f"artikelpositionen_ki_batch_{index}.xlsx", index=False)
            print(f"📏 Zwischenspeicherung nach {len(flush)} Dateien: artikelpositionen_ki_batch_{index}.xlsx")
            alle_dfs.clear()
        print("")  # ➕ Fügt nach jedem Datei-Durchlauf eine Leerzeile ein
    if alle_dfs:
        timestamped = output_excel.parent / f"artikelpositionen_ki_batch_final.xlsx"
        pd.concat(alle_dfs, ignore_index=True).to_excel(timestamped, index=False)

    merge_and_enrich(output_excel.parent)

    gesamt_dauer = time.time() - gesamt_start
    gesamt = anzahl_text + anzahl_ocr
    print("\n\n" + "═" * 60) # 🔽 Visuelle Trennung vor der Abschlussausgabe zur besseren Lesbarkeit
    
    # ⏱️ Ausgabe der Gesamtdauer in Stunden, Minuten, Sekunden
    stunden = int(gesamt_dauer // 3600)
    minuten = int((gesamt_dauer % 3600) // 60)
    sekunden = int(gesamt_dauer % 60)
    print(f"🌟 Verarbeitung beendet: {gesamt} Dateien in {gesamt_dauer:.1f}s")
    print("")  # ➕ Fügt eine Leerzeile ein
    print("📊 Verarbeitungsstatistik:\n")
    if gesamt:
        print(f"📜 Ø/Datei: {gesamt_dauer/gesamt:.2f}s")
        print(f"📄 Textbasiert: {anzahl_text} ({anzahl_text/gesamt:.1%}), Ø {dauer_text/max(1,anzahl_text):.2f}s")
        print(f"🧠 GPT-OCR: {anzahl_ocr} ({anzahl_ocr/gesamt:.1%}), Ø {dauer_ocr/max(1,anzahl_ocr):.2f}s")
        print(f"❌ Nicht-Rechnungen: {nicht_rechnungen} ({nicht_rechnungen/gesamt:.1%})")
        print(f"⚠️ Probleme: {probleme} ({probleme/gesamt:.1%})")

# ==========================================
# 🚀 SKRIPTSTART
# ==========================================

print("🧪 Aktive Version: Patchstand 20250618_1445")
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
                print(f"{fehler}")  # ✅ mit vollem Traceback direkt anzeigen

            print("\n📋 Bitte behebe die Fehler und starte das Skript erneut.")

            # 📝 Fehlerprotokoll mit Zeitstempel speichern
            fehlerlog_datei = output_excel.parent / f"{zeitstempel}_fehlerprotokoll.txt"
            with open(fehlerlog_datei, "w", encoding="utf-8") as f:
                f.write("🧾 Fehlerprotokoll mit vollständigen Tracebacks:\n\n")
                for eintrag in VERARBEITUNGSFEHLER:
                    f.write(f"{eintrag}\n\n")
        else:
            print("✅ Keine kritischen Fehler. Der Lauf war erfolgreich.")

        if 'abbrechen' in globals() and abbrechen:
            print("\n❌ Kritischer Fehler erkannt: Verarbeitung wurde bewusst abgebrochen.")
            print(f"📋 Bitte Ursache beheben (siehe {fehlerlog_datei.name}), dann erneut starten.")

