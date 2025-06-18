
# 🧾 KI-gestützte Rechnungsverarbeitung

Dieses Skript verarbeitet PDF-Rechnungen automatisiert mit Hilfe von GPT-4o, OCR, Extraktion und intelligenter Kategorisierung. Es wurde speziell für wiederkehrende PDF-Rechnungen aus Bau- und Handwerksumgebungen entwickelt und berücksichtigt Fehlerrobustheit, Protokollierung und Nachvollziehbarkeit.

---

## 🔧 Funktionen

- **PDF-Verarbeitung** (OCR & Texterkennung via GPT und PyMuPDF)
- **Kategorisierung** von Artikelzeilen (GPT + Log-Reuse)
- **Fehlerkorrektur** bei Zahlwerten (Fallback mit GPT)
- **Datenharmonisierung** über Mapping-Datei
- **Plausibilitätsprüfung** der extrahierten Daten
- **Batch-Verarbeitung** vieler Rechnungen
- **Robuster Abbruchschutz** und Fehlerprotokollierung
- **Next Steps Anleitung für User am Ende

---

## 📂 Ordnerstruktur

| Ordner | Bedeutung |
|--------|-----------|
| `zu_verarbeiten/` | Neue PDF-Rechnungen ablegen |
| `*_verarbeitet/` | Erfolgreich verarbeitete Rechnungen |
| `*_nicht_rechnung/` | Werbungen oder irrelevante Dokumente |
| `*_problemrechnungen/` | PDF-Dateien mit Extraktionsfehlern |
| `*_bereits_verarbeitet/` | Doppelt erkannte Dateien |

Zusätzlich werden erzeugt:
- `artikelpositionen_ki.xlsx` → Hauptausgabe
- `kategorielog_neu_*.xlsx` → GPT-Kategorielog
- `verarbeitete_dateien.xlsx` → Liste aller bearbeiteten Rechnungen
- `fehlerprotokoll.txt` → Zentrale Fehlerliste (sofern nötig)

---

## 🧠 GPT-Nutzung

- GPT-4o wird verwendet für:
  - Kategorisierung von Artikelzeilen
  - Inhaltsextraktion bei OCR oder schlechtem PDF
  - Fehlerkorrektur bei Zahlenformaten

Alle GPT-Funktionen sind abgesichert über `try/except` und sparen API-Kosten durch Wiederverwendung und Protokolle.

---

## ✅ Nutzung

1. Lege Rechnungs-PDFs in den Ordner `zu_verarbeiten/`
2. Starte `main.py`
3. Nach Abschluss: Lies die Datei `fehlerprotokoll.txt` und `artikelpositionen_ki.xlsx`
4. Prüfe ggf. `kategorielog_neu_...xlsx` für neue GPT-Kategorisierungen
5. Neue Einheiten ggf. in Mapping-Datei übernehmen

---

## 📌 Hinweise

- Nicht verschobene Batch-Dateien werden beim nächsten Lauf wiederverarbeitet.
- GPT-Nutzung kann bei schlechter Internetverbindung fehlschlagen → Fehler erscheinen in `fehlerprotokoll.txt`.
- Mapping-Datei pflegen, um wiederkehrende Einheiten automatisch zuzuordnen.
- Kategorien werden aus Log wiederverwendet, wenn sinnvoll.

---

## 🛑 Abbruch & Wiederaufnahme

Bei Skriptabbruch:
- Die bisher verarbeiteten Daten bleiben in der `.xlsx` erhalten.
- Fehler werden geloggt.
- Beim nächsten Lauf kann sicher weitergearbeitet werden.

---

## 🧪 Kontakt / Support

> Für Anpassungen, Verbesserungsvorschläge oder Support bitte an [Entwickler / KI-Berater] wenden.
