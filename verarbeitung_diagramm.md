```mermaid
flowchart TD
    A[Start: main.py] --> B[Lade Liste verarbeiteter Dateien]
    B --> C[Durchlaufe PDFs in /zu_verarbeiten]
    C --> D{PDF enthält ausreichend Text?}
    D -- Ja --> E[GPT-Klassifikation (dokument_klassifizieren)]
    D -- Nein --> F[OCR via GPT (ocr_fallback)]
    F --> E
    E --> G{Dokumententyp == "rechnung"?}
    G -- Nein --> H[Verschiebe in Nicht-Rechnung]
    G -- Ja --> I[Extrahiere Artikel per GPT (gpt_datenabfrage)]
    I --> J[Parse CSV in DataFrame (csv_parser)]
    J --> K{Tabelle gültig?}
    K -- Nein --> L[Verschiebe in Problemordner]
    K -- Ja --> M[Prüfe Plausibilität (plausibilitaet)]
    M --> N[Füge zu Ergebnisliste hinzu]
    N --> O[Excel-Datei speichern]
    O --> P[Ende]
```