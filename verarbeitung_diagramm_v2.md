```mermaid
flowchart TD
    A[PDF im Eingangsordner] --> B[Vorfilter: Textlayer vorhanden]
    B -- Nein --> C[Erzeuge PNG aus erster Seite]
    C --> D[GPT-Klassifikation mit Bild (Vision)]
    B -- Ja --> E[GPT-Klassifikation mit extrahiertem Text]
    D --> F{Dokumenttyp == rechnung?}
    E --> F
    F -- Nein --> X[Verschiebe in "Nicht-Rechnung"]
    F -- Ja --> Y[Verschiebe in Archivordner]
    Y --> Z[Weitere Verarbeitung (Artikel extrahieren etc.)]
