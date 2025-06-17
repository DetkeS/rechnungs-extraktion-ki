```mermaid
flowchart TD
    A[PDF im Eingangsordner] --> B[Vorfilter Textlayer vorhanden]
    B -- Nein --> C[Erzeuge PNG aus erster Seite]
    C --> D[GPT Klassifikation Bildbasiert]
    B -- Ja --> E[GPT Klassifikation Textbasiert]
    D --> F{Dokumenttyp ist rechnung}
    E --> F
    F -- Nein --> X[Verschiebe in Nicht Rechnung Ordner]
    F -- Ja --> Y[Verschiebe in Archivordner]
    Y --> Z[Weitere Verarbeitung Artikel Extraktion]
