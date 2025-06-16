```mermaid
flowchart TD
    A[PDF im Eingangsordner] --> B[Vorfilter: Textlayer vorhanden? (PyMuPDF)]
    B -- Nein --> C[→ PNG erstellen und GPT-Klassifikation (Vision)]
    B -- Ja --> D[→ GPT-Klassifikation basierend auf Text]
    C --> E{Dokumenttyp == "rechnung"?}
    D --> E
    E -- Nein --> X[Verschiebe in "Nicht-Rechnung"]
    E -- Ja --> Y[Verschiebe in Archivordner]
    Y --> Z[→ Weiterverarbeitung (Inhaltsextraktion etc.)]
```