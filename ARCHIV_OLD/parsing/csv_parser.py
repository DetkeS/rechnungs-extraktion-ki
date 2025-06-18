import pandas as pd
from io import StringIO

lieferanten_liste = ["Matthäi", "Eurovia", "Bauzentrum", "Remondis", "Kuhlmann", "BHK"]

def parse_csv_in_dataframe(csv_text, dateiname):
    if "```" in csv_text:
        csv_text = csv_text.replace("```csv", "").replace("```", "").strip()
    lines = [line for line in csv_text.splitlines() if line.strip()]
    if not lines or not ";" in lines[0]:
        return None
    if not lines[0].lower().startswith("artikelbezeichnung"):
        lines.insert(0, "Artikelbezeichnung;Menge;Einheit;Einzelpreis;Gesamtpreis;Lieferant;Rechnungsdatum;Rechnungsempfänger")
    try:
        df = pd.read_csv(StringIO("\n".join(lines)), sep=";", engine="python", on_bad_lines="skip")
        df["Dateiname"] = dateiname
        df["Lieferant_unbekannt"] = ~df["Lieferant"].apply(lambda x: any(l.lower() in str(x).lower() for l in lieferanten_liste))
        return df
    except:
        return None