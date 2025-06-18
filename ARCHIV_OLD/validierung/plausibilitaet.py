def plausibilitaet_pruefen(df):
    def bewerte_zeile(row):
        if not isinstance(row["Artikelbezeichnung"], str) or row["Artikelbezeichnung"].strip() == "":
            return ("Unplausibel", "Leere Artikelbezeichnung")
        if str(row["Menge"]).lower() in ["", "none", "nan"] or str(row["Gesamtpreis"]).lower() in ["", "none", "nan"]:
            return ("Unplausibel", "Fehlende Menge oder Gesamtpreis")
        if len(str(row["Artikelbezeichnung"])) < 4:
            return ("Verdächtig", "Sehr kurze Bezeichnung")
        return ("OK", "")
    status, bemerkung = zip(*df.apply(bewerte_zeile, axis=1))
    df["Plausibilitaet_Status"] = status
    df["Plausibilitaet_Bemerkung"] = bemerkung
    return df