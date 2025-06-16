import pandas as pd
from validierung.plausibilitaet import plausibilitaet_pruefen

def test_plausibilitaet_pruefen():
    df = pd.DataFrame([
        {
            "Artikelbezeichnung": "Bausand 0-2",
            "Menge": "20",
            "Einheit": "t",
            "Einzelpreis": "4.50",
            "Gesamtpreis": "90.00"
        },
        {
            "Artikelbezeichnung": "",
            "Menge": "5",
            "Einheit": "m³",
            "Einzelpreis": "12.00",
            "Gesamtpreis": "60.00"
        }
    ])
    df_checked = plausibilitaet_pruefen(df)
    assert df_checked.loc[0, "Plausibilitaet_Status"] == "OK"
    assert df_checked.loc[1, "Plausibilitaet_Status"] == "Unplausibel"