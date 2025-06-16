import pandas as pd
from daten.verarbeitungspfade import protokoll_excel
from datetime import datetime

def lade_verarbeitete_liste():
    if protokoll_excel.exists():
        return pd.read_excel(protokoll_excel)["Dateiname"].tolist()
    return []

def speichere_verarbeitete_datei(dateiname):
    df = pd.DataFrame([[dateiname, datetime.now().strftime("%Y-%m-%d %H:%M")]],
                      columns=["Dateiname", "Verarbeitet am"])
    if protokoll_excel.exists():
        bestehend = pd.read_excel(protokoll_excel)
        df = pd.concat([bestehend, df], ignore_index=True)
    df.to_excel(protokoll_excel, index=False)