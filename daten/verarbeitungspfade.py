from pathlib import Path
from datetime import datetime

basisverzeichnis = Path(__file__).resolve().parent.parent
zeitstempel = datetime.now().strftime('%Y%m%d_%H%M')

input_folder = basisverzeichnis / "zu_verarbeiten"
archiv_folder = basisverzeichnis / f"{zeitstempel}_verarbeitet"
nicht_rechnung_folder = basisverzeichnis / f"{zeitstempel}_nicht_rechnung"
problemordner = basisverzeichnis / f"{zeitstempel}_problemrechnungen"
bereits_verarbeitet_ordner = basisverzeichnis / f"{zeitstempel}_bereits_verarbeitet"
output_excel = basisverzeichnis / "artikelpositionen_ki.xlsx"
protokoll_excel = basisverzeichnis / "verarbeitete_dateien.xlsx"

for pfad in [input_folder, archiv_folder, nicht_rechnung_folder, problemordner,bereits_verarbeitet_ordner]:
    pfad.mkdir(parents=True, exist_ok=True)