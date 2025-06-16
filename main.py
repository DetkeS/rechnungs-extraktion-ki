# Auszug aus dem relevanten Hauptteil der main.py

...
start = time.time()
...
if not ist_lesbar:
    ...
    klassifikation = gpt_klassifikation(image_b64=b64)
    text = gpt_abfrage_ocr_text(b64)
    verfahren = "gpt-ocr"
else:
    klassifikation = gpt_klassifikation(text_path=pdf_path)
    text = extrahiere_text_aus_pdf(pdf_path)
    verfahren = "text"

dauer = time.time() - start
...
df["Dokumententyp"] = klassifikation
df["Klassifikation_vor_Plausibilitaet"] = klassifikation
df["Verfahren"] = verfahren
df["Verarbeitung_Dauer"] = round(dauer, 2)
df = plausibilitaet_pruefen(df)
alle_dfs.append(df)
...