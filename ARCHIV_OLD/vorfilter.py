import fitz  # Falls noch nicht vorhanden

def pdf_hat_nutzbaren_text(pdf_path, min_verwertbar=80):
    try:
        doc = fitz.open(pdf_path)
        text = "".join([page.get_text() for page in doc])
        return len(text.strip()) >= min_verwertbar and not text.startswith("VL<?LHH")
    except Exception as e:
        fehlermeldung = f"Fehler beim PDF-Vorfilter für {pdf_path}: {e}"
        print(fehlermeldung)
        VERARBEITUNGSFEHLER.append(fehlermeldung)
        return False
