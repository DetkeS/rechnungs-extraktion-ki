import fitz  # PyMuPDF

def pdf_hat_nutzbaren_text(pdf_path, min_verwertbar=80):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        return len(text.strip()) >= min_verwertbar and not text.startswith("VL<?LHH")
    except:
        return False