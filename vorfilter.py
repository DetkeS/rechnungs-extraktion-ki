import fitz  # PyMuPDF

def pdf_hat_text(pdf_path, min_zeichen=50):
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text = page.get_text()
            if text and len(text.strip()) >= min_zeichen:
                return True
        return False
    except:
        return False