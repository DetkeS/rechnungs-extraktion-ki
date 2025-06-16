from PyPDF2 import PdfReader
from pdf2image import convert_from_path
from base64 import b64encode
from io import BytesIO

def extrahiere_text_aus_pdf(pfad):
    try:
        reader = PdfReader(pfad)
        return "\n".join([page.extract_text() or "" for page in reader.pages]).strip()
    except:
        return ""

def konvertiere_erste_seite_zu_base64(pfad):
    try:
        image = convert_from_path(pfad, first_page=1, last_page=1)[0]
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return b64encode(buffer.getvalue()).decode("utf-8")
    except:
        return None