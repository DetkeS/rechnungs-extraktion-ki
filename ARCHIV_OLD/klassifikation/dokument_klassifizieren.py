import os
from dotenv import load_dotenv
import openai
import fitz  # PyMuPDF

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def gpt_klassifikation(text_path=None, image_b64=None):
    if image_b64:
        prompt = (
            "Du erhältst ein Bild eines Dokuments.\n"
            "Bitte klassifiziere den Dokumenttyp als einen der folgenden Begriffe:\n"
            "- rechnung\n- mahnung\n- anschreiben\n- email\n- gutschrift\n"
            "- zahlungserinnerung\n- behördlich\n- sonstiges\n\n"
            "Antworte ausschließlich mit einem dieser Begriffe."
        )
        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}}
            ]
        }]
    elif text_path:
        try:
            doc = fitz.open(text_path)
            extracted_text = "\n".join([page.get_text() for page in doc])
        except Exception as e:
            print(f"❌ Fehler beim Öffnen oder Lesen von PDF ({text_path}): {e}")
            return "unlesbar"

        prompt = (
            "Analysiere den folgenden Text und gib exakt einen Dokumenttyp zurück:\n"
            "- rechnung\n- mahnung\n- anschreiben\n- email\n- gutschrift\n"
            "- zahlungserinnerung\n- behördlich\n- sonstiges\n\n"
            "Wichtig: Wenn unklar, schätze.\n\n"
            f"{extracted_text[:3000]}"
        )
        messages = [{"role": "user", "content": prompt}]
    else:
        raise ValueError("Entweder text_path oder image_b64 muss übergeben werden.")

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        print(f"Fehler bei Klassifikation durch GPT: {e}")
        return "unbekannt"
