import os
from dotenv import load_dotenv
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def gpt_abfrage_ocr_text(b64_image):
    prompt = (
        "Analysiere das folgende Bild einer Rechnung.\n"
        "Auch wenn die Darstellung undeutlich ist, versuche so viel strukturierten Text wie möglich zu extrahieren.\n"
        "Ignoriere Layout-Fehler, Trennzeichen oder Formatierung – gib nur den vermutlichen reinen Rechnungstext wieder."
    )

    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_image}"}}
        ]
    }]

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Fehler bei GPT-OCR: {e}")
        return ""
