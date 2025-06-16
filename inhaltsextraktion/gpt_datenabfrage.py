import os
from dotenv import load_dotenv
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def gpt_abfrage_inhalt(text=None, b64_image=None):
    prompt = (
        "Extrahiere alle Artikelpositionen aus dieser Rechnung in folgender CSV-Struktur:\n"
        "Artikelbezeichnung;Menge;Einheit;Einzelpreis;Gesamtpreis;Lieferant;Rechnungsdatum;Rechnungsempfänger\n"
        "Gib ausschließlich die Tabelle als CSV mit Semikolon-Trennung zurück.\n"
        "WICHTIG: Gib KEINE fiktiven Daten an. Wenn keine Daten enthalten sind, gib eine leere Tabelle zurück."
    )
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    if b64_image:
        messages[0]["content"].append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_image}"}})
    elif text:
        messages[0]["content"].append({"type": "text", "text": text[:4000]})
    try:
        response = openai.chat.completions.create(
            model="gpt-4o", messages=messages, temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except:
        return "FEHLER"