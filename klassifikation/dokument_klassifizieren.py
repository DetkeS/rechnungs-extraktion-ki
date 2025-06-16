import os
from dotenv import load_dotenv
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def gpt_klassifikation(text):
    prompt = (
        "Analysiere den folgenden Dokumenttext und gib 'rechnung' zurück, wenn es sich um eine Rechnung jeglicher Art handelt "
        "(z. B. auch Abschlagsrechnung, Teilrechnung etc.).\n"
        "Wenn es keine Rechnung ist, gib den konkreten Typ zurück wie z. B. Mahnung, Gutschrift, Angebot etc.\n\n"
        "Ziel: Nur ein Wort für den Dokumententyp, keine Sätze!\n\n"
        f"{text[:3000]}"
    )
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip().lower()
    except:
        return "unbekannt"