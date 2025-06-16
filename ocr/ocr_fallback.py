import os
from dotenv import load_dotenv
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def gpt_abfrage_ocr_text(b64_image):
    prompt = "Extrahiere den lesbaren Textinhalt dieses Geschäftsdokuments zur weiteren Analyse."
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
    except:
        return ""