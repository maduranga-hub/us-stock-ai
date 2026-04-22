import requests
import os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TELEGRAM_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

msg = "✅ *US Stock AI: System Hardened!*\n\nThe scanner has been updated with v4.2 stability patches to ensure reliable alerts. 🚀"

url = f"https://api.telegram.org/bot{token}/sendMessage"
payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}

print(f"Testing connection to Telegram...")
try:
    response = requests.post(url, json=payload, timeout=10)
    result = response.json()
    if result.get("ok"):
        print("Success: Alert sent successfully.")
    else:
        print(f"Error from Telegram: {result}")
except Exception as e:
    print(f"Failed to connect: {e}")
