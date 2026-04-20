import requests
import os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TELEGRAM_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

msg = "✅ *US Stock AI: Connection Successful!*\n\nඔබගේ Telegram Bot එක දැන් සාර්ථකව පද්ධතිය සමඟ සම්බන්ධ වී ඇත. දැන් ඔබට market signals ලැබීම ආරම්භ වනු ඇත. 🚀"

url = f"https://api.telegram.org/bot{token}/sendMessage"
payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}

response = requests.post(url, json=payload)
print(response.json())
