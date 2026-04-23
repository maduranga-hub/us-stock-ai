import os
import json
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

def add_headers():
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    
    if not gs_id or not gs_service_account:
        print("Missing credentials")
        return
        
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(gs_id).sheet1
        
        headers = ["Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
        
        # Insert headers at the first row
        sheet.insert_row(headers, 1)
        print("Successfully added headers to Google Sheet!")
    except Exception as e:
        print(f"Failed to add headers: {e}")

if __name__ == "__main__":
    add_headers()
