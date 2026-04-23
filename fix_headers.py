import os
import json
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

def fix_monthly_headers():
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(gs_id)
        
        # Target the specific monthly sheet
        sheet_name = "April 2026"
        sheet = spreadsheet.worksheet(sheet_name)
        
        # Corrected Header List with S/N at the start
        headers = ["S/N", "Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
        
        # Update the first row (1-based index)
        sheet.update('A1', [headers])
        print(f"Successfully fixed headers in '{sheet_name}' sheet!")
        
    except Exception as e:
        print(f"Failed to fix headers: {e}")

if __name__ == "__main__":
    fix_monthly_headers()
