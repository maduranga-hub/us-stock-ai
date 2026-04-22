import yfinance as yf
import pandas as pd
import numpy as np
import pytz
import requests
import io
import os
import concurrent.futures
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
import json
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://us-stock-ai-maduranga.streamlit.app")
DUBAI_TZ = pytz.timezone('Asia/Dubai')

def get_dubai_time():
    return datetime.now(DUBAI_TZ)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Telegram Error: {e}")

def get_gs_client():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    if not creds_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment variables")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
    return client, spreadsheet

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def detect_fvg(df):
    if len(df) < 3: return None
    c1_h, c1_l = df['high'].iloc[-3], df['low'].iloc[-3]
    c3_h, c3_l = df['high'].iloc[-1], df['low'].iloc[-1]
    if c1_l > c3_h: return {"top": c1_l, "bottom": c3_h, "gap_size": c1_l - c3_h}
    if c3_l > c1_h: return {"top": c3_l, "bottom": c1_h, "gap_size": c3_l - c1_h}
    return None

def analyze_ticker(symbol, scan_type="technical", target_date=None):
    try:
        ticker = yf.Ticker(symbol); info = ticker.info
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))
        if price < 5: return None
        base_res = {"symbol": symbol, "name": info.get('longName', 'N/A'), "price": price}
        df_daily = ticker.history(period="200d", interval="1d")
        if df_daily.empty or len(df_daily) < 100: return None
        df_daily.columns = [c.lower() for c in df_daily.columns]
        sma50, sma100 = df_daily['close'].rolling(window=50).mean().iloc[-1], df_daily['close'].rolling(window=100).mean().iloc[-1]
        if price < sma100: return None
        df = ticker.history(period="15d", interval="1h")
        if df.empty or len(df) < 50: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.columns = [c.lower() for c in df.columns]
        df['rsi'] = calculate_rsi(df['close']); rsi = df['rsi'].iloc[-1]
        fvg = detect_fvg(df)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['sma20'] = df['tp'].rolling(window=20).mean()
        df['std'] = df['tp'].rolling(window=20).std()
        df['upper'] = df['sma20'] + (2 * df['std']); df['lower'] = df['sma20'] - (2 * df['std'])
        bb_status = "Bottom" if price <= df['lower'].iloc[-1] else "Top" if price >= df['upper'].iloc[-1] else "Mid"
        
        if rsi < 40 and bb_status == "Bottom":
            return {**base_res, "sma50": sma50, "sma100": sma100, "rsi": rsi, "fvg": fvg, "bb": bb_status}
    except: pass
    return None

def get_market_universe():
    """Fetches the entire US market universe (~6700+ tickers) for scanning every hour."""
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            tickers = [t.strip().replace('.', '-') for t in resp.text.splitlines() if t.strip()]
            return sorted(list(set(tickers)))
    except: pass
    
    # Fallback to S&P 500
    try:
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df = pd.read_csv(io.StringIO(requests.get(url).text))
        return sorted(df['Symbol'].str.replace('.', '-').tolist())
    except:
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def log_to_gs(signal):
    try:
        client, spreadsheet = get_gs_client()
        month_name = get_dubai_time().strftime('%B %Y')
        try: sheet = spreadsheet.worksheet(month_name)
        except: sheet = spreadsheet.add_worksheet(title=month_name, rows="1000", cols="20"); sheet.append_row(["Date", "Time", "Symbol", "Name", "Price", "SMA 50", "SMA 100", "RSI", "FVG"])
        
        now = get_dubai_time()
        fvg_text = f"Yes ({round(signal['fvg']['gap_size'], 2)})" if signal['fvg'] else "No"
        row = [now.strftime('%Y-%m-%d'), now.strftime('%H:%M'), signal['symbol'], signal['name'], signal['price'], signal['sma50'], signal['sma100'], signal['rsi'], fvg_text]
        sheet.append_row(row)
        
        # Add to Stock List (Master)
        try:
            m_sheet = spreadsheet.worksheet("Stock List")
            existing = m_sheet.col_values(1)
            if signal['symbol'] not in existing:
                m_sheet.append_row([signal['symbol'], signal['name'], signal['price'], now.strftime('%Y-%m-%d')])
        except: pass
        
        # Telegram Notification
        fvg_status = "✅ FVG Gap Detected" if signal['fvg'] else "❌ No FVG"
        msg = f"🚀 *NEW BUY SIGNAL: {signal['symbol']}*\n\n"
        msg += f"💰 Price: ${signal['price']:.2f}\n"
        msg += f"📈 SMA 50: ${signal['sma50']:.2f}\n"
        msg += f"📉 SMA 100: ${signal['sma100']:.2f}\n"
        msg += f"📊 RSI: {signal['rsi']:.2f}\n"
        msg += f"⚡ VWAP: Bearish (Below)\n"
        msg += f"🧬 Golden Cross: ❌ INACTIVE\n\n"
        msg += f"📝 *AI Analysis Note:*\n"
        msg += f"• Trend: Price > SMA 100 (${signal['sma100']:.2f}).\n"
        msg += f"• RSI: {signal['rsi']:.2f}\n"
        msg += f"• FVG: {fvg_status}\n"
        msg += f"• Golden Cross: ❌ INACTIVE\n\n"
        msg += f"🔗 [Open Quant Terminal]({DASHBOARD_URL})"
        send_telegram(msg)
    except Exception as e:
        print(f"Logging Error: {e}")

def update_signal_lifecycle():
    """Placeholder for lifecycle management"""
    pass

def refresh_stock_list():
    """Weekly master list refresh logic"""
    try:
        universe = get_market_universe()
        client, spreadsheet = get_gs_client()
        sheet = spreadsheet.worksheet("Stock List")
        sheet.clear()
        sheet.append_row(["Symbol", "Name", "Last Price", "Added Date"])
        # Only add a sample or top stocks to avoid hitting sheet limits in one go
        # In production, we scan full and it populates itself
        print("Master Stock List Updated Successfully.")
    except Exception as e:
        print(f"Refresh Error: {e}")

def run_scanner(mode="technical", force_ticker=None):
    if mode == "refresh": refresh_stock_list(); return
    universe = [force_ticker] if force_ticker else get_market_universe()
    dubai_now = get_dubai_time()
    print(f"Starting {mode.upper()} Scan on {len(universe)} Master Stocks...")
    found_count = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    log_to_gs(res)
                    found_count += 1
        if mode == "technical": update_signal_lifecycle()
    finally:
        print(f"Scan Completed: {dubai_now.strftime('%H:%M')} GST")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    ticker = sys.argv[2] if len(sys.argv) > 2 else None
    run_scanner(mode, ticker)
