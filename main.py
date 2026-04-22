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
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Telegram Error: {e}")

def get_gs_client():
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    if not gs_id or not gs_service_account: return None, None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(gs_id)
        return client, spreadsheet
    except: return None, None

def get_or_create_sheet(spreadsheet, title, headers=None):
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        new_sheet = spreadsheet.add_worksheet(title=title, rows="5000", cols="20")
        if headers: new_sheet.append_row(headers)
        return new_sheet

def parse_mkt_cap(cap_str):
    """Converts market cap string like '$1.23B' or '$500.5M' to a number."""
    try:
        cap_str = str(cap_str).replace('$', '').replace(',', '').strip()
        if 'T' in cap_str: return float(cap_str.replace('T', '')) * 1_000_000_000_000
        if 'B' in cap_str: return float(cap_str.replace('B', '')) * 1_000_000_000
        if 'M' in cap_str: return float(cap_str.replace('M', '')) * 1_000_000
        return float(cap_str)
    except: return 0

def refresh_stock_list():
    """Scans all 6700+ US stocks and filters for > $500M Market Cap using a robust method."""
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    
    print("Refreshing Master Stock List from full market data...")
    send_telegram("🔄 *Refreshing Master Stock List...* (Filtering 6,700+ stocks by >$500M Cap)")
    
    # Get the huge list that we verified is working
    resp = requests.get("https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt")
    universe = sorted(list(set([t.strip().replace('.', '-') for t in resp.text.splitlines() if t.strip()])))
    
    qualified = []
    
    def check_cap_robust(symbol):
        try:
            # Using a simplified check to avoid crumb errors
            ticker = yf.Ticker(symbol)
            cap = ticker.fast_info.get('market_cap', 0)
            if cap >= 500_000_000:
                name = ticker.info.get('longName', 'N/A')
                return [symbol, name, f"${cap/1_000_000_000:.2f}B"]
        except: pass
        return None

    # Using 100 workers for speed
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(check_cap_robust, s): s for s in universe}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: qualified.append(res)

    if qualified:
        sheet = get_or_create_sheet(spreadsheet, "Stock List", ["Symbol", "Company Name", "Market Cap"])
        sheet.clear()
        sheet.append_row(["Symbol", "Company Name", "Market Cap"])
        sheet.append_rows(sorted(qualified))
        msg = f"✅ *Master Stock List Updated!*\nFound {len(qualified)} stocks with Market Cap > $500M across all US exchanges."
        send_telegram(msg)
        print("Master Stock List Updated Successfully.")

def get_market_universe():
    """Fetches the entire US market universe (~6700+ tickers) for scanning."""
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

def log_to_google_sheet(data_row):
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    sheet_name = dubai_now.strftime('%B %Y')
    headers = ["S/N", "Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
    sheet = get_or_create_sheet(spreadsheet, sheet_name, headers)
    try:
        all_rows = sheet.get_all_values()
        data_row.insert(0, len(all_rows))
        sheet.append_row(data_row)
    except Exception as e: print(f"Sync error: {e}")

def update_sheet_lifecycle(sheet):
    try:
        all_rows = sheet.get_all_values()
        if len(all_rows) < 2: return 0
        dubai_now = get_dubai_time()
        updated_count = 0
        for i in range(len(all_rows) - 1, max(0, len(all_rows) - 100), -1):
            row = all_rows[i]
            if len(row) < 11: continue
            symbol, entry_p, status = row[3], float(row[4]), row[10]
            timestamp_str = f"{row[1]} {row[2]}"
            try: signal_time = DUBAI_TZ.localize(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M'))
            except: continue
            if status == "ACTIVE":
                if dubai_now > signal_time + timedelta(hours=4):
                    sheet.update_cell(i + 1, 11, "EXPIRED"); updated_count += 1
                else:
                    try:
                        ticker = yf.Ticker(symbol); hist = ticker.history(period="1d", interval="1h")
                        if not hist.empty:
                            hist.columns = [c.lower() for c in hist.columns]
                            curr_p = hist['close'].iloc[-1]
                            hist_d = ticker.history(period="150d", interval="1d")
                            hist_d.columns = [c.lower() for c in hist_d.columns]
                            sma100 = hist_d['close'].rolling(window=100).mean().iloc[-1]
                            delta = hist['close'].diff()
                            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean().iloc[-1]
                            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean().iloc[-1]
                            rsi = 100 - (100 / (1 + (gain/loss))) if loss != 0 else 50
                            if curr_p < sma100 or rsi > 55:
                                sheet.update_cell(i + 1, 11, "DEACTIVATED"); updated_count += 1
                    except: pass
            if len(row) > 12 and row[11] == "" and dubai_now > signal_time + timedelta(days=7):
                try:
                    ticker = yf.Ticker(symbol); hist = ticker.history(start=(signal_time + timedelta(days=7)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=8)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p7d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 12, f"{p7d:.2f}"); sheet.update_cell(i + 1, 13, f"{((p7d - entry_p) / entry_p * 100):.2f}%")
                        updated_count += 1
                except: pass
            if len(row) > 14 and row[13] == "" and dubai_now > signal_time + timedelta(days=30):
                try:
                    ticker = yf.Ticker(symbol); hist = ticker.history(start=(signal_time + timedelta(days=30)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=31)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p30d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 14, f"{p30d:.2f}"); sheet.update_cell(i + 1, 15, f"{((p30d - entry_p) / entry_p * 100):.2f}%")
                        updated_count += 1
                except: pass
        return updated_count
    except: return 0

def update_signal_lifecycle():
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    curr_sheet = get_or_create_sheet(spreadsheet, dubai_now.strftime('%B %Y'))
    updates = update_sheet_lifecycle(curr_sheet)
    prev_month_time = dubai_now.replace(day=1) - timedelta(days=1)
    try:
        prev_sheet = spreadsheet.worksheet(prev_month_time.strftime('%B %Y'))
        updates += update_sheet_lifecycle(prev_sheet)
    except: pass

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def detect_fvg(df):
    if len(df) < 3: return None
    c1_h, c3_l = df['high'].iloc[-3], df['low'].iloc[-1]
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
        df['tpv'] = df['tp'] * df['volume']; df['date'] = df.index.date
        df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
        vwap = df['vwap'].iloc[-1]; vwap_status = "Bullish (Above)" if price > vwap else "Bearish (Below)"
        avg_vol_20 = df['volume'].rolling(window=20).mean().iloc[-1]; high_volume = df['volume'].iloc[-1] >= (1.5 * avg_vol_20)
        is_signal = rsi <= 35 or fvg is not None; high_conviction = rsi <= 40 and fvg is not None and price > vwap and sma50 > sma100
        base_res.update({"type": "technical", "rsi": rsi, "fvg": fvg, "vwap_status": vwap_status, "golden_cross": sma50 > sma100, "sma50_daily": sma50, "sma100_daily": sma100, "timestamp": get_dubai_time().strftime('%Y-%m-%d %H:%M'), "high_volume": high_volume, "is_signal": is_signal, "high_conviction": high_conviction})
        return base_res
    except: return None

def run_scanner(mode="technical", force_ticker=None):
    if mode == "refresh": refresh_stock_list(); return
    universe = [force_ticker] if force_ticker else get_market_universe()
    dubai_now = get_dubai_time()
    print(f"Starting {mode.upper()} Scan on {len(universe)} Master Stocks...")
    found_count = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    if mode == "technical":
                        if force_ticker: res['is_signal'] = True
                        if not res.get('is_signal'): continue
                    found_count += 1
                    fvg_s = f"✅ Bullish FVG Found (${res['fvg']['bottom']:.2f} - ${res['fvg']['top']:.2f})" if res.get('fvg') else "❌ No FVG"
                    analysis_note = (f"• *Trend:* Price > SMA 100 (${res['sma100_daily']:.2f}).\n• *RSI:* {res['rsi']:.2f}\n• *FVG:* {fvg_s}\n• *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}")
                    msg = (f"{'🔥 HIGH CONVICTION' if res.get('high_conviction') else '🚀 NEW BUY SIGNAL'}: *{res['symbol']}*\n\n💰 *Price:* ${res['price']:.2f}\n📈 *SMA 50:* ${res['sma50_daily']:.2f}\n📉 *SMA 100:* ${res['sma100_daily']:.2f}\n📊 *RSI:* {res['rsi']:.2f}\n⚡ *VWAP:* {res['vwap_status']}\n🧬 *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}\n\n📝 *AI Analysis Note:*\n{analysis_note}\n\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    send_telegram(msg)
                    gs_row = [dubai_now.strftime('%Y-%m-%d'), dubai_now.strftime('%H:%M'), res['symbol'], f"{res['price']:.2f}", f"{res['rsi']:.2f}", res['vwap_status'], f"FVG: {fvg_s}", "YES" if res.get('golden_cross') else "NO", "Normal" if not res.get('high_volume') else "🔥 High Spike", "ACTIVE", "", "", "", "", analysis_note.replace('\n', ' ')]
                    log_to_google_sheet(gs_row)
        if mode == "technical": update_signal_lifecycle()
    finally:
        dubai_time_str = dubai_now.strftime('%H:%M')
        print(f"Scan Completed: {dubai_time_str} GST")
        send_telegram(f"🔔 SCAN COMPLETED: {dubai_time_str} GST\n✅ Found: {found_count} Signals from Master List ({len(universe)} stocks).")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    ticker = sys.argv[2] if len(sys.argv) > 2 else None
    run_scanner(mode, ticker)
