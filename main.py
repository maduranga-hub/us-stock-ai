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

def format_market_cap(cap):
    if cap >= 1_000_000_000_000: return f"${cap/1_000_000_000_000:.2f}T"
    if cap >= 1_000_000_000: return f"${cap/1_000_000_000:.2f}B"
    if cap >= 1_000_000: return f"${cap/1_000_000:.2f}M"
    return f"${cap}"

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

def get_or_create_monthly_sheet(spreadsheet, dubai_now):
    sheet_name = dubai_now.strftime('%B %Y')
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        headers = ["S/N", "Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
        new_sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=str(len(headers)))
        new_sheet.append_row(headers)
        return new_sheet

def log_to_google_sheet(data_row):
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    sheet = get_or_create_monthly_sheet(spreadsheet, dubai_now)
    
    # Prepend S/N
    try:
        all_rows = sheet.get_all_values()
        sn = len(all_rows) # If headers exist, row 2 will be S/N 1
        data_row.insert(0, sn)
    except:
        data_row.insert(0, "-")

    print(f"Syncing {data_row[3]} to {sheet.title} (S/N: {data_row[0]})...")
    try:
        sheet.append_row(data_row)
        print(f"Google Sheets Sync Success: {data_row[3]}")
    except Exception as e:
        print(f"Google Sheets Sync Failed: {e}")

def update_sheet_lifecycle(sheet):
    try:
        all_rows = sheet.get_all_values()
        if len(all_rows) < 2: return 0
        dubai_now = get_dubai_time()
        updated_count = 0
        
        # New Indexes (0-based):
        # 0:S/N, 1:Date, 2:Time, 3:Symbol, 4:Price, 5:RSI, 6:VWAP, 7:FVG, 8:GC, 9:Volume, 10:Status, 11:P7D, 12:Pr7D, 13:P30D, 14:Pr30D, 15:Analysis
        
        for i in range(len(all_rows) - 1, max(0, len(all_rows) - 100), -1):
            row = all_rows[i]
            if len(row) < 11: continue
            symbol = row[3]
            entry_price = float(row[4])
            status = row[10]
            timestamp_str = f"{row[1]} {row[2]}"
            try:
                signal_time = DUBAI_TZ.localize(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M'))
            except: continue
            
            if status == "ACTIVE":
                if dubai_now > signal_time + timedelta(hours=4):
                    sheet.update_cell(i + 1, 11, "EXPIRED") # Col 11 is Status (1-based)
                    updated_count += 1
                else:
                    try:
                        ticker = yf.Ticker(symbol)
                        hist = ticker.history(period="1d", interval="1h")
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
                                sheet.update_cell(i + 1, 11, "DEACTIVATED")
                                updated_count += 1
                    except: pass
            
            if len(row) > 12 and row[11] == "" and dubai_now > signal_time + timedelta(days=7):
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=7)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=8)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p7d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 12, f"{p7d:.2f}")
                        sheet.update_cell(i + 1, 13, f"{((p7d - entry_price) / entry_price * 100):.2f}%")
                        updated_count += 1
                except: pass

            if len(row) > 14 and row[13] == "" and dubai_now > signal_time + timedelta(days=30):
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=30)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=31)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p30d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 14, f"{p30d:.2f}")
                        sheet.update_cell(i + 1, 15, f"{((p30d - entry_price) / entry_price * 100):.2f}%")
                        updated_count += 1
                except: pass
        return updated_count
    except Exception as e: print(f"Update error: {e}"); return 0

def update_signal_lifecycle():
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    curr_sheet = get_or_create_monthly_sheet(spreadsheet, dubai_now)
    updates = update_sheet_lifecycle(curr_sheet)
    prev_month_time = dubai_now.replace(day=1) - timedelta(days=1)
    prev_sheet_name = prev_month_time.strftime('%B %Y')
    try:
        prev_sheet = spreadsheet.worksheet(prev_sheet_name)
        updates += update_sheet_lifecycle(prev_sheet)
    except: pass
    if updates > 0: print(f"Total Updates: {updates}")

def get_market_universe():
    tickers = set()
    try:
        url_sp500 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df_sp500 = pd.read_csv(io.StringIO(requests.get(url_sp500).text))
        tickers.update(df_sp500['Symbol'].tolist())
        popular = ["TSLA", "NVDA", "AMD", "NFLX", "COIN", "PLTR", "SQ", "SHOP", "U", "RIVN", "GE", "UNH", "RTX", "ISRG", "DHR", "CB", "COF", "NOC", "MMM", "AMX", "ADC", "AERO", "AUB"]
        tickers.update(popular)
    except: tickers.update(["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GE", "UNH", "RTX"])
    return sorted(list(set([t.replace('.', '-') for t in tickers if t])))

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
        ticker = yf.Ticker(symbol)
        info = ticker.info
        mkt_cap = info.get('marketCap', 0); price = info.get('currentPrice', info.get('regularMarketPrice', 0))
        if mkt_cap < 500_000_000 or price < 5: return None
        base_res = {"symbol": symbol, "name": info.get('longName', 'N/A'), "price": price, "market_cap": mkt_cap, "market_cap_fmt": format_market_cap(mkt_cap)}
        if scan_type == "earnings":
            cal = ticker.calendar; e_date = None
            if isinstance(cal, pd.DataFrame) and not cal.empty:
                if 'Earnings Date' in cal.index: e_date = cal.loc['Earnings Date'].iloc[0]
            elif isinstance(cal, dict) and 'Earnings Date' in cal: e_date = cal['Earnings Date'][0]
            if e_date:
                e_date = e_date.date() if hasattr(e_date, 'date') else pd.to_datetime(e_date).date()
                if e_date == target_date:
                    base_res.update({"type": "earnings", "forecast_eps": info.get('forwardEps', 'N/A'), "last_year_eps": info.get('trailingEps', 'N/A')})
                    return base_res
            return None
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
    universe = [force_ticker] if force_ticker else get_market_universe()
    dubai_now = get_dubai_time(); target_date = (dubai_now + timedelta(days=1)).date()
    print(f"Starting {mode.upper()} Scan on {len(universe)} stocks...")
    found_count = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode, target_date): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    if mode == "technical":
                        if force_ticker: res['is_signal'] = True
                        if not res.get('is_signal'): continue
                    found_count += 1
                    if res['type'] == "earnings":
                        msg = (f"📅 *EARNINGS ALERT: {res['symbol']}*\n\n🏢 *Company:* {res['name']}\n📊 *Forecast:* {res['forecast_eps']}\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    else:
                        v_s = "Above" if "Above" in res['vwap_status'] else "Below"
                        fvg_s = f"✅ Bullish FVG Found (${res['fvg']['bottom']:.2f} - ${res['fvg']['top']:.2f})" if res.get('fvg') else "❌ No FVG"
                        analysis_note = (f"• *Trend Check:* Price is above Daily SMA 100 (${res['sma100_daily']:.2f}).\n• *SMA 50:* ${res['sma50_daily']:.2f}\n• *SMA 100:* ${res['sma100_daily']:.2f}\n• *Opportunity:* RSI is at {res['rsi']:.2f}.\n• *Momentum:* Price is {v_s} VWAP.\n• *FVG:* {fvg_s}\n• *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}")
                        msg = (f"{'🔥 HIGH CONVICTION' if res.get('high_conviction') else '🚀 NEW BUY SIGNAL'}: *{res['symbol']}*\n\n💰 *Price:* ${res['price']:.2f}\n📈 *SMA 50:* ${res['sma50_daily']:.2f}\n📉 *SMA 100:* ${res['sma100_daily']:.2f}\n📊 *RSI:* {res['rsi']:.2f}\n⚡ *VWAP:* {res['vwap_status']}\n🧬 *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}\n\n📝 *AI Analysis Note:*\n{analysis_note}\n\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    send_telegram(msg)
                    gs_row = [dubai_now.strftime('%Y-%m-%d'), dubai_now.strftime('%H:%M'), res['symbol'], f"{res['price']:.2f}", f"{res['rsi']:.2f}", res['vwap_status'], f"FVG: {fvg_s}", "YES" if res.get('golden_cross') else "NO", "Normal" if not res.get('high_volume') else "🔥 High Spike", "ACTIVE", "", "", "", "", analysis_note.replace('\n', ' ')]
                    log_to_google_sheet(gs_row)
        if mode == "technical": update_signal_lifecycle()
    finally:
        if mode == "technical": send_telegram(f"🔔 {mode.upper()} SCAN COMPLETED: {dubai_now.strftime('%H:%M')} GST\n✅ Found: {found_count}")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    ticker = sys.argv[2] if len(sys.argv) > 2 else None
    run_scanner(mode, ticker)
