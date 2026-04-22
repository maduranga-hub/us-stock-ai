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
        headers = ["Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
        new_sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=str(len(headers)))
        new_sheet.append_row(headers)
        return new_sheet

def log_to_google_sheet(data_row):
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    sheet = get_or_create_monthly_sheet(spreadsheet, dubai_now)
    print(f"Syncing {data_row[2]} to {sheet.title}...")
    try:
        sheet.append_row(data_row)
        print(f"Google Sheets Sync Success: {data_row[2]}")
    except Exception as e:
        print(f"Google Sheets Sync Failed: {e}")

def update_sheet_lifecycle(sheet):
    """Processes lifecycle updates for a specific sheet."""
    try:
        all_rows = sheet.get_all_values()
        if len(all_rows) < 2: return 0
        dubai_now = get_dubai_time()
        updated_count = 0
        
        # Check last 100 rows for signals to update
        for i in range(len(all_rows) - 1, max(0, len(all_rows) - 100), -1):
            row = all_rows[i]
            if len(row) < 10: continue
            symbol = row[2]
            entry_price = float(row[3])
            status = row[9]
            timestamp_str = f"{row[0]} {row[1]}"
            try:
                signal_time = DUBAI_TZ.localize(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M'))
            except: continue
            
            # Lifecycle Updates
            if status == "ACTIVE":
                if dubai_now > signal_time + timedelta(hours=4):
                    sheet.update_cell(i + 1, 10, "EXPIRED")
                    updated_count += 1
                else:
                    try:
                        ticker = yf.Ticker(symbol)
                        hist = ticker.history(period="1d", interval="1h")
                        if not hist.empty:
                            hist.columns = [c.lower() for c in hist.columns]
                            curr_price = hist['close'].iloc[-1]
                            hist_d = ticker.history(period="150d", interval="1d")
                            hist_d.columns = [c.lower() for c in hist_d.columns]
                            sma100_d = hist_d['close'].rolling(window=100).mean().iloc[-1]
                            delta = hist['close'].diff()
                            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean().iloc[-1]
                            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean().iloc[-1]
                            rsi = 100 - (100 / (1 + (gain/loss))) if loss != 0 else 50
                            if curr_price < sma100_d or rsi > 55:
                                sheet.update_cell(i + 1, 10, "DEACTIVATED")
                                updated_count += 1
                    except: pass
            
            # Performance Updates (7D/30D)
            if len(row) > 11 and row[10] == "" and dubai_now > signal_time + timedelta(days=7):
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=7)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=8)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p7d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 11, f"{p7d:.2f}")
                        sheet.update_cell(i + 1, 12, f"{((p7d - entry_price) / entry_price * 100):.2f}%")
                        updated_count += 1
                except: pass

            if len(row) > 13 and row[12] == "" and dubai_now > signal_time + timedelta(days=30):
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=30)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=31)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        p30d = hist['Close'].iloc[0]
                        sheet.update_cell(i + 1, 13, f"{p30d:.2f}")
                        sheet.update_cell(i + 1, 14, f"{((p30d - entry_price) / entry_price * 100):.2f}%")
                        updated_count += 1
                except: pass
        return updated_count
    except: return 0

def update_signal_lifecycle():
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    
    print("Updating Signal Lifecycles across monthly sheets...")
    # Update current month
    curr_sheet = get_or_create_monthly_sheet(spreadsheet, dubai_now)
    updates = update_sheet_lifecycle(curr_sheet)
    
    # Update previous month (to catch transitions near month boundaries)
    prev_month_time = dubai_now.replace(day=1) - timedelta(days=1)
    prev_sheet_name = prev_month_time.strftime('%B %Y')
    try:
        prev_sheet = spreadsheet.worksheet(prev_sheet_name)
        updates += update_sheet_lifecycle(prev_sheet)
    except: pass
    
    if updates > 0: print(f"Total Updates made: {updates}")

def get_market_universe():
    tickers = set()
    try:
        url_sp500 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df_sp500 = pd.read_csv(io.StringIO(requests.get(url_sp500).text))
        tickers.update(df_sp500['Symbol'].tolist())
        popular = ["TSLA", "NVDA", "AMD", "NFLX", "COIN", "PLTR", "SQ", "SHOP", "U", "RIVN", "GE", "UNH", "RTX", "ISRG", "DHR", "CB", "COF", "NOC", "MMM", "AMX", "ADC", "AERO", "AUB"]
        tickers.update(popular)
    except:
        tickers.update(["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GE", "UNH", "RTX"])
    return sorted(list(set([t.replace('.', '-') for t in tickers if t])))

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def detect_fvg(df):
    if len(df) < 3: return None
    c1_high = df['high'].iloc[-3]
    c3_low = df['low'].iloc[-1]
    if c3_low > c1_high: return {"top": c3_low, "bottom": c1_high, "gap_size": c3_low - c1_high}
    return None

def analyze_ticker(symbol, scan_type="technical", target_date=None):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        mkt_cap = info.get('marketCap', 0)
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))
        if mkt_cap < 500_000_000 or price < 5: return None
        base_res = {"symbol": symbol, "name": info.get('longName', 'N/A'), "price": price, "market_cap": mkt_cap, "market_cap_fmt": format_market_cap(mkt_cap)}
        if scan_type == "earnings":
            cal = ticker.calendar
            e_date = None
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
        sma50_d = df_daily['close'].rolling(window=50).mean().iloc[-1]
        sma100_d = df_daily['close'].rolling(window=100).mean().iloc[-1]
        golden_cross = sma50_d > sma100_d
        if price < sma100_d: return None
        df = ticker.history(period="15d", interval="1h")
        if df.empty or len(df) < 50: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.columns = [c.lower() for c in df.columns]
        df['rsi'] = calculate_rsi(df['close'])
        rsi = df['rsi'].iloc[-1]
        fvg = detect_fvg(df)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tpv'] = df['tp'] * df['volume']
        df['date'] = df.index.date
        df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
        vwap = df['vwap'].iloc[-1]
        vwap_status = "Bullish (Above)" if price > vwap else "Bearish (Below)"
        avg_vol_20 = df['volume'].rolling(window=20).mean().iloc[-1]
        high_volume = df['volume'].iloc[-1] >= (1.5 * avg_vol_20)
        is_signal = rsi <= 35 or fvg is not None
        high_conviction = rsi <= 40 and fvg is not None and price > vwap and golden_cross
        base_res.update({
            "type": "technical", "rsi": rsi, "fvg": fvg, "vwap_status": vwap_status,
            "golden_cross": golden_cross, "sma50_daily": sma50_d, "sma100_daily": sma100_d,
            "timestamp": get_dubai_time().strftime('%Y-%m-%d %H:%M'),
            "high_volume": high_volume, "is_signal": is_signal, "high_conviction": high_conviction
        })
        return base_res
    except Exception as e: print(f"Error analyzing {symbol}: {e}"); return None

def run_scanner(mode="technical", force_ticker=None):
    universe = [force_ticker] if force_ticker else get_market_universe()
    dubai_now = get_dubai_time()
    target_date = (dubai_now + timedelta(days=1)).date()
    print(f"Starting {mode.upper()} Scan on {len(universe)} stocks...")
    signals = []; found_count = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode, target_date): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    if mode == "technical":
                        if force_ticker: res['is_signal'] = True
                        if not res.get('is_signal'): continue
                    signals.append(res); found_count += 1
                    if res['type'] == "earnings":
                        msg = (f"📅 *EARNINGS ALERT: {res['symbol']}*\n\n🏢 *Company:* {res['name']}\n📊 *Forecast:* {res['forecast_eps']}\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    else:
                        v_status = "Above" if "Above" in res['vwap_status'] else "Below"
                        fvg_status = f"✅ Bullish FVG Found (${res['fvg']['bottom']:.2f} - ${res['fvg']['top']:.2f})" if res.get('fvg') else "❌ No FVG"
                        header_icon = "🔥 HIGH CONVICTION SIGNAL" if res.get('high_conviction') else "🚀 NEW BUY SIGNAL"
                        analysis_note = (f"• *Trend Check:* Price is above Daily SMA 100 (${res['sma100_daily']:.2f}).\n• *SMA 50:* ${res['sma50_daily']:.2f}\n• *SMA 100:* ${res['sma100_daily']:.2f}\n• *Opportunity:* RSI is at {res['rsi']:.2f}.\n• *Momentum:* Price is {v_status} VWAP.\n• *FVG:* {fvg_status}\n• *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}")
                        msg = (f"{header_icon}: *{res['symbol']}*\n\n💰 *Price:* ${res['price']:.2f}\n📈 *SMA 50:* ${res['sma50_daily']:.2f}\n📉 *SMA 100:* ${res['sma100_daily']:.2f}\n📊 *RSI:* {res['rsi']:.2f}\n⚡ *VWAP:* {res['vwap_status']}\n🧬 *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}\n\n📝 *AI Analysis Note:*\n{analysis_note}\n\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    send_telegram(msg)
                    gs_row = [dubai_now.strftime('%Y-%m-%d'), dubai_now.strftime('%H:%M'), res['symbol'], f"{res['price']:.2f}", f"{res['rsi']:.2f}", res['vwap_status'], f"FVG: {fvg_status}", "YES" if res.get('golden_cross') else "NO", "Normal" if not res.get('high_volume') else "🔥 High Spike", "ACTIVE", "", "", "", "", analysis_note.replace('\n', ' ')]
                    log_to_google_sheet(gs_row)
        if mode == "technical": update_signal_lifecycle()
    finally:
        if mode == "technical":
            status_msg = (f"🔔 {mode.upper()} SCAN COMPLETED: {dubai_now.strftime('%H:%M')} GST\n✅ Total Analyzed: {len(universe)} | Found: {found_count}")
            send_telegram(status_msg)

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    ticker = sys.argv[2] if len(sys.argv) > 2 else None
    run_scanner(mode, ticker)
