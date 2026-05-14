import yfinance as yf
import pandas as pd
import numpy as np
import pytz
import requests
import io
import os
import time
import concurrent.futures
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
import json
from google.oauth2.service_account import Credentials
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWS_CHANNEL_ID = os.getenv("NEWS_CHANNEL_ID") or "-1003889088299"
EARNINGS_CHANNEL_ID = os.getenv("EARNINGS_CHANNEL_ID") or "-1003737032970"
DASHBOARD_URL = os.getenv("DASHBOARD_URL") or "https://us-stock-ai-maduranga.streamlit.app"
DUBAI_TZ = pytz.timezone('Asia/Dubai')

SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Industrials": "XLI",
    "Communication Services": "XLC",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Basic Materials": "XLB"
}

def get_dubai_time():
    return datetime.now(DUBAI_TZ)

def send_telegram(message, channel="signal"):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    if channel == "signal":
        chat_id = TELEGRAM_CHAT_ID
    elif channel == "news":
        chat_id = NEWS_CHANNEL_ID
    elif channel == "earnings":
        chat_id = EARNINGS_CHANNEL_ID
    else:
        chat_id = TELEGRAM_CHAT_ID
        
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Telegram Error: {e}")

def get_gs_client():
    global google_sheet_error
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    if gs_id: gs_id = gs_id.strip()
    gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    if not gs_id:
        google_sheet_error = "GOOGLE_SHEET_ID is missing."
        return None, None
    if not gs_service_account:
        google_sheet_error = "GCP_SERVICE_ACCOUNT_KEY is missing."
        return None, None
    
    # Strip any accidental single or double quotes wrapped around the JSON
    gs_service_account = gs_service_account.strip().strip("'").strip('"')
    
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(gs_id)
        return client, spreadsheet
    except Exception as e: 
        google_sheet_error = f"Auth/JSON Error: {e}"
        return None, None

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
    send_telegram("🔄 *Refreshing Master Stock List...* (Filtering 6,700+ stocks by >$500M Cap)", channel="signal")
    
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
        send_telegram(msg, channel="signal")
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

global google_sheet_error
google_sheet_error = ""

def get_master_list():
    global google_sheet_error
    """Fetches the stock list from Google Sheets (Stock List tab) provided by the user."""
    client, spreadsheet = get_gs_client()
    if spreadsheet:
        try:
            sheet = spreadsheet.worksheet("Stock List")
            symbols = [s.strip() for s in sheet.col_values(1)[1:] if s.strip()] # Skip header
            if symbols: return symbols
        except Exception as e:
            google_sheet_error = f"Failed to read from Google Sheet: {e}"
            print(google_sheet_error)
            pass
    elif not google_sheet_error:
        google_sheet_error = "Spreadsheet object is None for unknown reason."
    return []

def log_to_google_sheet(data_row, mode="technical"):
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    
    if mode == "earnings":
        sheet_name = "Earnings Logs"
        headers = ["Date", "Symbol", "Price", "Earnings Date", "Market Cap", "RSI", "Note"]
    else:
        sheet_name = dubai_now.strftime('%B %Y')
        headers = ["S/N", "Date", "Time", "Symbol", "Price", "RSI", "VWAP Status", "FVG Status", "Golden Cross", "Volume", "Signal Status", "Price 7D", "Profit 7D %", "Price 30D", "Profit 30D %", "AI Analysis Note"]
    
    sheet = get_or_create_sheet(spreadsheet, sheet_name, headers)
    try:
        if mode == "technical":
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
        for i in range(len(all_rows) - 1, max(0, len(all_rows) - 500), -1):
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
            price_7d = row[11] if len(row) > 11 else ""
            if price_7d == "" and dubai_now > signal_time + timedelta(days=7):
                try:
                    time.sleep(1)
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=7)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=12)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        hist.columns = [c.lower() for c in hist.columns]
                        p7d = hist['close'].iloc[0]
                        sheet.update_cell(i + 1, 12, f"{p7d:.2f}")
                        sheet.update_cell(i + 1, 13, f"{((p7d - entry_p) / entry_p * 100):.2f}%")
                        updated_count += 1
                except Exception as e: 
                    print(f"Error 7D profit for {symbol}: {e}")
            
            price_30d = row[13] if len(row) > 13 else ""
            if price_30d == "" and dubai_now > signal_time + timedelta(days=30):
                try:
                    time.sleep(1)
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=(signal_time + timedelta(days=30)).strftime('%Y-%m-%d'), end=(signal_time + timedelta(days=35)).strftime('%Y-%m-%d'))
                    if not hist.empty:
                        hist.columns = [c.lower() for c in hist.columns]
                        p30d = hist['close'].iloc[0]
                        sheet.update_cell(i + 1, 14, f"{p30d:.2f}")
                        sheet.update_cell(i + 1, 15, f"{((p30d - entry_p) / entry_p * 100):.2f}%")
                        updated_count += 1
                except Exception as e:
                    print(f"Error 30D profit for {symbol}: {e}")
        return updated_count
    except: return 0

def update_signal_lifecycle():
    client, spreadsheet = get_gs_client()
    if not spreadsheet: return
    dubai_now = get_dubai_time()
    # Check current month, previous month, and the month before that
    for m in range(3):
        target_month = dubai_now - timedelta(days=30 * m)
        sheet_title = target_month.strftime('%B %Y')
        try:
            target_sheet = spreadsheet.worksheet(sheet_title)
            print(f"Updating lifecycle for: {sheet_title}")
            update_sheet_lifecycle(target_sheet)
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

def check_volume_profile_fvg(df, fvg, bins=50):
    if fvg is None: return False
    try:
        min_p = df['low'].min()
        max_p = df['high'].max()
        if min_p == max_p: return False
        
        price_bins = np.linspace(min_p, max_p, bins + 1)
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        df_vp = df.copy()
        df_vp['bin'] = pd.cut(tp, bins=price_bins, labels=False, include_lowest=True)
        vol_profile = df_vp.groupby('bin')['volume'].sum()
        
        if vol_profile.empty: return False
        
        mean_vol = vol_profile.mean()
        hvn_threshold = mean_vol * 1.5
        
        for bin_idx, vol in vol_profile.items():
            if vol >= hvn_threshold:
                bin_low = price_bins[int(bin_idx)]
                bin_high = price_bins[int(bin_idx) + 1]
                
                overlap = max(0, min(bin_high, fvg['top']) - max(bin_low, fvg['bottom']))
                if overlap > 0:
                    return True
        return False
    except:
        return False

def analyze_ticker(symbol, scan_type="technical", target_date=None):
    try:
        ticker = yf.Ticker(symbol)
        # Use fast_info for basic metrics to avoid slow .info call
        fast = ticker.fast_info
        price = fast.get('last_price', 0)
        if price == 0: # Fallback
            price = ticker.info.get('currentPrice', 0)
        
        if price < 5: return None
        
        base_res = {"symbol": symbol, "price": price}
        
        if scan_type == "earnings":
            # For earnings, we try multiple methods as Yahoo is often unstable
            e_dates = []
            try:
                cal = ticker.calendar
                if cal is not None and not cal.empty and 'Earnings Date' in cal.index:
                    e_dates = list(cal.loc['Earnings Date'].values)
            except: pass
            
            if not e_dates:
                try:
                    # Fallback to earnings_dates property (sometimes more stable)
                    edf = ticker.earnings_dates
                    if edf is not None and not edf.empty:
                        e_dates = list(edf.index)
                except: pass

            if e_dates:
                today = get_dubai_time().date()
                tomorrow = today + timedelta(days=1)
                found_date = None
                for ed in e_dates:
                    try:
                        # Convert to date object regardless of format
                        if hasattr(ed, 'date'): ed_date = ed.date()
                        elif isinstance(ed, str): ed_date = datetime.strptime(ed[:10], '%Y-%m-%d').date()
                        else: ed_date = pd.to_datetime(ed).date()
                        
                        if ed_date == today or ed_date == tomorrow:
                            found_date = ed_date
                            break
                    except: continue
                
                if found_date:
                    # Try to get name but don't fail if rate limited
                    name = symbol
                    try: name = ticker.info.get('longName', symbol)
                    except: pass
                    
                    base_res.update({
                        "type": "earnings",
                        "earnings_date": found_date.strftime('%Y-%m-%d'),
                        "mkt_cap": fast.get('market_cap', 0),
                        "name": name
                    })
                    return base_res
            return None

        if scan_type == "news":
            try:
                news_list = ticker.news
                if news_list and len(news_list) > 0:
                    latest = news_list[0]
                    info = ticker.info
                    base_res.update({
                        "type": "news",
                        "title": latest.get("title", ""),
                        "publisher": latest.get("publisher", ""),
                        "link": latest.get("link", ""),
                        "sector": info.get("sector", "N/A"),
                        "industry": info.get("industry", "N/A")
                    })
                    return base_res
            except: pass
            return None

        # Technical Scan Logic
        df_daily = ticker.history(period="150d", interval="1d")
        if df_daily.empty or len(df_daily) < 100: return None
        df_daily.columns = [c.lower() for c in df_daily.columns]
        
        # SMAs
        sma50 = df_daily['close'].rolling(window=50).mean().iloc[-1]
        sma100 = df_daily['close'].rolling(window=100).mean().iloc[-1]
        
        if price < sma100 or price < sma50: return None
        
        df = ticker.history(period="15d", interval="1h")
        if df.empty or len(df) < 50: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.columns = [c.lower() for c in df.columns]
        
        df['rsi'] = calculate_rsi(df['close']); rsi = df['rsi'].iloc[-1]
        fvg = detect_fvg(df)
        fvg_has_volume = check_volume_profile_fvg(df, fvg)
        
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tpv'] = df['tp'] * df['volume']; df['date'] = df.index.date
        df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
        
        vwap = df['vwap'].iloc[-1]; vwap_status = "Bullish (Above)" if price > vwap else "Bearish (Below)"
        avg_vol_20 = df['volume'].rolling(window=20).mean().iloc[-1]; high_volume = df['volume'].iloc[-1] >= (1.5 * avg_vol_20)
        
        is_signal = rsi < 35
        # High Conviction requires all conditions to be met, but still obeys the is_signal filter
        high_conviction = rsi < 35 and fvg is not None and fvg_has_volume and price > vwap and sma50 > sma100
        
        upcoming_events = []
        if is_signal:
            try:
                today = get_dubai_time().date()
                target_date = today + timedelta(days=14)
                
                # Check Earnings
                e_dates = []
                try:
                    cal = ticker.calendar
                    if cal is not None and not cal.empty and 'Earnings Date' in cal.index:
                        e_dates = list(cal.loc['Earnings Date'].values)
                except: pass
                
                if not e_dates:
                    try:
                        edf = ticker.earnings_dates
                        if edf is not None and not edf.empty:
                            e_dates = list(edf.index)
                    except: pass
                
                for ed in e_dates:
                    try:
                        if hasattr(ed, 'date'): ed_date = ed.date()
                        elif isinstance(ed, str): ed_date = datetime.strptime(ed[:10], '%Y-%m-%d').date()
                        else: ed_date = pd.to_datetime(ed).date()
                        
                        if today <= ed_date <= target_date:
                            upcoming_events.append(f"Earnings on {ed_date.strftime('%Y-%m-%d')}")
                            break # Just need the next one
                    except: continue

                # Check Dividends
                try:
                    cal = ticker.calendar
                    if cal is not None and not cal.empty and 'Ex-Dividend Date' in cal.index:
                        d_dates = list(cal.loc['Ex-Dividend Date'].values)
                        for dd in d_dates:
                            try:
                                if hasattr(dd, 'date'): d_date = dd.date()
                                elif isinstance(dd, str): d_date = datetime.strptime(dd[:10], '%Y-%m-%d').date()
                                else: d_date = pd.to_datetime(dd).date()
                                
                                if today <= d_date <= target_date:
                                    upcoming_events.append(f"Ex-Dividend on {d_date.strftime('%Y-%m-%d')}")
                                    break
                            except: continue
                except: pass

            except Exception as e:
                print(f"Error fetching events for {symbol}: {e}")

            # Sector Analysis
            sector_analysis = None
            try:
                sector_name = ticker.info.get('sector')
                etf_symbol = SECTOR_ETF_MAP.get(sector_name) if sector_name else None
                if etf_symbol:
                    etf_ticker = yf.Ticker(etf_symbol)
                    etf_hist = etf_ticker.history(period="60d")
                    spy_hist = yf.Ticker("SPY").history(period="60d")
                    if not etf_hist.empty and not spy_hist.empty and len(etf_hist) >= 30 and len(spy_hist) >= 30:
                        etf_close = etf_hist['Close'].iloc[-1]
                        etf_sma50 = etf_hist['Close'].rolling(window=50).mean().iloc[-1]
                        sector_uptrend = etf_close > etf_sma50
                        
                        etf_30d = etf_hist['Close'].iloc[-21]
                        etf_ret = ((etf_close - etf_30d) / etf_30d) * 100
                        
                        spy_close = spy_hist['Close'].iloc[-1]
                        spy_30d = spy_hist['Close'].iloc[-21]
                        spy_ret = ((spy_close - spy_30d) / spy_30d) * 100
                        
                        sector_analysis = {
                            "name": sector_name,
                            "etf": etf_symbol,
                            "uptrend": sector_uptrend,
                            "etf_ret": etf_ret,
                            "spy_ret": spy_ret,
                            "outperforming": etf_ret > spy_ret
                        }
            except Exception as e:
                print(f"Error checking sector for {symbol}: {e}")

        base_res.update({
            "type": "technical", 
            "name": ticker.info.get('longName', symbol),
            "rsi": rsi, "fvg": fvg, "fvg_has_volume": fvg_has_volume, "vwap_status": vwap_status, 
            "golden_cross": sma50 > sma100, "sma50_daily": sma50, "sma100_daily": sma100, 
            "timestamp": get_dubai_time().strftime('%Y-%m-%d %H:%M'), 
            "high_volume": high_volume, "is_signal": is_signal, "high_conviction": high_conviction,
            "upcoming_events": upcoming_events,
            "sector_analysis": sector_analysis if is_signal else None
        })
        return base_res
    except: return None

def load_sent_items(filename="sent_news.txt"):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_sent_item(item, filename="sent_news.txt"):
    with open(filename, "a") as f:
        f.write(f"{item}\n")

def get_google_news(keyword):
    try:
        query = urllib.parse.quote(f"{keyword} Finance Stock Market")
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req, timeout=10)
        xml_data = response.read()
        root = ET.fromstring(xml_data)
        
        for item in root.findall('.//item'):
            title = item.find('title').text if item.find('title') is not None else ""
            link = item.find('link').text if item.find('link') is not None else ""
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ""
            source = item.find('source').text if item.find('source') is not None else "Google News"
            
            if title and link:
                return {
                    "type": "sector_news",
                    "title": title,
                    "publisher": source,
                    "link": link,
                    "pubDate": pub_date
                }
    except Exception as e:
        print(f"Error fetching Google News for {keyword}: {e}")
    return None

def run_scanner(mode="technical", force_ticker=None):
    if mode == "refresh": refresh_stock_list(); return
    if force_ticker:
        universe = [force_ticker]
    else:
        universe = get_master_list()
        
    dubai_now = get_dubai_time()
    print(f"Starting {mode.upper()} Scan on {len(universe)} Stocks...")
    if not universe:
        global google_sheet_error
        print("No stocks found to scan. Please add stocks to the 'Stock List' Google Sheet.")
        send_telegram(f"⚠️ *Scan Aborted:* No stocks found in the Google Sheet 'Stock List'.\n*Error details:* {google_sheet_error}", channel="signal")
        return
    found_count = 0
    results_for_csv = []
    
    # Use fewer workers for earnings to avoid rate limiting on .calendar
    max_workers = 30 if mode == "earnings" else 50
    if mode == "news":
        sent_items = load_sent_items("sent_news.txt")
    elif mode == "earnings":
        sent_items = load_sent_items("sent_earnings.txt")
    else:
        sent_items = set()
    
    collected_sectors = set()
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    found_count += 1
                    if mode == "technical":
                        if force_ticker: res['is_signal'] = True
                        if not res.get('is_signal'): 
                            found_count -= 1
                            continue
                            
                        if res.get('fvg'):
                            fvg_s = f"✅ Bullish FVG Found (${res['fvg']['bottom']:.2f} - ${res['fvg']['top']:.2f})"
                            if res.get('fvg_has_volume'):
                                fvg_s += " 📊 (Backed by Volume Profile)"
                            else:
                                fvg_s += " ⚠️ (No Volume Support)"
                        else:
                            fvg_s = "❌ No FVG"
                        
                        analysis_note = (f"• *Trend:* Price > SMA 100 (${res['sma100_daily']:.2f}).\n• *RSI:* {res['rsi']:.2f}\n• *FVG:* {fvg_s}\n• *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}")
                        
                        events_text = "\n\n📅 *Upcoming Events (Next 14 Days):*\n"
                        if res.get('upcoming_events'):
                            events_text += "\n".join([f"• {e}" for e in res['upcoming_events']])
                        else:
                            events_text += "• ❌ No major events (Earnings/Dividends) scheduled"

                        sector_text = ""
                        sa = res.get('sector_analysis')
                        if sa:
                            uptrend_s = "✅ Uptrend (Price > SMA 50)" if sa['uptrend'] else "❌ Downtrend"
                            perf_s = "🔥 Outperforming SPY" if sa['outperforming'] else "⚠️ Underperforming SPY"
                            sector_text = f"\n\n🏢 *Sector Analysis ({sa['name']} - {sa['etf']}):*\n• Trend: {uptrend_s}\n• Strength: {perf_s} (Sector: {sa['etf_ret']:.1f}% | SPY: {sa['spy_ret']:.1f}%)"

                        msg = (f"{'🔥 HIGH CONVICTION' if res.get('high_conviction') else '🚀 NEW BUY SIGNAL'}: *{res['symbol']}*\n\n💰 *Price:* ${res['price']:.2f}\n📈 *SMA 50:* ${res['sma50_daily']:.2f}\n📉 *SMA 100:* ${res['sma100_daily']:.2f}\n📊 *RSI:* {res['rsi']:.2f}\n⚡ *VWAP:* {res['vwap_status']}\n🧬 *Golden Cross:* {'✅ ACTIVE' if res.get('golden_cross') else '❌ INACTIVE'}{sector_text}{events_text}\n\n📝 *AI Analysis Note:*\n{analysis_note}\n\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                        send_telegram(msg, channel="signal")
                        
                        gs_row = [dubai_now.strftime('%Y-%m-%d'), dubai_now.strftime('%H:%M'), res['symbol'], f"{res['price']:.2f}", f"{res['rsi']:.2f}", res['vwap_status'], f"FVG: {fvg_s}", "YES" if res.get('golden_cross') else "NO", "Normal" if not res.get('high_volume') else "🔥 High Spike", "ACTIVE", "", "", "", "", analysis_note.replace('\n', ' ')]
                        log_to_google_sheet(gs_row, mode="technical")
                        results_for_csv.append(res)
                    
                    elif mode == "earnings":
                        identifier = f"{res['symbol']}_{res['earnings_date']}"
                        if identifier not in sent_items:
                            msg = (f"📅 *UPCOMING EARNINGS:* *{res['symbol']}*\n\n🏢 *Company:* {res['name']}\n💰 *Price:* ${res['price']:.2f}\n📆 *Earnings Date:* {res['earnings_date']}\n💎 *Market Cap:* ${res['mkt_cap']/1e9:.2f}B\n\n🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                            send_telegram(msg, channel="earnings")
                            
                            gs_row = [dubai_now.strftime('%Y-%m-%d'), res['symbol'], f"{res['price']:.2f}", res['earnings_date'], f"${res['mkt_cap']/1e9:.2f}B", "N/A", "Upcoming Report"]
                            log_to_google_sheet(gs_row, mode="earnings")
                            results_for_csv.append(res)
                            
                            sent_items.add(identifier)
                            save_sent_item(identifier, "sent_earnings.txt")
                        else:
                            found_count -= 1

                    elif mode == "news":
                        link = res.get('link', '')
                        sector = res.get('sector', 'N/A')
                        if sector and sector != 'N/A':
                            collected_sectors.add(sector)
                            
                        if link and link not in sent_items:
                            msg = f"📰 *{res['symbol']} News*\n🏢 *Sector:* {sector}\n🏭 *Industry:* {res.get('industry', 'N/A')}\n\n*{res['title']}*\n_{res['publisher']}_\n\n🔗 [Read More]({link})"
                            send_telegram(msg, channel="news")
                            sent_items.add(link)
                            save_sent_item(link, "sent_news.txt")
                            results_for_csv.append(res)
                        else:
                            found_count -= 1

        # Fetch Sector News via Google News
        if mode == "news" and collected_sectors:
            print(f"Fetching Google News for sectors: {collected_sectors}")
            for sector in collected_sectors:
                s_news = get_google_news(f"{sector} Sector")
                if s_news:
                    link = s_news.get('link', '')
                    if link and link not in sent_items:
                        msg = f"🌐 *{sector} Sector News*\n\n*{s_news['title']}*\n_{s_news['publisher']}_\n\n🔗 [Read More]({link})"
                        send_telegram(msg, channel="news")
                        sent_items.add(link)
                        save_sent_item(link, "sent_news.txt")
                        s_news['sector'] = sector
                        s_news['symbol'] = f"{sector} Sector"
                        results_for_csv.append(s_news)
                        found_count += 1

        # Update local CSVs
        if results_for_csv:
            df = pd.DataFrame(results_for_csv)
            if mode == "technical": csv_name = "active_signals.csv"
            elif mode == "earnings": csv_name = "active_earnings_signals.csv"
            else: csv_name = "active_news.csv"
            
            # For news, we might append or just save latest, let's just save latest
            df.to_csv(csv_name, index=False)
            
        if mode == "technical": 
            update_signal_lifecycle()
    finally:
        dubai_time_str = dubai_now.strftime('%H:%M')
        print(f"Scan Completed: {dubai_time_str} GST")
        
        channel_type = mode if mode in ["news", "earnings"] else "signal"
        send_telegram(f"🔔 {mode.upper()} SCAN COMPLETED: {dubai_time_str} GST\n✅ Found: {found_count} {'Signals' if mode=='technical' else ('Reports' if mode=='earnings' else 'News Items')} from Master List ({len(universe)} stocks).", channel=channel_type)

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    ticker = sys.argv[2] if len(sys.argv) > 2 else None
    run_scanner(mode, ticker)
