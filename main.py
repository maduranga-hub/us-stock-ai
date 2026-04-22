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

# Dubai Timezone setup
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
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Telegram Error: {e}")

def log_to_google_sheet(data_row):
    """Logs a signal row to Google Sheets."""
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    
    if not gs_id or not gs_service_account:
        return 
    
    print(f"Syncing {data_row[1]} to Google Sheets...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(gs_id).sheet1
        sheet.append_row(data_row)
        print(f"✅ Google Sheets Sync: {data_row[1]}")
    except Exception as e:
        print(f"Google Sheets Sync Failed: {e}")

def get_market_universe():
    """Fetches high-quality S&P 500 and NASDAQ tickers."""
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
    """Detects Bullish Fair Value Gaps (FVG)."""
    # Look at the last 3 candles
    # Bullish FVG: Low of Candle 3 > High of Candle 1
    if len(df) < 3: return None
    
    c1_high = df['high'].iloc[-3]
    c3_low = df['low'].iloc[-1]
    
    if c3_low > c1_high:
        return {
            "top": c3_low,
            "bottom": c1_high,
            "gap_size": c3_low - c1_high
        }
    return None

def analyze_ticker(symbol, scan_type="technical", target_date=None):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        mkt_cap = info.get('marketCap', 0)
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))

        if mkt_cap < 500_000_000 or price < 5:
            return None

        base_res = {
            "symbol": symbol,
            "name": info.get('longName', 'N/A'),
            "price": price,
            "market_cap": mkt_cap,
            "market_cap_fmt": format_market_cap(mkt_cap)
        }

        if scan_type == "earnings":
            cal = ticker.calendar
            earnings_date = None
            if isinstance(cal, pd.DataFrame) and not cal.empty:
                if 'Earnings Date' in cal.index: earnings_date = cal.loc['Earnings Date'].iloc[0]
            elif isinstance(cal, dict) and 'Earnings Date' in cal:
                earnings_date = cal['Earnings Date'][0]

            if earnings_date:
                e_date = earnings_date.date() if hasattr(earnings_date, 'date') else pd.to_datetime(earnings_date).date()
                if e_date == target_date:
                    last_year_date = (pd.to_datetime(e_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
                    base_res.update({
                        "type": "earnings",
                        "forecast_eps": info.get('forwardEps', 'N/A'),
                        "last_year_eps": info.get('trailingEps', 'N/A'),
                        "last_year_date": last_year_date
                    })
                    return base_res
            return None

        else:
            # --- MTF: Daily Trend Check ---
            df_daily = ticker.history(period="200d", interval="1d")
            if df_daily.empty or len(df_daily) < 100: return None
            df_daily.columns = [c.lower() for c in df_daily.columns]
            
            sma50_daily = df_daily['close'].rolling(window=50).mean().iloc[-1]
            sma100_daily = df_daily['close'].rolling(window=100).mean().iloc[-1]
            
            golden_cross = sma50_daily > sma100_daily
            price_above_sma100 = price > sma100_daily
            
            # Primary Trend Filter: Price must be above Daily SMA 100
            if not price_above_sma100:
                return None

            # --- Hourly Analysis ---
            df = ticker.history(period="15d", interval="1h")
            if df.empty or len(df) < 50: return None
            
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            
            # Indicators
            df['rsi'] = calculate_rsi(df['close'])
            rsi = df['rsi'].iloc[-1]
            
            # FVG Detection
            fvg = detect_fvg(df)
            
            # VWAP
            df['tp'] = (df['high'] + df['low'] + df['close']) / 3
            df['tpv'] = df['tp'] * df['volume']
            df['date'] = df.index.date
            df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
            vwap = df['vwap'].iloc[-1]
            vwap_status = "Bullish (Above)" if price > vwap else "Bearish (Below)"
            
            # Volume
            avg_vol_20 = df['volume'].rolling(window=20).mean().iloc[-1]
            curr_vol = df['volume'].iloc[-1]
            high_volume = curr_vol >= (1.5 * avg_vol_20)
            
            # Signal Logic: RSI <= 35 (Oversold) OR FVG Presence
            # While keeping Price > SMA 100 (Daily)
            is_signal = rsi <= 35 or fvg is not None
            
            # High Conviction: RSI Oversold AND FVG AND Price > VWAP
            high_conviction = rsi <= 40 and fvg is not None and price > vwap

            base_res.update({
                "type": "technical",
                "rsi": rsi,
                "fvg": fvg,
                "vwap_status": vwap_status,
                "golden_cross": golden_cross,
                "trend_status": "📈 Trend: Price > SMA 100",
                "sma100_daily": sma100_daily,
                "timestamp": get_dubai_time().strftime('%Y-%m-%d %H:%M'),
                "high_volume": high_volume,
                "is_signal": is_signal,
                "high_conviction": high_conviction
            })
            return base_res
    except Exception as e:
        print(f"Error analyzing {symbol}: {e}")
    return None

def run_scanner(mode="technical"):
    universe = get_market_universe()
    dubai_now = get_dubai_time()
    today_weekday = dubai_now.weekday()

    if mode == "earnings":
        if today_weekday == 4: target_date = (dubai_now + timedelta(days=3)).date()
        elif today_weekday == 5: target_date = (dubai_now + timedelta(days=2)).date()
        elif today_weekday == 6: target_date = (dubai_now + timedelta(days=1)).date()
        else: target_date = (dubai_now + timedelta(days=1)).date()
    else:
        target_date = (dubai_now + timedelta(days=1)).date()

    print(f"Starting {mode.upper()} Scan (Target: {target_date}) on {len(universe)} stocks...")
    
    signals = []
    all_processed = []
    found_count = 0
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_ticker = {executor.submit(analyze_ticker, s, mode, target_date): s for s in universe}
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res:
                    all_processed.append(res)
                    if mode == "technical" and not res.get('is_signal'):
                        continue

                    signals.append(res)
                    found_count += 1
                    
                    if res['type'] == "earnings":
                        msg = (f"📅 *EARNINGS ALERT: {res['symbol']}*\n\n"
                               f"🏢 *Company:* {res['name']}\n"
                               f"💰 *Market Cap:* {res['market_cap_fmt']}\n"
                               f"📊 *EPS Forecast:* {res['forecast_eps']}\n"
                               f"📉 *Last Year's EPS:* {res['last_year_eps']}\n\n"
                               f"🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    else:
                        v_status = "Above" if "Above" in res['vwap_status'] else "Below"
                        vol_status = "🔥 High Spike" if res.get('high_volume') else "Normal"
                        gc_status = "✅ ACTIVE" if res.get('golden_cross') else "❌ INACTIVE"
                        fvg_status = f"✅ Bullish FVG Found (${res['fvg']['bottom']:.2f} - ${res['fvg']['top']:.2f})" if res.get('fvg') else "❌ No FVG"
                        
                        header_icon = "🔥 HIGH CONVICTION SIGNAL" if res.get('high_conviction') else "🚀 NEW BUY SIGNAL"
                        
                        analysis_note = (f"• *Trend:* Price > Daily SMA 100 ({res['sma100_daily']:.2f}).\n"
                                         f"• *Momentum:* RSI is {res['rsi']:.2f}.\n"
                                         f"• *FVG:* {fvg_status}\n"
                                         f"• *VWAP:* Price is {v_status} VWAP.\n"
                                         f"• *Golden Cross:* {gc_status}")
                        
                        msg = (f"{header_icon}: *{res['symbol']}*\n\n"
                               f"💰 *Price:* ${res['price']:.2f}\n"
                               f"📉 *RSI:* {res['rsi']:.2f}\n"
                               f"📊 *Volume:* {vol_status}\n"
                               f"⚡ *VWAP:* {res['vwap_status']}\n"
                               f"🕳️ *FVG:* {fvg_status}\n"
                               f"🧬 *Golden Cross:* {gc_status}\n\n"
                               f"📝 *AI Analysis Note:*\n"
                               f"{analysis_note}\n\n"
                               f"🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                    
                    send_telegram(msg)
                    
                    gs_row = [
                        dubai_now.strftime('%Y-%m-%d %H:%M'),
                        res['symbol'],
                        f"{res['price']:.2f}",
                        f"{res['rsi']:.2f}",
                        res['vwap_status'],
                        f"FVG: {fvg_status}",
                        vol_status,
                        analysis_note.replace('\n', ' ')
                    ]
                    log_to_google_sheet(gs_row)

        if mode == "earnings" and found_count == 0:
            msg = (f"🔔 EARNINGS REPORT: {target_date}\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"No major earnings reports are scheduled for {target_date}.\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"Status: System Active")
            send_telegram(msg)

        if all_processed:
            if mode == "technical":
                pd.DataFrame(signals).to_csv("active_signals.csv", index=False)
                pd.DataFrame(all_processed).to_csv("market_overview_technical.csv", index=False)
                print(f"Technical signals saved (Dubai Time: {dubai_now.strftime('%H:%M')})")
            else:
                pd.DataFrame(all_processed).to_csv("active_earnings_signals.csv", index=False)
                pd.DataFrame(all_processed).to_csv("active_signals.csv", index=False)
                print(f"Earnings signals saved (Dubai Time: {dubai_now.strftime('%H:%M')})")
    
    finally:
        if mode == "technical":
            icon = "🎯" if found_count > 0 else "ℹ️"
            summary = f"New Signals Found: {found_count}" if found_count > 0 else "No signals matched criteria."
            status_msg = (f"🔔 {mode.upper()} SCAN COMPLETED: {dubai_now.strftime('%H:%M')} GST\n"
                          f"━━━━━━━━━━━━━━━━━━━━\n"
                          f"✅ Total Stocks Analyzed: {len(universe)}\n"
                          f"{icon} {summary}\n"
                          f"━━━━━━━━━━━━━━━━━━━━\n"
                          f"Status: System Active")
            send_telegram(status_msg)

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    run_scanner(mode)
