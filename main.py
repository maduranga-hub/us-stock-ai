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
        print(f"❌ Telegram Error: {e}")

def log_to_google_sheet(data_row):
    """Logs a signal row to Google Sheets."""
    gs_id = os.getenv("GOOGLE_SHEET_ID")
    gs_service_account = os.getenv("GSPREAD_SERVICE_ACCOUNT")
    
    if not gs_id or not gs_service_account:
        return # Skip if not configured
        
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        service_account_info = json.loads(gs_service_account)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(gs_id).sheet1
        sheet.append_row(data_row)
        print(f"✅ Google Sheets Sync: {data_row[1]}")
    except Exception as e:
        print(f"⚠️ Google Sheets Sync Failed: {e}")

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
                # Use target_date from weekend-aware logic
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
            df_daily = ticker.history(period="150d", interval="1d")
            if df_daily.empty or len(df_daily) < 100: return None
            df_daily.columns = [c.lower() for c in df_daily.columns]
            sma100_daily = df_daily['close'].rolling(window=100).mean().iloc[-1]
            if price <= sma100_daily: return None # Price must be above Daily SMA 100

            # --- Hourly Analysis ---
            df = ticker.history(period="15d", interval="1h")
            if df.empty or len(df) < 50: return None
            
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            
            # VWAP Calculation (Session-based)
            df['tp'] = (df['high'] + df['low'] + df['close']) / 3
            df['tpv'] = df['tp'] * df['volume']
            df['date'] = df.index.date
            df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
            vwap = df['vwap'].iloc[-1]
            vwap_status = "Bullish (Above)" if price > vwap else "Bearish (Below)"
            
            # Volume Filter: 1.5x of 20-period average
            avg_vol_20 = df['volume'].rolling(window=20).mean().iloc[-1]
            curr_vol = df['volume'].iloc[-1]
            high_volume = curr_vol >= (1.5 * avg_vol_20)
            
            # MACD Calculation (12, 26, 9)
            ema12 = df['close'].ewm(span=12, adjust=False).mean()
            ema26 = df['close'].ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            
            # MACD Bullish Crossover in last 3 candles
            macd_bullish = False
            for i in range(-3, 0):
                try:
                    if (macd_line.iloc[i] > signal_line.iloc[i]) and (macd_line.iloc[i-1] <= signal_line.iloc[i-1]):
                        macd_bullish = True
                        break
                except: pass
            
            df['rsi'] = calculate_rsi(df['close'])
            rsi = df['rsi'].iloc[-1]
            sma100 = df['close'].rolling(window=100).mean().iloc[-1]
            
            # High Conviction Signal: RSI <= 35 and Price > SMA 100 (Hourly)
            sma100_h = df['close'].rolling(window=100).mean().iloc[-1]
            is_signal = rsi <= 35 and price > sma100_h

            # Check for earnings near target_date
            cal = ticker.calendar
            earnings_near = False
            try:
                if isinstance(cal, pd.DataFrame) and not cal.empty:
                    if 'Earnings Date' in cal.index:
                        e_date = cal.loc['Earnings Date'].iloc[0]
                        if hasattr(e_date, 'date'): e_date = e_date.date()
                        elif not isinstance(e_date, datetime): e_date = pd.to_datetime(e_date).date()
                        if e_date == target_date: earnings_near = True
                elif isinstance(cal, dict) and 'Earnings Date' in cal:
                    e_date = cal['Earnings Date'][0]
                    if hasattr(e_date, 'date'): e_date = e_date.date()
                    elif not isinstance(e_date, datetime): e_date = pd.to_datetime(e_date).date()
                    if e_date == target_date: earnings_near = True
            except: pass

            base_res.update({
                "type": "technical",
                "rsi": rsi,
                "vwap_status": vwap_status,
                "earnings_near": earnings_near,
                "high_volume": high_volume,
                "macd_bullish": macd_bullish,
                "is_signal": is_signal,
                "high_conviction": is_signal and high_volume
            })
            return base_res
    except:
        pass
    return None

def run_scanner(mode="technical"):
    universe = get_market_universe()
    dubai_now = get_dubai_time()
    today_weekday = dubai_now.weekday() # 0=Mon, 4=Fri, 5=Sat, 6=Sun

    # --- WEEKEND-AWARE LOGIC ---
    if mode == "earnings":
        if today_weekday == 4: # Friday
            target_date = (dubai_now + timedelta(days=3)).date()
            header = "📅 MONDAY'S EARNINGS PREVIEW (Weekend Special)"
        elif today_weekday == 5: # Saturday
            target_date = (dubai_now + timedelta(days=2)).date()
            header = "📅 MONDAY'S EARNINGS PREVIEW"
        elif today_weekday == 6: # Sunday
            target_date = (dubai_now + timedelta(days=1)).date()
            header = "📅 MONDAY'S EARNINGS PREVIEW"
        else:
            target_date = (dubai_now + timedelta(days=1)).date()
            header = "📅 TOMORROW'S EARNINGS PREVIEW"
    else:
        target_date = (dubai_now + timedelta(days=1)).date() # Default tomorrow
        header = "🚀 BUY SIGNAL"

    print(f"🚀 Starting {mode.upper()} Scan (Target: {target_date}) on {len(universe)} stocks...")
    
    signals = []
    all_processed = []
    found_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_ticker = {executor.submit(analyze_ticker, s, mode, target_date): s for s in universe}
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            if res:
                all_processed.append(res)
                if mode == "technical" and not res.get('is_signal'):
                    continue # Heatmap uses all, but alerts/signals CSV only use signals

                signals.append(res)
                found_count += 1
                
                if res['type'] == "earnings":
                    msg = (f"📅 *EARNINGS ALERT: {res['symbol']}*\n\n"
                           f"🏢 *Company:* {res['name']}\n"
                           f"💰 *Market Cap:* {res['market_cap_fmt']}\n"
                           f"📊 *EPS Forecast:* {res['forecast_eps']}\n"
                           f"📉 *Last Year's EPS:* {res['last_year_eps']}\n\n"
                           f"📝 *AI Analysis Note:*\n"
                           f"• *Opportunity:* Forecasted EPS of {res['forecast_eps']} vs {res['last_year_eps']} last year.\n"
                           f"• *Risk Context:* High volatility event. Professional risk management required for pre-earnings positions.\n"
                           f"• *Market Sentiment:* Tracking institutional positioning ahead of {target_date}.\n\n"
                           f"🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                else:
                    v_status = "Above" if "Above" in res['vwap_status'] else "Below"
                    m_status = "Bullish" if "Above" in res['vwap_status'] else "Bearish"
                    vol_status = "🔥 High Spike" if res.get('high_volume') else "Normal"
                    macd_status = "⚡ Bullish Crossover" if res.get('macd_bullish') else "Neutral"
                    e_risk = "⚠️ Earnings report scheduled for tomorrow. High event-based volatility risk." if res.get('earnings_near') else "✅ No earnings reported for tomorrow, reducing event-based volatility risk."
                    
                    header_icon = "🔥 HIGH CONVICTION SIGNAL" if res.get('high_conviction') else "🚀 NEW BUY SIGNAL"
                    
                    analysis_note = (f"• *Trend Check:* Price is above SMA 100 on both Daily & Hourly charts (Long-term Bullish).\n"
                                     f"• *Opportunity:* RSI is at {res['rsi']:.2f} on 1H timeframe. Momentum is shifting from oversold levels.\n"
                                     f"• *Volume/Momentum:* Price is {v_status} VWAP with {m_status} momentum.\n"
                                     f"• *Risk Context:* {e_risk}")
                    
                    msg = (f"{header_icon}: *{res['symbol']}*\n\n"
                           f"💰 *Price:* ${res['price']:.2f}\n"
                           f"📉 *RSI:* {res['rsi']:.2f}\n"
                           f"📊 *Volume:* {vol_status}\n"
                           f"📈 *MACD:* {macd_status}\n"
                           f"⚡ *VWAP:* {res['vwap_status']}\n\n"
                           f"📝 *AI Analysis Note:*\n"
                           f"{analysis_note}\n\n"
                           f"🔗 [Open Quant Terminal]({DASHBOARD_URL})")
                
                send_telegram(msg)
                
                # --- Google Sheets Sync ---
                gs_row = [
                    dubai_now.strftime('%Y-%m-%d %H:%M'),
                    res['symbol'],
                    f"{res['price']:.2f}",
                    f"{res['rsi']:.2f}",
                    res['vwap_status'],
                    macd_status,
                    vol_status,
                    analysis_note.replace('\n', ' ')
                ]
                log_to_google_sheet(gs_row)

    # Fallback for Earnings
    if mode == "earnings" and found_count == 0:
        msg = (f"🔔 EARNINGS REPORT: {target_date}\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"No major earnings reports are scheduled for {target_date} for stocks above $500M Market Cap.\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Status: System Active")
        send_telegram(msg)

    # Save results to CSV (for Dashboard)
    if all_processed:
        if mode == "technical":
            pd.DataFrame(signals).to_csv("active_signals.csv", index=False)
            pd.DataFrame(all_processed).to_csv("market_overview_technical.csv", index=False)
            print(f"✅ Technical signals and market overview saved (Dubai Time: {dubai_now.strftime('%H:%M')})")
        else:
            # For earnings, save to specific file AND active_signals.csv as requested
            pd.DataFrame(all_processed).to_csv("active_earnings_signals.csv", index=False)
            pd.DataFrame(all_processed).to_csv("active_signals.csv", index=False)
            print(f"✅ Earnings signals and sync feed saved (Dubai Time: {dubai_now.strftime('%H:%M')})")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    run_scanner(mode)
