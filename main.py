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

def get_market_universe():
    """Fetches a high-coverage ticker list including major exchanges."""
    print("🌐 Fetching Stock Universe...")
    tickers = set()
    try:
        # Source 1: Large curated list
        url1 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        r1 = requests.get(url1)
        tickers.update([t.strip() for t in r1.text.split("\n") if t.strip()])
        
        # Source 2: Specifically ensure S&P 500 for high-cap coverage
        url2 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        r2 = requests.get(url2)
        sp500_df = pd.read_csv(io.StringIO(r2.text))
        tickers.update(sp500_df['Symbol'].tolist())
        
    except Exception as e:
        print(f"⚠️ Error fetching universe: {e}")
        # Fallback to a core list if all else fails
        tickers.update(["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GE", "UNH", "RTX", "ISRG", "DHR", "CB", "COF", "NOC", "MMM"])
    
    return sorted(list(tickers))

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_ticker(symbol, scan_type="technical"):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        mkt_cap = info.get('marketCap', 0)
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))

        # Basic filtering to reduce noise
        if mkt_cap < 500_000_000 or price < 5:
            return None

        if scan_type == "earnings":
            cal = ticker.calendar
            tomorrow_date = (get_dubai_time() + timedelta(days=1)).date()
            
            earnings_date = None
            if isinstance(cal, pd.DataFrame) and not cal.empty:
                if 'Earnings Date' in cal.index: earnings_date = cal.loc['Earnings Date'].iloc[0]
            elif isinstance(cal, dict) and 'Earnings Date' in cal:
                earnings_date = cal['Earnings Date'][0]

            if earnings_date:
                e_date = earnings_date.date() if hasattr(earnings_date, 'date') else pd.to_datetime(earnings_date).date()
                if e_date == tomorrow_date:
                    last_year_date = (pd.to_datetime(e_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
                    
                    return {
                        "type": "earnings",
                        "symbol": symbol,
                        "name": info.get('longName', 'N/A'),
                        "forecast_eps": info.get('forwardEps', 'N/A'),
                        "last_year_eps": info.get('trailingEps', 'N/A'),
                        "last_year_date": last_year_date,
                        "market_cap": format_market_cap(mkt_cap)
                    }
            return None

        else:
            # Technical Scan Logic
            df = ticker.history(period="15d", interval="1h")
            if df.empty or len(df) < 100: return None
            
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            
            df['rsi'] = calculate_rsi(df['close'])
            rsi = df['rsi'].iloc[-1]
            sma100 = df['close'].rolling(window=100).mean().iloc[-1]
            
            if rsi <= 35 and price > sma100:
                return {
                    "type": "technical",
                    "symbol": symbol,
                    "price": price,
                    "rsi": rsi,
                    "name": info.get('longName', 'N/A')
                }
    except:
        pass
    return None

def run_scanner(mode="technical"):
    universe = get_market_universe()
    # Increase coverage to 6000 stocks for full market scan
    universe = universe[:6000] 
    
    print(f"🚀 Starting {mode.upper()} Scan on {len(universe)} stocks...")
    
    results = []
    found_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_ticker = {executor.submit(analyze_ticker, s, mode): s for s in universe}
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            if res:
                results.append(res)
                found_count += 1
                
                if res['type'] == "earnings":
                    msg = (f"🔔 EARNINGS TOMORROW: {res['symbol']}\n\n"
                           f"🏢 Company: {res['name']}\n"
                           f"💰 Market Cap: {res['market_cap']}\n"
                           f"📊 EPS Forecast: {res['forecast_eps']}\n"
                           f"📉 Last Year's EPS: {res['last_year_eps']}\n"
                           f"📅 Last Year's Date: {res['last_year_date']}\n"
                           f"🕒 Dubai Time: {get_dubai_time().strftime('%H:%M')} GST")
                else:
                    msg = (f"🚀 BUY SIGNAL: {res['symbol']}\n\n"
                           f"💰 Price: ${res['price']:.2f}\n"
                           f"📉 RSI: {res['rsi']:.2f}\n"
                           f"📈 Trend: Bullish\n\n"
                           f"🔗 [Dashboard]({DASHBOARD_URL})")
                
                send_telegram(msg)

    if mode == "earnings" and found_count == 0:
        tomorrow = (get_dubai_time() + timedelta(days=1)).strftime('%Y-%m-%d')
        msg = (f"🔔 EARNINGS REPORT: {tomorrow}\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"No major earnings reports are scheduled for tomorrow for stocks above $500M Market Cap.\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Status: System Active")
        send_telegram(msg)

    if results:
        pd.DataFrame(results).to_csv(f"active_{mode}_signals.csv", index=False)

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    run_scanner(mode)
