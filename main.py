import yfinance as yf
import pandas as pd
import numpy as np
import pytz
import requests
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
    """Fetches a large list of US tickers from multiple exchanges."""
    print("🌐 Fetching Stock Universe...")
    try:
        # Using a reliable curated list from a public GitHub source
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        response = requests.get(url)
        tickers = [t.strip() for t in response.text.split("\n") if t.strip()]
        # Filtering out indices or non-stock tickers if necessary
        return tickers
    except Exception as e:
        print(f"⚠️ Error fetching universe: {e}")
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "NFLX"]

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_ticker(symbol, scan_type="technical"):
    """
    Analyzes a single ticker based on filtering criteria.
    Market Cap > $500M, Price > $5.
    """
    try:
        ticker = yf.Ticker(symbol)
        
        # Performance optimization: Fetch info and history in one go
        # Note: t.info is slow, but necessary for Market Cap
        info = ticker.info
        mkt_cap = info.get('marketCap', 0)
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))

        # 1. Fundamental Filter
        if mkt_cap < 500_000_000 or price < 5:
            return None

        if scan_type == "earnings":
            # Check Earnings for Tomorrow
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
                    return {
                        "type": "earnings",
                        "symbol": symbol,
                        "name": info.get('longName', 'N/A'),
                        "forecast_eps": info.get('earningsGrowth', 'N/A'),
                        "prev_eps": info.get('trailingEps', 'N/A'),
                        "price": price
                    }
            return None

        else:
            # 2. Technical Analysis Scan
            df = ticker.history(period="15d", interval="1h")
            if df.empty or len(df) < 100: return None
            
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            
            df['rsi'] = calculate_rsi(df['close'])
            df['sma100'] = df['close'].rolling(window=100).mean()
            
            rsi = df['rsi'].iloc[-1]
            sma100 = df['sma100'].iloc[-1]
            
            # Strategy: RSI <= 35 and Price > SMA 100
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
    # Limiting to top 2500 for safety in free-tier GitHub Actions
    universe = universe[:2500] 
    
    print(f"🚀 Starting {mode.upper()} Scan on {len(universe)} stocks...")
    
    results = []
    # Multi-threading to speed up the process
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        future_to_ticker = {executor.submit(analyze_ticker, s, mode): s for s in universe}
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            if res:
                results.append(res)
                
                # Immediate Telegram Alert
                if res['type'] == "earnings":
                    msg = (f"🔔 *EARNINGS TOMORROW: {res['symbol']}*\n\n"
                           f"🏢 Company: {res['name']}\n"
                           f"💰 Price: ${res['price']:.2f}\n"
                           f"📊 Forecast EPS: {res['forecast_eps']}\n"
                           f"📉 Trailing EPS: {res['prev_eps']}\n\n"
                           f"🕒 Dubai Time: {get_dubai_time().strftime('%H:%M')} GST")
                else:
                    msg = (f"🚀 *BUY SIGNAL DETECTED: {res['symbol']}*\n\n"
                           f"💰 Price: *${res['price']:.2f}*\n"
                           f"📉 RSI: *{res['rsi']:.2f}*\n"
                           f"📈 Status: *Above SMA 100*\n\n"
                           f"🔗 [Open Dashboard]({DASHBOARD_URL})")
                
                send_telegram(msg)

    # Save to CSV for Streamlit Dashboard
    if results:
        df_res = pd.DataFrame(results)
        df_res.to_csv(f"active_{mode}_signals.csv", index=False)
        print(f"✅ Found {len(results)} {mode} results.")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "technical"
    run_scanner(mode)
