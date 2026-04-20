import yfinance as yf
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8501")

WATCHLIST = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "NFLX"]

def get_dubai_time():
    dubai_tz = pytz.timezone('Asia/Dubai')
    return datetime.now(dubai_tz)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def check_upcoming_earnings():
    """Checks for earnings reports in the next 3 days."""
    print("📅 Scanning Earnings Calendar for Watchlist...")
    alerts = []
    dubai_now = get_dubai_time()
    three_days_out = dubai_now + timedelta(days=3)
    
    for symbol in WATCHLIST:
        try:
            ticker = yf.Ticker(symbol)
            cal = ticker.calendar
            
            # yfinance calendar structure can vary; we check common fields
            if cal is not None:
                # Calendar usually returns a DataFrame or Dictionary
                # We look for 'Earnings Date' or 'Earnings Date' in the index
                earnings_date = None
                if isinstance(cal, pd.DataFrame) and not cal.empty:
                    if 'Earnings Date' in cal.index:
                        earnings_date = cal.loc['Earnings Date'].iloc[0]
                elif isinstance(cal, dict) and 'Earnings Date' in cal:
                    earnings_date = cal['Earnings Date'][0]

                if earnings_date:
                    # Convert to date for comparison
                    if hasattr(earnings_date, 'date'):
                        e_date = earnings_date.date()
                    else:
                        e_date = pd.to_datetime(earnings_date).date()
                        
                    if dubai_now.date() <= e_date <= three_days_out.date():
                        alerts.append(f"🔔 *{symbol}* Earnings Report on {e_date.strftime('%b %d')}")
        except Exception as e:
            print(f"⚠️ Error checking earnings for {symbol}: {e}")
            continue
            
    if alerts:
        msg = "🗓️ *UPCOMING EARNINGS ALERT (3-Day Window)*\n\n" + "\n".join(alerts)
        msg += f"\n\n🔗 [Open Dashboard]({DASHBOARD_URL})"
        send_telegram(msg)
        print("✅ Earnings alerts sent to Telegram.")
    else:
        print("ℹ️ No upcoming earnings in the next 3 days.")

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Telegram Alert Error: {e}")

def scan_markets():
    dubai_now = get_dubai_time()
    print(f"🔍 Scan Started: {dubai_now.strftime('%H:%M')} GST")
    
    # 🕒 Daily Earnings Check (Runs at 3:30 PM Dubai Time scan)
    if dubai_now.hour == 15 and dubai_now.minute < 35:
        check_upcoming_earnings()

    for symbol in WATCHLIST:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="10d", interval="1h")
            
            if df.empty:
                continue
                
            # Flatten MultiIndex if necessary
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            
            # Indicators
            df['rsi'] = calculate_rsi(df['close'])
            df['sma100'] = df['close'].rolling(window=100).mean()
            
            current_price = df['close'].iloc[-1]
            current_rsi = df['rsi'].iloc[-1]
            current_sma100 = df['sma100'].iloc[-1]
            
            # 🎯 SIGNAL LOGIC: RSI <= 35 AND Price > SMA 100
            if not pd.isna(current_rsi) and current_rsi <= 35:
                if not pd.isna(current_sma100) and current_price > current_sma100:
                    
                    ny_tz = pytz.timezone('America/New_York')
                    ny_now = datetime.now(ny_tz)
                    market_status = "LIVE" if 9 <= ny_now.hour < 16 else "PRE/POST"
                    
                    msg = (
                        f"🚀 *BUY SIGNAL DETECTED: {symbol}*\n\n"
                        f"💰 Current Price: *${current_price:.2f}*\n"
                        f"📉 RSI (14): *{current_rsi:.2f}* (Oversold)\n"
                        f"📈 Trend: *Above SMA 100* (Bullish)\n\n"
                        f"📅 Date: {dubai_now.strftime('%Y-%m-%d')} (Dubai)\n"
                        f"🕒 Time: {dubai_now.strftime('%H:%M')} (Dubai)\n"
                        f"📡 Market: *{market_status}*\n\n"
                        f"🔗 [Open Dashboard]({DASHBOARD_URL})"
                    )
                    send_telegram(msg)
                    print(f"✅ Signal sent for {symbol}")
            
        except Exception as e:
            print(f"⚠️ Error scanning {symbol}: {e}")

if __name__ == "__main__":
    scan_markets()
