import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import gspread
import json
import os
from google.oauth2.service_account import Credentials

# Page Configuration
st.set_page_config(page_title="US Stock AI | Quant Terminal", layout="wide")

# Custom CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Inter:wght@400;700&display=swap');
    .stApp { background: radial-gradient(circle at top right, #0d001a 0%, #050505 100%); color: #ffffff; font-family: 'Inter', sans-serif; }
    .cyber-header { font-family: 'Orbitron'; color: #00f2ff; text-shadow: 0 0 15px #00f2ff; text-align: center; letter-spacing: 5px; margin-bottom: 30px; }
    .glass-card { background: rgba(255, 255, 255, 0.03); backdrop-filter: blur(15px); border: 1px solid rgba(0, 242, 255, 0.1); border-radius: 20px; padding: 25px; margin-bottom: 20px; }
    .stSidebar { background-color: rgba(0,0,0,0.5) !important; border-right: 1px solid rgba(0,242,255,0.1); }
    /* Aggressive Visibility Fixes */
    .stSidebar [data-testid="stWidgetLabel"] p, .stSidebar label p { color: #ffffff !important; opacity: 1 !important; }
    
    /* Tabs: Force White and Cyan */
    button[data-baseweb="tab"] { color: #ffffff !important; }
    button[data-baseweb="tab"] div p { color: #ffffff !important; font-family: 'Orbitron' !important; }
    button[aria-selected="true"] { border-bottom-color: #00f2ff !important; }
    button[aria-selected="true"] div p { color: #00f2ff !important; text-shadow: 0 0 10px rgba(0, 242, 255, 0.5) !important; }
    
    /* Alerts: Force Bright Cyan */
    div[data-testid="stNotification"] { background-color: rgba(0, 242, 255, 0.1) !important; border: 1px solid #00f2ff !important; }
    div[data-testid="stNotification"] * { color: #00f2ff !important; font-weight: bold !important; }
    
    /* Status Colors */
    .status-active { color: #00ff00 !important; font-weight: bold; }
    .status-expired { color: #ff9900 !important; font-weight: bold; }
    .status-invalid { color: #ff3300 !important; font-weight: bold; text-transform: uppercase; }
    </style>
""", unsafe_allow_html=True)

# Sidebar Navigation
with st.sidebar:
    st.markdown('<h2 style="color: #00f2ff; font-family: \'Orbitron\'; text-align: center;">MENU</h2>', unsafe_allow_html=True)
    page = st.radio("Go to:", ["📊 QUANT DASHBOARD", "📖 SYSTEM DOCUMENTATION"])
    st.markdown("---")
    st.markdown('<p style="text-align: center; color: #00f2ff; opacity: 0.8; font-size: 0.8rem; font-family: \'Orbitron\'; letter-spacing: 2px;">v4.1 PRO RELEASE</p>', unsafe_allow_html=True)

def get_signal_status(row):
    try:
        now = datetime.now(pytz.timezone('Asia/Dubai'))
        sig_time = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=pytz.timezone('Asia/Dubai'))
        
        # Check Expiry (4 Hours)
        if (now - sig_time) > timedelta(hours=4):
            return "🕒 EXPIRED"
        
        # Check Invalidation (Price < Daily SMA 100)
        # For efficiency, we use the price from the scan, but in a real scenario we'd fetch live
        # To meet "Immediately", we will simulate a live check if possible or use the last known price
        if row['price'] < row['sma100_daily']:
            return "⚠️ INVALID: TREND BREAK"
            
        return "✅ ACTIVE"
    except:
        return "UNKNOWN"

if page == "📊 QUANT DASHBOARD":
    st.markdown('<h1 class="cyber-header">US STOCK AI : QUANT TERMINAL</h1>', unsafe_allow_html=True)
    tab_signals, tab_overview, tab_earnings, tab_heatmap, tab_history = st.tabs(["🚀 BUY SIGNALS", "🌍 MARKET WATCH", "📅 EARNINGS CENTER", "📊 RSI HEATMAP", "📈 PERFORMANCE"])

    with tab_signals:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        try:
            df_sig = pd.read_csv("active_signals.csv")
            if not df_sig.empty:
                df_sig['STATUS'] = df_sig.apply(get_signal_status, axis=1)
                
                # Reorder to show status first
                cols = ['STATUS', 'symbol', 'price', 'rsi', 'vwap_status', 'timestamp']
                display_df = df_sig[[c for c in cols if c in df_sig.columns]]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No active signals.")
        except Exception as e: 
            st.info(f"Scanning... ({e})")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_overview:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        try:
            df_ov = pd.read_csv("market_overview_technical.csv")
            st.dataframe(df_ov.sort_values(by='market_cap', ascending=False), use_container_width=True, hide_index=True)
        except: st.info("Loading...")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_earnings:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        try:
            df_earn = pd.read_csv("active_earnings_signals.csv")
            st.dataframe(df_earn, use_container_width=True, hide_index=True)
        except: st.info("No reports.")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_heatmap:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        try:
            df_h = pd.read_csv("market_overview_technical.csv")
            fig = px.treemap(df_h, path=['symbol'], values='market_cap', color='rsi', color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig, use_container_width=True)
        except: st.info("Generating...")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_history:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<h3 style="color: #00f2ff; font-family: \'Orbitron\';">7-DAY HISTORICAL PERFORMANCE</h3>', unsafe_allow_html=True)
        
        gs_id = os.getenv("GOOGLE_SHEET_ID")
        gs_service_account = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
        
        if not gs_id or not gs_service_account:
            st.warning("⚠️ Google Sheets credentials not found. Please set GOOGLE_SHEET_ID and GCP_SERVICE_ACCOUNT_KEY in Streamlit Secrets.")
        else:
            try:
                # Fetch Data from Google Sheets
                scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                service_account_info = json.loads(gs_service_account)
                creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
                client = gspread.authorize(creds)
                sheet = client.open_by_key(gs_id).sheet1
                data = sheet.get_all_values()
                
                if len(data) > 1:
                    df_hist = pd.DataFrame(data[1:], columns=data[0])
                    df_hist['Timestamp'] = pd.to_datetime(df_hist.iloc[:, 0])
                    
                    # Filter for last 7 days
                    seven_days_ago = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist['Timestamp'] >= seven_days_ago]
                    
                    if not df_hist.empty:
                        # Performance Calculation
                        with st.spinner("Calculating Success Rate..."):
                            success_count = 0
                            total_count = len(df_hist)
                            
                            # Fetch current prices for performance
                            tickers = df_hist.iloc[:, 1].unique().tolist()
                            prices = yf.download(tickers, period="1d")['Close'].iloc[-1]
                            
                            perf_data = []
                            for _, row in df_hist.iterrows():
                                sym = row.iloc[1]
                                entry_price = float(row.iloc[2])
                                curr_price = float(prices[sym]) if sym in prices else entry_price
                                change = ((curr_price - entry_price) / entry_price) * 100
                                is_profit = curr_price > entry_price
                                if is_profit: success_count += 1
                                
                                perf_data.append({
                                    "DATE": row.iloc[0],
                                    "TICKER": sym,
                                    "ENTRY": f"${entry_price:.2f}",
                                    "CURRENT": f"${curr_price:.2f}",
                                    "CHG %": f"{change:+.2f}%",
                                    "OUTCOME": "✅ PROFIT" if is_profit else "❌ LOSS"
                                })
                        
                        success_rate = (success_count / total_count) * 100
                        
                        col1, col2 = st.columns(2)
                        col1.metric("WIN RATE (7D)", f"{success_rate:.1f}%", delta=f"{success_rate-50:.1f}% vs Avg")
                        col2.metric("TOTAL SIGNALS", total_count)
                        
                        # Volume Chart
                        df_hist['Date'] = df_hist['Timestamp'].dt.date
                        vol_df = df_hist.groupby('Date').size().reset_index(name='Signals')
                        fig_vol = px.bar(vol_df, x='Date', y='Signals', title="Signal Volume (Last 7 Days)", color_discrete_sequence=['#00f2ff'])
                        fig_vol.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig_vol, use_container_width=True)
                        
                        # Detailed Table
                        st.dataframe(pd.DataFrame(perf_data), use_container_width=True, hide_index=True)
                    else: st.info("No signals found in the last 7 days.")
                else: st.info("Google Sheet is empty.")
            except Exception as e:
                st.error(f"Error fetching historical data: {e}")
        st.markdown('</div>', unsafe_allow_html=True)

    # Deep Analysis Section
    st.markdown("---")
    st.markdown('<h3 style="color: #00f2ff; font-family: \'Orbitron\';">🔍 DEEP ANALYSIS</h3>', unsafe_allow_html=True)
    selected_stock = st.selectbox("Select Ticker", ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "AMD", "GE", "UNH", "RTX"], label_visibility="collapsed")
    with st.spinner("Decoding Alpha Data..."):
        t = yf.Ticker(selected_stock)
        df_view = t.history(period="60d", interval="1h")
        if not df_view.empty:
            if isinstance(df_view.columns, pd.MultiIndex): df_view.columns = df_view.columns.get_level_values(0)
            df_view.columns = [c.lower() for c in df_view.columns]
            df_view['sma100'] = df_view['close'].rolling(100).mean()
            df_view['tp'] = (df_view['high'] + df_view['low'] + df_view['close']) / 3
            df_view['tpv'] = df_view['tp'] * df_view['volume']
            df_view['date'] = df_view.index.date
            df_view['vwap'] = df_view.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())
            fig_v = go.Figure()
            fig_v.add_trace(go.Candlestick(x=df_view.index, open=df_view['open'], high=df_view['high'], low=df_view['low'], close=df_view['close'], name="Price"))
            fig_v.add_trace(go.Scatter(x=df_view.index, y=df_view['sma100'], line=dict(color='#00f2ff', width=2), name="SMA 100"))
            fig_v.add_trace(go.Scatter(x=df_view.index, y=df_view['vwap'], line=dict(color='#ff9900', width=1.5, dash='dash'), name="VWAP"))
            fig_v.update_layout(template="plotly_dark", height=600, xaxis_rangeslider_visible=False)
            st.plotly_chart(fig_v, use_container_width=True)

else:
    # SYSTEM DOCUMENTATION PAGE (IFRAME COMPONENT)
    st.markdown('<h1 class="cyber-header">SYSTEM DOCUMENTATION</h1>', unsafe_allow_html=True)
    
    html_file_path = "walkthrough.html"
    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=2000, scrolling=True)
    except Exception as e:
        st.error(f"Error loading documentation: {e}")

st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.6); font-size: 0.8rem; font-family: \'Orbitron\'; letter-spacing: 3px;">NIRMAL RSA QUANT • DUBAI GST SYNCED</p>', unsafe_allow_html=True)
