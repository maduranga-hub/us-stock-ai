import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime

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
    </style>
""", unsafe_allow_html=True)

# Sidebar Navigation
with st.sidebar:
    st.markdown('<h2 style="color: #00f2ff; font-family: \'Orbitron\'; text-align: center;">MENU</h2>', unsafe_allow_html=True)
    page = st.radio("Go to:", ["📊 QUANT DASHBOARD", "📖 SYSTEM DOCUMENTATION"])
    st.markdown("---")
    st.markdown('<p style="text-align: center; color: #00f2ff; opacity: 0.5;">v4.0 PRO RELEASE</p>', unsafe_allow_html=True)

if page == "📊 QUANT DASHBOARD":
    st.markdown('<h1 class="cyber-header">US STOCK AI : QUANT TERMINAL</h1>', unsafe_allow_html=True)

    tab_signals, tab_overview, tab_earnings, tab_heatmap = st.tabs([
        "🚀 BUY SIGNALS", "🌍 MARKET WATCH", "📅 EARNINGS CENTER", "📊 RSI HEATMAP"
    ])

    with tab_signals:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        try:
            df_sig = pd.read_csv("active_signals.csv")
            st.dataframe(df_sig, use_container_width=True, hide_index=True)
        except: st.info("Scanning...")
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
    # DOCUMENTATION PAGE
    st.markdown('<h1 class="cyber-header">SYSTEM DOCUMENTATION</h1>', unsafe_allow_html=True)
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    
    # Architecture Overview with CSS-based styling
    st.markdown("""
    <div style="background: rgba(0,0,0,0.5); padding: 20px; border-radius: 15px; border: 1px solid #00f2ff; margin-bottom: 30px;">
        <h3 style="color: #00f2ff; font-family: 'Orbitron'; text-align: center;">📊 OPERATIONAL WORKFLOW</h3>
        <p style="text-align: center; color: #fff;">
            GitHub Actions Scheduler ➔ <b>main.py Engine</b> ➔ MTF Check (SMA 100) ➔ 1-Hour Analysis (RSI/MACD) ➔ <b>Signal Delivery</b>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    ### 🔍 PHASE 1: THE MULTI-TIMEFRAME (MTF) BRAIN
    **1. Daily Trend Filter**: Every stock must trade above its 100-day Moving Average.
    **2. Hourly Execution**: RSI <= 35 and Bullish MACD Crossover on the 1-hour timeframe.
    
    ### ⚡ PHASE 2: HIGH CONVICTION FILTERING
    Signals are flagged as **🔥 HIGH CONVICTION** if:
    - Volume Spike > 1.5x average.
    - Price Above VWAP.
    
    ### 📲 PHASE 3: REAL-TIME SYNCHRONIZATION
    - **Telegram**: Instant alerts with analysis notes.
    - **Google Sheets**: Automated logging for mobile tracking.
    
    ### 🖥️ PHASE 4: THE QUANT DASHBOARD
    - **RSI Heatmap**: Visualization of market sentiment.
    - **Deep Analysis**: Interactive technical charting terminal.
    
    ### 🕒 TIMING LOGIC (DUBAI GST)
    - **Technical Scans**: Hourly at :25 past (4:25 PM - 12:25 AM).
    - **Earnings Scans**: Daily at 3:30 PM Dubai Time.
    """)
    
    st.markdown("---")
    st.markdown("#### 📄 Printable Version")
    st.info("To download the full visual report with diagrams, open the `walkthrough.html` file from your repository.")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.2); font-size: 0.8rem;">NIRMAL RSA QUANT • DUBAI GST SYNCED</p>', unsafe_allow_html=True)
