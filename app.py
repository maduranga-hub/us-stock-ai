import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime

# Page Configuration
st.set_page_config(page_title="US Stock AI | Quant Terminal", layout="wide")

# Custom CSS for Dashboard
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
    tab_signals, tab_overview, tab_earnings, tab_heatmap = st.tabs(["🚀 BUY SIGNALS", "🌍 MARKET WATCH", "📅 EARNINGS CENTER", "📊 RSI HEATMAP"])

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

    # Deep Analysis
    st.markdown("---")
    st.markdown('<h3 style="color: #00f2ff; font-family: \'Orbitron\';">🔍 DEEP ANALYSIS</h3>', unsafe_allow_html=True)
    selected_stock = st.selectbox("Select Ticker", ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "AMD", "GE", "UNH", "RTX"], label_visibility="collapsed")
    with st.spinner("Decoding Data..."):
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
    # PREMIUM DOCUMENTATION (FULL HTML REPLICA)
    st.markdown("""
        <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
        <script>mermaid.initialize({startOnLoad:true, theme: 'dark'});</script>
        <style>
            .doc-container { background: rgba(255, 255, 255, 0.02); padding: 40px; border-radius: 30px; border: 1px solid rgba(0, 242, 255, 0.15); }
            .doc-header { font-family: 'Orbitron'; color: #00f2ff; text-align: center; border-bottom: 2px solid rgba(0, 242, 255, 0.2); padding-bottom: 20px; letter-spacing: 5px; }
            .section-box { background: rgba(255, 255, 255, 0.03); padding: 25px; border-radius: 20px; margin: 25px 0; border-left: 5px solid #00f2ff; }
            .mermaid-box { background: rgba(0,0,0,0.3); padding: 20px; border-radius: 15px; border: 1px solid rgba(0, 242, 255, 0.1); margin: 20px 0; display: flex; justify-content: center; }
            h3 { color: #00f2ff; font-family: 'Orbitron'; font-size: 1.1rem; }
            p, li { color: #ccc; line-height: 1.6; }
        </style>
        <div class="doc-container">
            <h1 class="doc-header">US STOCK AI v4.0</h1>
            <p style="text-align: center; color: #00f2ff; font-weight: bold; opacity: 0.7;">WORKFLOW WALKTHROUGH</p>
            
            <div class="section-box">
                <h3>📊 System Architecture Overview</h3>
                <p>The system operates as a serverless pipeline, leveraging GitHub Actions for orchestration, YFinance for data, and Telegram/Google Sheets for delivery.</p>
                <div class="mermaid-box">
                    <pre class="mermaid" style="background: transparent; border: none;">
                        graph TD
                        A[GitHub Actions Scheduler] -->|Trigger :25 past| B(main.py Engine)
                        B --> C{MTF Check}
                        C -->|Price < Daily SMA 100| D[Discard]
                        C -->|Price > Daily SMA 100| E[1-Hour Analysis]
                        E --> F{Technical Logic}
                        F -->|Match: RSI <= 35 & MACD Cross| G[Generate Alert]
                        G --> H[Telegram Broadcast]
                        G --> I[Google Sheets Sync]
                        G --> J[CSV Data Push]
                        J --> K[Streamlit Dashboard]
                    </pre>
                </div>
            </div>

            <div class="section-box">
                <h3>🔍 Phase 1: The Multi-Timeframe (MTF) Brain</h3>
                <p><b>1. Daily Trend Filter:</b> Price must be above SMA 100 on the Daily chart.</p>
                <p><b>2. Hourly Execution:</b> RSI &le; 35 and Bullish MACD Crossover on the 1-Hour chart.</p>
            </div>

            <div class="section-box">
                <h3>⚡ Phase 2: High Conviction Filtering</h3>
                <p>Signals are flagged as <b>🔥 HIGH CONVICTION</b> if Volume is > 1.5x average and price is Above VWAP.</p>
            </div>

            <div class="section-box">
                <h3>📲 Phase 3: Real-Time Synchronization</h3>
                <ul>
                    <li><b>Telegram Alert:</b> Structured message with AI Analysis Note.</li>
                    <li><b>Google Sheets Sync:</b> Live logging for mobile tracking.</li>
                </ul>
            </div>

            <div class="section-box">
                <h3>🕒 Timing Logic (Dubai GST)</h3>
                <ul>
                    <li><b>Technical Scans:</b> Hourly at :25 past (4:25 PM - 12:25 AM).</li>
                    <li><b>Earnings Scans:</b> Daily at 3:30 PM Dubai Time.</li>
                </ul>
            </div>
            
            <p style="text-align: center; color: rgba(0,242,255,0.3); font-size: 0.8rem; margin-top: 40px;">NIRMAL RSA QUANT • PRODUCTION READY v4.0</p>
        </div>
    """, unsafe_allow_html=True)

st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.2); font-size: 0.8rem;">NIRMAL RSA QUANT • DUBAI GST SYNCED</p>', unsafe_allow_html=True)
