import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime

# Page Configuration
st.set_page_config(page_title="US Stock AI | Quant Terminal", layout="wide")

# Custom CSS for Glassmorphism & Cyberpunk UI
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Inter:wght@400;700&display=swap');
    
    .stApp {
        background: radial-gradient(circle at top right, #0d001a 0%, #050505 100%);
        color: #ffffff;
        font-family: 'Inter', sans-serif;
    }
    
    /* Neon Headers */
    .cyber-header {
        font-family: 'Orbitron', sans-serif;
        color: #00f2ff;
        text-shadow: 0 0 15px #00f2ff;
        text-align: center;
        letter-spacing: 5px;
        margin-bottom: 30px;
    }
    
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(15px);
        border: 1px solid rgba(0, 242, 255, 0.1);
        border-radius: 20px;
        padding: 25px;
        margin-bottom: 20px;
    }

    /* High Contrast Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: rgba(255, 255, 255, 0.02);
        padding: 5px;
        border-radius: 12px;
    }

    .stTabs [data-baseweb="tab"] {
        color: #ffffff !important;
        font-weight: 700 !important;
        font-family: 'Inter', sans-serif;
        border-radius: 8px;
        padding: 10px 20px;
    }

    .stTabs [aria-selected="true"] {
        background-color: #00f2ff !important;
        color: #050505 !important;
        box-shadow: 0 0 20px rgba(0, 242, 255, 0.6);
    }

    /* Dataframe Visibility */
    .stDataFrame {
        background-color: rgba(255, 255, 255, 0.01);
        border: 1px solid rgba(0, 242, 255, 0.1);
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="cyber-header">US STOCK AI : QUANT TERMINAL</h1>', unsafe_allow_html=True)

# Tabs for Navigation
tab_signals, tab_overview, tab_earnings, tab_heatmap, tab_guide = st.tabs([
    "🚀 BUY SIGNALS", 
    "🌍 MARKET WATCH (>500M)", 
    "📅 EARNINGS CENTER", 
    "📊 RSI HEATMAP",
    "📖 SYSTEM GUIDE"
])

with tab_signals:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("📡 Live Technical Signal Feed")
    try:
        df_sig = pd.read_csv("active_signals.csv")
        if df_sig.empty:
            st.markdown('<div style="text-align: center; padding: 40px; border: 1px dashed rgba(0, 242, 255, 0.3); border-radius: 15px;">'
                        '<h3 style="color: #00f2ff; text-shadow: 0 0 10px #00f2ff;">📡 SCANNING IN PROGRESS...</h3>'
                        '</div>', unsafe_allow_html=True)
        else:
            st.dataframe(df_sig, use_container_width=True, hide_index=True)
    except:
        st.info("Initializing engine...")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_overview:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("🌍 Global Market Watch")
    try:
        df_ov = pd.read_csv("market_overview_technical.csv")
        st.dataframe(df_ov.sort_values(by='market_cap', ascending=False), use_container_width=True, hide_index=True)
    except:
        st.info("Loading market data...")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_earnings:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("🗓️ Earnings Board")
    try:
        df_earn = pd.read_csv("active_earnings_signals.csv")
        st.dataframe(df_earn, use_container_width=True, hide_index=True)
    except:
        st.info("No major earnings reports scheduled.")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_heatmap:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("🗺️ Live RSI Momentum Heatmap")
    try:
        df_h = pd.read_csv("market_overview_technical.csv")
        fig = px.treemap(df_h, path=['symbol'], values='market_cap', color='rsi',
                         color_continuous_scale='RdYlGn_r', range_color=[20, 80])
        fig.update_layout(template="plotly_dark", margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    except:
        st.info("Heatmap generating...")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_guide:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<h2 style="color: #00f2ff; font-family: \'Orbitron\';">📖 US STOCK AI v4.0 WORKFLOW</h2>', unsafe_allow_html=True)
    
    st.markdown("""
    ### 📊 System Architecture
    The system operates as a serverless pipeline, leveraging GitHub Actions for orchestration, YFinance for data, and Telegram/Google Sheets for delivery.
    
    ### 🔍 Phase 1: The Multi-Timeframe (MTF) Brain
    1. **Daily Trend Filter**: Price must be above SMA 100 on the Daily chart.
    2. **Hourly Entry**: RSI <= 35 and Bullish MACD Crossover on the 1-Hour chart.
    
    ### ⚡ Phase 2: High Conviction Filtering
    A signal is upgraded to **🔥 HIGH CONVICTION** if Volume is > 1.5x higher than average and price shows strong intraday momentum (Above VWAP).
    
    ### 🕒 Timing Logic (Dubai GST)
    - **Technical Scans**: Hourly at :25 past (4:25 PM - 12:25 AM).
    - **Earnings Scans**: Daily at 3:30 PM.
    ---
    <p style="text-align: center; color: #00f2ff; opacity: 0.5;">System Status: Operational v4.0</p>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Selection for Analysis
st.markdown("---")
st.markdown('<h3 style="color: #00f2ff; text-shadow: 0 0 10px #00f2ff; font-family: \'Orbitron\';">🔍 DEEP ANALYSIS</h3>', unsafe_allow_html=True)
selected_stock = st.selectbox("Select Ticker", ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "AMD", "GE", "UNH", "RTX"], label_visibility="collapsed")

with st.spinner("Decoding Alpha Data..."):
    t = yf.Ticker(selected_stock)
    df_view = t.history(period="60d", interval="1h")
    if not df_view.empty:
        if isinstance(df_view.columns, pd.MultiIndex): df_view.columns = df_view.columns.get_level_values(0)
        df_view.columns = [c.lower() for c in df_view.columns]
        df_view['sma100'] = df_view['close'].rolling(100).mean()
        
        # VWAP Calculation
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

st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.2); font-size: 0.8rem;">NIRMAL RSA QUANT • DUBAI GST SYNCED</p>', unsafe_allow_html=True)
