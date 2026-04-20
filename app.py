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
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
        background-color: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        color: white;
        padding: 0 20px;
    }

    .stTabs [aria-selected="true"] {
        background-color: #00f2ff !important;
        color: #000 !important;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="cyber-header">US STOCK AI : QUANT TERMINAL</h1>', unsafe_allow_html=True)

# Tabs for Navigation
tab_signals, tab_earnings, tab_heatmap = st.tabs(["🚀 BUY SIGNALS", "📅 EARNINGS CENTER", "📊 MARKET HEATMAP"])

with tab_signals:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("📡 Live Technical Signal Feed")
    try:
        df_sig = pd.read_csv("active_technical_signals.csv")
        st.dataframe(df_sig, use_container_width=True, hide_index=True)
    except:
        st.info("Scanner is currently looking for signals... Check Telegram for live alerts.")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_earnings:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("🗓️ Tomorrow's Earnings Reports")
    try:
        df_earn = pd.read_csv("active_earnings_signals.csv")
        st.dataframe(df_earn, use_container_width=True, hide_index=True)
    except:
        st.info("No major earnings reports scheduled for tomorrow.")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_heatmap:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.subheader("🗺️ RSI Momentum Heatmap (S&P 500 Sample)")
    # Mock data for visualization since live scan of 500 stocks on page load is slow
    mock_data = {
        'Symbol': ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'COST'],
        'RSI': [42, 38, 65, 52, 48, 59, 28, 35, 72, 45],
        'MarketCap': [3.0, 2.8, 2.5, 1.8, 1.7, 1.2, 0.6, 0.3, 0.2, 0.4]
    }
    df_h = pd.DataFrame(mock_data)
    fig = px.treemap(df_h, path=['Symbol'], values='MarketCap', color='RSI',
                     color_continuous_scale='RdYlGn_r',
                     range_color=[20, 80])
    fig.update_layout(template="plotly_dark", margin=dict(t=0, l=0, r=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Selection for Analysis
st.markdown("---")
selected_stock = st.selectbox("🔍 SELECT STOCK FOR DEEP ANALYSIS", ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "AMD"])

with st.spinner("Decoding Alpha Data..."):
    t = yf.Ticker(selected_stock)
    df_view = t.history(period="60d", interval="1h")
    if not df_view.empty:
        if isinstance(df_view.columns, pd.MultiIndex): df_view.columns = df_view.columns.get_level_values(0)
        df_view.columns = [c.lower() for c in df_view.columns]
        
        # Simple SMA for view
        df_view['sma100'] = df_view['close'].rolling(100).mean()
        
        fig_v = go.Figure()
        fig_v.add_trace(go.Candlestick(x=df_view.index, open=df_view['open'], high=df_view['high'], low=df_view['low'], close=df_view['close'], name="Price"))
        fig_v.add_trace(go.Scatter(x=df_view.index, y=df_view['sma100'], line=dict(color='#00f2ff', width=2), name="SMA 100"))
        fig_v.update_layout(template="plotly_dark", height=600, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig_v, use_container_width=True)

st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.2); font-size: 0.8rem; letter-spacing: 3px;">NIRMAL RSA QUANT • DUBAI GST SYNCED</p>', unsafe_allow_html=True)
