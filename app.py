import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import pytz

# Page config
st.set_page_config(page_title="US Stock AI | Terminal", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for Cyberpunk Aesthetic
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;900&family=Inter:wght@400;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #050505;
        color: #ffffff;
    }
    
    .stApp {
        background: radial-gradient(circle at top right, #0a0022 0%, #050505 100%);
    }
    
    .cyber-header {
        font-family: 'Orbitron', sans-serif;
        font-size: 3rem;
        font-weight: 900;
        letter-spacing: 5px;
        color: #00f2ff;
        text-shadow: 0 0 20px #00f2ff55;
        margin-bottom: 5px;
    }
    
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(0, 242, 255, 0.1);
        border-radius: 15px;
        padding: 20px;
        margin-bottom: 20px;
    }
    
    .stat-label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: rgba(0, 242, 255, 0.5);
        font-weight: 700;
    }
    
    .stat-value {
        font-size: 1.5rem;
        font-weight: 900;
        color: #ffffff;
    }
    
    /* Table Styling */
    .stDataFrame {
        border: 1px solid rgba(0, 242, 255, 0.1);
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# Technical Indicator Functions
def calculate_indicators(df):
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain/loss)))
    
    # SMA 100
    df['sma100'] = df['close'].rolling(window=100).mean()
    
    # VWAP
    tp = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (df['volume'] * tp).cumsum() / df['volume'].cumsum()
    
    return df

def get_watchlist_data(symbols):
    results = []
    for s in symbols:
        try:
            t = yf.Ticker(s)
            h = t.history(period="2d", interval="1h")
            if isinstance(h.columns, pd.MultiIndex): h.columns = h.columns.get_level_values(0)
            h.columns = [c.lower() for c in h.columns]
            
            price = h['close'].iloc[-1]
            prev_price = h['close'].iloc[-2]
            change = ((price - prev_price) / prev_price) * 100
            
            # Fetch RSI
            full_h = t.history(period="5d", interval="1h")
            if isinstance(full_h.columns, pd.MultiIndex): full_h.columns = full_h.columns.get_level_values(0)
            full_h.columns = [c.lower() for c in full_h.columns]
            rsi = calculate_indicators(full_h)['rsi'].iloc[-1]
            sma100 = full_h['close'].rolling(100).mean().iloc[-1]
            
            trend = "BULLISH" if price > sma100 else "BEARISH"
            
            results.append({
                "Ticker": s,
                "Price": f"${price:.2f}",
                "Change %": f"{change:+.2f}%",
                "RSI": round(rsi, 2),
                "Trend": trend
            })
        except: continue
    return pd.DataFrame(results)

def get_earnings_calendar(symbols):
    results = []
    for s in symbols:
        try:
            t = yf.Ticker(s)
            cal = t.calendar
            if cal is not None:
                e_date = "N/A"
                if isinstance(cal, pd.DataFrame) and not cal.empty:
                    if 'Earnings Date' in cal.index: e_date = cal.loc['Earnings Date'].iloc[0]
                elif isinstance(cal, dict) and 'Earnings Date' in cal:
                    e_date = cal['Earnings Date'][0]
                
                results.append({"Ticker": s, "Earnings Date": e_date})
        except: continue
    return pd.DataFrame(results)

# Header
st.markdown('<h1 class="cyber-header">US STOCK AI</h1>', unsafe_allow_html=True)
st.markdown('<p style="color: rgba(0,242,255,0.4); font-weight: 700; margin-bottom: 40px; letter-spacing: 5px;">QUANTITATIVE TERMINAL • v2.0 EARNINGS EDITION</p>', unsafe_allow_html=True)

WATCHLIST = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "NFLX"]

# Layout: Two Columns for Overview
col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<p class="stat-label">📡 Signal Pulse (Watchlist Overview)</p>', unsafe_allow_html=True)
    pulse_df = get_watchlist_data(WATCHLIST)
    st.dataframe(pulse_df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col_right:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<p class="stat-label">📅 Upcoming Earnings</p>', unsafe_allow_html=True)
    earnings_df = get_earnings_calendar(WATCHLIST)
    st.dataframe(earnings_df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Selection for Deep Analysis
st.markdown('<p class="stat-label" style="margin-left: 10px;">🔍 Deep Asset Analysis</p>', unsafe_allow_html=True)
selected_stock = st.selectbox("SELECT TICKER", WATCHLIST, label_visibility="collapsed")

with st.spinner(f"DECODING {selected_stock} MARKET DATA..."):
    t_obj = yf.Ticker(selected_stock)
    df = t_obj.history(period="60d", interval="1h")
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = calculate_indicators(df)

if not df.empty:
    # Chart
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name="Price", increasing_line_color='#00ff88', decreasing_line_color='#ff3366'
    ))
    fig.add_trace(go.Scatter(x=df.index, y=df['sma100'], line=dict(color='#ff00ff', width=2), name="SMA 100"))
    fig.add_trace(go.Scatter(x=df.index, y=df['vwap'], line=dict(color='#ffffff', width=1, dash='dot'), name="VWAP"))

    fig.update_layout(
        template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        height=500, margin=dict(l=0, r=0, t=0, b=0), xaxis_rangeslider_visible=False,
        yaxis=dict(gridcolor='rgba(0,242,255,0.05)'), xaxis=dict(gridcolor='rgba(0,242,255,0.05)')
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown('<p style="text-align: center; color: rgba(0,242,255,0.2); font-size: 0.7rem; margin-top: 50px; letter-spacing: 2px;">DUBAI GST CLOCK SYNCED • CORE v2.0 • NIRMAL RSA</p>', unsafe_allow_html=True)
