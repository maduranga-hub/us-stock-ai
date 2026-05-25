# =============================================================================
# OPTIONS SCREENER — CALL / PUT / WAIT
# =============================================================================
# - Screens all S&P 500 stocks with market cap > $5B
# - Pulls 3 years of daily data from Alpaca
# - Runs LSTM signal model on each
# - Outputs clean table: Stock | Price | Signal | Confidence
#
# pip install alpaca-trade-api tensorflow scikit-learn pandas numpy requests
# =============================================================================

import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from sklearn.utils.class_weight import compute_class_weight
from sklearn.ensemble import RandomForestClassifier
import warnings
warnings.filterwarnings("ignore")

# =============================================================================
# CONFIG
# =============================================================================
ALPACA_API_KEY    = "PKHJPE2YU6BFOQQXIY7L33JI4P"
ALPACA_SECRET_KEY = "E3zKp3YWqym9rdVjTniP8myqb1tWDQSVDd32GZ1Ec3HG"
ALPACA_BASE_URL   = "https://data.alpaca.markets/v2"
ALPACA_TRADE_URL  = "https://api.alpaca.markets/v2"   # for latest quote

YEARS_BACK         = 3       # 3 years of training data
SEQ_LEN            = 40      # 2 months lookback (simplified)
FORWARD_WINDOW     = 60      # predict move within 60 trading days
MOVE_THRESHOLD_PCT = 0.04    # 4% move minimum
ASYMMETRY_RATIO    = 2.0     # gain must be 2x loss to call it CALL (or vice versa)
CONFIDENCE         = 0.60    # minimum probability to show signal
MIN_MARKET_CAP_B   = 5       # $5 billion minimum

TRAIN_FRAC = 0.70
VAL_FRAC   = 0.15

PUT  = 0
WAIT = 1
CALL = 2

# =============================================================================
# UNIVERSE — S&P 500 stocks > $5B market cap
# Hardcoded list of liquid large/mid caps to avoid needing a separate API
# Sorted by market cap descending — top names first
# =============================================================================
UNIVERSE = [
    # Mega cap
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK.B","JPM","V",
    "UNH","XOM","LLY","JNJ","MA","PG","AVGO","HD","MRK","CVX",
    "ABBV","COST","NFLX","AMD","CRM","BAC","PEP","TMO","ORCL","ACN",
    "MCD","ADBE","WMT","CSCO","ABT","TXN","NKE","PM","MS","GS",
    "AMGN","DHR","NEE","RTX","SCHW","BMY","QCOM","LIN","INTU","SPGI",
    # Large cap
    "BA","GE","CAT","DE","MMM","UPS","FDX","T","VZ","INTC",
    "IBM","F","GM","C","WFC","USB","PNC","AXP","BLK","CB",
    "AMT","PLD","CCI","EQIX","DLR","PSA","SPG","O","AVB","EQR",
    "SYK","MDT","BSX","ZTS","ISRG","REGN","VRTX","ILMN","BIIB","GILD",
    "MO","KO","PEP","CL","KMB","GIS","K","HSY","MKC","SJM",
]
# Remove duplicates preserving order
seen = set()
UNIVERSE = [x for x in UNIVERSE if not (x in seen or seen.add(x))]

# =============================================================================
# 1. FETCH LATEST REAL-TIME PRICE (fixes the stale price bug)
# =============================================================================

def fetch_latest_price(symbol: str, headers: dict) -> float:
    """Fetch real-time last trade price from Alpaca."""
    try:
        url  = f"{ALPACA_BASE_URL}/stocks/{symbol}/trades/latest"
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return float(resp.json()["trade"]["p"])
    except Exception:
        pass
    try:
        # Fallback: latest quote mid
        url  = f"{ALPACA_BASE_URL}/stocks/{symbol}/quotes/latest"
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            q = resp.json()["quote"]
            return (float(q["ap"]) + float(q["bp"])) / 2
    except Exception:
        pass
    return None


# =============================================================================
# 2. FETCH HISTORICAL BARS
# =============================================================================

def fetch_bars(symbol: str, years: int, headers: dict) -> pd.DataFrame:
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=years * 365)
    params = {
        "start":     start_date.strftime("%Y-%m-%d"),
        "end":       end_date.strftime("%Y-%m-%d"),
        "timeframe": "1Day",
        "limit":     10000,
        "feed":      "iex",
    }
    url      = f"{ALPACA_BASE_URL}/stocks/{symbol}/bars"
    all_bars = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        all_bars.extend(data.get("bars", []))
        nxt = data.get("next_page_token")
        if nxt:
            params["page_token"] = nxt
        else:
            break

    if len(all_bars) < 200:   # not enough history
        return None

    df = pd.DataFrame(all_bars)
    df.rename(columns={"t":"date","o":"open","h":"high",
                        "l":"low","c":"close","v":"volume"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    return df[["date","open","high","low","close","volume"]].sort_values("date").reset_index(drop=True)


# =============================================================================
# 3. FEATURE ENGINEERING (pure pandas — no PySpark for screener speed)
# =============================================================================

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    p = df.copy()

    # Returns
    p["returns"] = p["close"].pct_change()

    # SMAs + ratios (no raw price)
    for n in [10, 20, 50, 100, 200]:
        p[f"sma{n}"] = p["close"].rolling(n).mean()
    p["price_vs_sma50"]  = p["close"] / (p["sma50"]  + 1e-9)
    p["price_vs_sma200"] = p["close"] / (p["sma200"] + 1e-9)
    p["sma50_vs_sma200"] = p["sma50"] / (p["sma200"] + 1e-9)
    p["price_vs_sma20"]  = p["close"] / (p["sma20"]  + 1e-9)

    # Donchian channels
    for n in [20, 55]:
        p[f"don_high_{n}"] = p["high"].rolling(n).max()
        p[f"don_low_{n}"]  = p["low"].rolling(n).min()
        p[f"don_pos_{n}"]  = ((p["close"] - p[f"don_low_{n}"])
                              / (p[f"don_high_{n}"] - p[f"don_low_{n}"] + 1e-9))
        p[f"don_break_{n}"] = ((p["close"] - p[f"don_high_{n}"].shift(1))
                               / (p["close"] + 1e-9))
        p[f"don_down_{n}"]  = ((p[f"don_low_{n}"].shift(1) - p["close"])
                               / (p["close"] + 1e-9))

    # RSI 14
    delta = p["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    p["rsi"] = 100 - (100 / (1 + gain / (loss + 1e-9)))

    # RSI divergence
    p["rsi_slope_5"]   = p["rsi"] - p["rsi"].shift(5)
    p["price_slope_5"] = p["close"].pct_change(5)
    p["rsi_div_5"]     = (p["rsi_slope_5"] / 100) - p["price_slope_5"]
    p["rsi_slope_10"]  = p["rsi"] - p["rsi"].shift(10)
    p["price_slope_10"]= p["close"].pct_change(10)
    p["rsi_div_10"]    = (p["rsi_slope_10"] / 100) - p["price_slope_10"]

    # Fibonacci proxy
    p["high_252"] = p["high"].rolling(252).max()
    p["low_252"]  = p["low"].rolling(252).min()
    p["fib_pos"]  = ((p["close"] - p["low_252"])
                     / (p["high_252"] - p["low_252"] + 1e-9))
    p["dist_382"] = (p["fib_pos"] - 0.382).abs()
    p["dist_618"] = (p["fib_pos"] - 0.618).abs()

    # Bollinger Bands
    bb_mid      = p["close"].rolling(20).mean()
    bb_std      = p["close"].rolling(20).std()
    bb_upper    = bb_mid + 2 * bb_std
    bb_lower    = bb_mid - 2 * bb_std
    p["bb_pos"] = (p["close"] - bb_lower) / (bb_upper - bb_lower + 1e-9)
    p["bb_wid"] = (bb_upper - bb_lower) / (bb_mid + 1e-9)
    p["bb_sqz"] = p["bb_wid"] / (p["bb_wid"].rolling(60).max() + 1e-9)

    # Volume
    p["vol_ratio"] = p["volume"] / (p["volume"].rolling(20).mean() + 1e-9)
    p["vol_dir"]   = p["returns"] * p["vol_ratio"]
    p["don_vol"]   = p["don_break_20"] * p["vol_ratio"]

    # Volatility
    p["vola20"] = p["returns"].rolling(20).std()
    p["vola60"] = p["returns"].rolling(60).std()
    p["vol_reg"]= p["vola20"] / (p["vola60"] + 1e-9)

    # MACD (normalised)
    ema12       = p["close"].ewm(span=12, adjust=False).mean()
    ema26       = p["close"].ewm(span=26, adjust=False).mean()
    macd        = ema12 - ema26
    p["macd"]   = macd / (p["close"] + 1e-9)
    p["macd_sig"]= macd.ewm(span=9, adjust=False).mean() / (p["close"] + 1e-9)
    p["macd_h"] = p["macd"] - p["macd_sig"]
    p["macd_sl"]= p["macd"] - p["macd"].shift(3)

    # Momentum (normalised %)
    for lag in [5, 10, 20, 40]:
        p[f"mom_{lag}"] = p["close"].pct_change(lag)

    # Gap + HL ratio
    p["gap"]    = (p["open"] - p["close"].shift(1)) / (p["close"].shift(1) + 1e-9)
    p["hl_rat"] = (p["high"] - p["low"]) / (p["close"] + 1e-9)

    return p


# =============================================================================
# 4. BUILD TARGETS + LAG
# =============================================================================

FEATURE_RAW = [
    "price_vs_sma20","price_vs_sma50","price_vs_sma200","sma50_vs_sma200",
    "don_pos_20","don_break_20","don_down_20",
    "don_pos_55","don_break_55","don_down_55","don_vol",
    "rsi","rsi_div_5","rsi_div_10","rsi_slope_5","rsi_slope_10",
    "fib_pos","dist_382","dist_618",
    "bb_pos","bb_wid","bb_sqz",
    "vol_ratio","vol_dir",
    "returns","macd","macd_sig","macd_h","macd_sl",
    "vola20","vola60","vol_reg",
    "mom_5","mom_10","mom_20","mom_40",
    "gap","hl_rat",
]

def build_targets_and_lag(p: pd.DataFrame) -> pd.DataFrame:
    close = p["close"].values
    n     = len(close)
    max_gain = np.full(n, np.nan)
    max_loss = np.full(n, np.nan)
    for i in range(n - FORWARD_WINDOW):
        fwd           = close[i+1 : i+1+FORWARD_WINDOW]
        max_gain[i]   = (fwd.max()  - close[i]) / close[i]
        max_loss[i]   = (close[i]   - fwd.min()) / close[i]

    p["max_gain"] = max_gain
    p["max_loss"] = max_loss

    def classify(g, l):
        if pd.isna(g) or pd.isna(l):   return np.nan
        if g >= MOVE_THRESHOLD_PCT and g > l * ASYMMETRY_RATIO: return CALL
        if l >= MOVE_THRESHOLD_PCT and l > g * ASYMMETRY_RATIO: return PUT
        return WAIT

    p["target"] = [classify(g, l)
                   for g, l in zip(max_gain, max_loss)]

    for col in FEATURE_RAW:
        p[f"{col}_lag1"] = p[col].shift(1)

    lag_cols = [f"{col}_lag1" for col in FEATURE_RAW]
    keep     = ["date","close","max_gain","max_loss"] + lag_cols + ["target"]
    p        = p[keep].dropna().reset_index(drop=True)
    p["target"] = p["target"].astype(int)
    return p, lag_cols


# =============================================================================
# 5. SEQUENCES
# =============================================================================

def make_seq(X, y, seq_len=SEQ_LEN):
    Xs, ys = [], []
    for i in range(seq_len, len(X)):
        # Flatten the 2D window (time_steps, features) into a 1D array for scikit-learn
        Xs.append(X[i-seq_len:i].flatten())
        ys.append(y[i])
    return np.array(Xs), np.array(ys)


# =============================================================================
# 6. MODEL
# =============================================================================

def build_model(n_features):
    # Random Forest works well on tabular/flattened data out of the box
    return RandomForestClassifier(
        n_estimators=100, 
        max_depth=10, 
        random_state=42, 
        n_jobs=-1
    )


def train(model, X_tr, y_tr, X_vl, y_vl):
    classes = np.unique(y_tr)
    if len(classes) < 3:
        # Fallback equal weights if a class is missing in train split
        cw = {0:1.5, 1:2.5, 2:1.5}
    else:
        w  = compute_class_weight("balanced", classes=classes, y=y_tr)
        cw = {int(c): float(w[i]) for i, c in enumerate(classes)}
        # Always boost WAIT — it's the hardest class
        cw[WAIT] = max(cw.get(WAIT, 1.0), 2.0)

    model.set_params(class_weight=cw)
    model.fit(X_tr, y_tr)
    return model


# =============================================================================
# 7. PROCESS ONE STOCK → returns signal row or None
# =============================================================================

def process_stock(symbol: str, headers: dict) -> dict | None:
    # Fetch bars
    df = fetch_bars(symbol, YEARS_BACK, headers)
    if df is None or len(df) < 300:
        return None

    # Features
    try:
        feat = build_features(df)
        pdf, lag_cols = build_targets_and_lag(feat)
    except Exception:
        return None

    if len(pdf) < SEQ_LEN + 50:
        return None

    # Check class distribution — need all 3 classes
    dist = pdf["target"].value_counts()
    if len(dist) < 2:
        return None

    # Split
    n     = len(pdf)
    t_end = int(n * TRAIN_FRAC)
    v_end = int(n * (TRAIN_FRAC + VAL_FRAC))

    train_df = pdf.iloc[:t_end]
    val_df   = pdf.iloc[t_end:v_end]

    scaler  = MinMaxScaler()
    X_train = scaler.fit_transform(train_df[lag_cols])
    X_val   = scaler.transform(val_df[lag_cols])
    y_train = train_df["target"].values
    y_val   = val_df["target"].values

    if len(np.unique(y_train)) < 2:
        return None

    X_tr, y_tr = make_seq(X_train, y_train)
    X_vl, y_vl = make_seq(X_val,   y_val)

    if len(X_tr) < 30 or len(X_vl) < 10:
        return None

    # Train
    try:
        model = build_model(len(lag_cols))
        model = train(model, X_tr, y_tr, X_vl, y_vl)
    except Exception:
        return None

    # Live signal — last SEQ_LEN rows
    recent = pdf[lag_cols].tail(SEQ_LEN).values
    if len(recent) < SEQ_LEN:
        return None

    # Flatten for Random Forest
    X_live = scaler.transform(recent).flatten().reshape(1, -1)
    
    # Predict probabilities
    probs  = model.predict_proba(X_live)[0]

    # Map probabilities to classes safely (since not all classes may be present in model)
    p_put, p_wait, p_call = 0.0, 0.0, 0.0
    for idx, cls in enumerate(model.classes_):
        if cls == PUT:   p_put  = float(probs[idx])
        elif cls == WAIT: p_wait = float(probs[idx])
        elif cls == CALL: p_call = float(probs[idx])

    # Determine signal
    max_p = max(p_put, p_wait, p_call)
    if max_p < CONFIDENCE:
        signal     = "WAIT"
        confidence = p_wait
    elif p_call >= CONFIDENCE:
        signal     = "CALL [UP]"
        confidence = p_call
    elif p_put >= CONFIDENCE:
        signal     = "PUT [DOWN]"
        confidence = p_put
    else:
        signal     = "WAIT [NO_EDGE]"
        confidence = p_wait

    # Real-time price
    live_price = fetch_latest_price(symbol, headers)
    bar_price  = float(pdf["close"].iloc[-1])
    price      = live_price if live_price else bar_price
    price_src  = "live" if live_price else "last bar"

    # Strike suggestions
    strike_call = round(price * 1.018, 2)
    strike_put  = round(price * 0.982, 2)

    return {
        "symbol":      symbol,
        "price":       price,
        "price_src":   price_src,
        "signal":      signal,
        "p_call":      round(p_call  * 100, 1),
        "p_put":       round(p_put   * 100, 1),
        "p_wait":      round(p_wait  * 100, 1),
        "confidence":  round(confidence * 100, 1),
        "call_strike": strike_call,
        "put_strike":  strike_put,
    }


# =============================================================================
# 8. PRINT RESULTS TABLE
# =============================================================================

def print_table(results: list[dict]) -> None:
    if not results:
        print("No signals generated.")
        return

    df = pd.DataFrame(results)

    # Separate into CALL, PUT, WAIT
    calls = df[df["signal"].str.startswith("CALL")].sort_values("p_call", ascending=False)
    puts  = df[df["signal"].str.startswith("PUT") ].sort_values("p_put",  ascending=False)
    waits = df[df["signal"].str.startswith("WAIT")].sort_values("p_wait", ascending=False)

    header = (f"\n{'='*72}\n"
              f"  OPTIONS SCREENER  |  {datetime.today().strftime('%Y-%m-%d %H:%M')}  "
              f"|  Min confidence: {CONFIDENCE*100:.0f}%\n"
              f"{'='*72}")
    print(header)

    col_w = {"sym":6, "price":8, "sig":10, "conf":8, "strike":10}

    def section_header(title):
        print(f"\n  {title}")
        print(f"  {'─'*68}")
        print(f"  {'STOCK':<{col_w['sym']}}  "
              f"{'PRICE':>{col_w['price']}}  "
              f"{'SIGNAL':<{col_w['sig']}}  "
              f"{'CONF %':>{col_w['conf']}}  "
              f"{'STRIKE':>{col_w['strike']}}  "
              f"{'P(CALL)%':>9}  {'P(PUT)%':>8}  {'SRC':<6}")
        print(f"  {'─'*68}")

    def print_row(r):
        if r["signal"].startswith("CALL"):
            strike = f"${r['call_strike']}"
        elif r["signal"].startswith("PUT"):
            strike = f"${r['put_strike']}"
        else:
            strike = "—"
        print(f"  {r['symbol']:<{col_w['sym']}}  "
              f"${r['price']:>{col_w['price']-1}.2f}  "
              f"{r['signal']:<{col_w['sig']}}  "
              f"{r['confidence']:>{col_w['conf']}.1f}%  "
              f"{strike:>{col_w['strike']}}  "
              f"{r['p_call']:>9.1f}%  "
              f"{r['p_put']:>8.1f}%  "
              f"{r['price_src']:<6}")

    if not calls.empty:
        section_header("[UP] CALL SIGNALS")
        for _, r in calls.iterrows():
            print_row(r)

    if not puts.empty:
        section_header("[DOWN] PUT SIGNALS")
        for _, r in puts.iterrows():
            print_row(r)

    if not waits.empty:
        section_header("[NO_EDGE] WAIT")
        for _, r in waits.iterrows():
            print_row(r)

    print(f"\n{'='*72}")
    print(f"  SUMMARY  |  "
          f"CALL: {len(calls)}  |  PUT: {len(puts)}  |  WAIT: {len(waits)}  |  "
          f"Total screened: {len(df)}")
    print(f"{'='*72}\n")

    # Also save to CSV
    df.to_csv("/tmp/options_signals.csv", index=False)
    print("  Full results saved → /tmp/options_signals.csv\n")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    headers = {
        "APCA-API-KEY-ID":     ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }

    print(f"\n{'='*72}")
    print(f"  OPTIONS SCREENER — {len(UNIVERSE)} stocks, >{MIN_MARKET_CAP_B}B market cap")
    print(f"  Data: {YEARS_BACK} years | Seq: {SEQ_LEN}d | Horizon: {FORWARD_WINDOW}d")
    print(f"  Move threshold: {MOVE_THRESHOLD_PCT*100:.0f}% | Confidence: {CONFIDENCE*100:.0f}%")
    print(f"{'='*72}\n")

    results = []
    failed  = []

    for i, symbol in enumerate(UNIVERSE, 1):
        print(f"  [{i:3d}/{len(UNIVERSE)}] {symbol:<6} ... ", end="", flush=True)
        try:
            row = process_stock(symbol, headers)
            if row:
                results.append(row)
                print(f"{row['signal']:<12} ({row['confidence']:.0f}%) "
                      f"@ ${row['price']:.2f}")
            else:
                failed.append(symbol)
                print("skipped (insufficient data)")
        except Exception as e:
            failed.append(symbol)
            print(f"error: {e}")

    print_table(results)

    if failed:
        print(f"  Skipped ({len(failed)}): {', '.join(failed)}\n")
