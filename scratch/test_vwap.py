import yfinance as yf
import pandas as pd

ticker = yf.Ticker("AAPL")
df = ticker.history(period="5d", interval="1h")
if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
df.columns = [c.lower() for c in df.columns]

df['tp'] = (df['high'] + df['low'] + df['close']) / 3
df['tpv'] = df['tp'] * df['volume']
df['date'] = df.index.date

# Manual VWAP
df['vwap'] = df.groupby('date', group_keys=False).apply(lambda x: x['tpv'].cumsum() / x['volume'].cumsum())

print(df[['close', 'vwap']].tail(10))
