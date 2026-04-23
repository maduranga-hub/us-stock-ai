import main
import pandas as pd

res = main.analyze_ticker("AAPL")
if res:
    print(f"Symbol: {res['symbol']}")
    print(f"Price: {res['price']}")
    print(f"VWAP Status: {res.get('vwap_status')}")
    print(f"Is Signal: {res.get('is_signal')}")
else:
    print("Ticker skipped or error occurred.")
