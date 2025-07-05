import yfinance as yf
import pandas as pd

def fetch_data(ticker, start_date, end_date, interval='1d'):
    """
    Fetch and properly format data for backtesting.py
    """
    # Download data
    data = yf.download(ticker, start=start_date, end=end_date, interval=interval)
    print(data)
    print(type(data))
    # Extract just the price data (removing ticker level from MultiIndex)
    if isinstance(data.columns, pd.MultiIndex):
        data = data.xs(ticker, axis=1, level=1)

    # Rename columns to match backtesting.py's exact requirements
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

    # Ensure proper datetime index
    # data.index = pd.to_datetime(data.index)
    print(data)
    return data

