import yfinance as yf
import sqlite3
import pandas as pd


class DataLoader:

    def __init__(self, path = 'yfinance_cache.db'):
        self.path = path
        self.setup_db()

    def fetch_data(self, ticker, start_date, end_date, interval='1d'):
        """
        Smart data fetcher that uses cached data when available,
        otherwise downloads fresh data and caches it.
        Returns data formatted for backtesting.py
        """
        # First check cache (only for daily data)
        if interval == '1d' and self.data_available_in_cache(ticker, start_date, end_date):
            print(f"Using cached data for {ticker}")
            data = self.get_cached_data(ticker, start_date, end_date)
        else:
            # Download fresh data
            print(f"Downloading fresh data for {ticker}")
            data = yf.download(ticker, start=start_date, end=end_date, interval=interval)

            # Handle MultiIndex if present
            if isinstance(data.columns, pd.MultiIndex):
                data = data.xs(ticker, axis=1, level=1)

            # Ensure we have all required columns
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()

            # Cache daily data (skip for other intervals)
            if interval == '1d':
                self.cache_data(ticker, start_date, end_date, data)

        return data


        return data
    def setup_db(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data(
                    ticker TEXT, 
                    date TEXT, 
                    open REAL, 
                    close REAL, 
                    high REAL, 
                    low REAL, 
                    volume, 
                    PRIMARY KEY (ticker, date))    
                """)


    def cache_data(self, ticker: str, start_date: str, end_date: str, data):
        with sqlite3.connect(self.path) as conn:
            for date, row in data.iterrows():
                # Convert Volume to int safely
                volume = int(row['Volume'].iloc[0]) if isinstance(row['Volume'], pd.Series) else int(row['Volume'])

                conn.execute("""INSERT OR IGNORE INTO stock_data VALUES(?,?,?,?,?,?,?)
                """, (
                    ticker,
                    date.strftime('%Y-%m-%d'),
                    float(row['Open'].iloc[0] if isinstance(row['Open'], pd.Series) else row['Open']),
                    float(row['Close'].iloc[0] if isinstance(row['Close'], pd.Series) else row['Close']),
                    float(row['High'].iloc[0] if isinstance(row['High'], pd.Series) else row['High']),
                    float(row['Low'].iloc[0] if isinstance(row['Low'], pd.Series) else row['Low']),
                    volume
                ))
            print(f"Cached {len(data)} days of {ticker} data")

    def get_cached_data(self, ticker, start_date, end_date):
        with sqlite3.connect(self.path) as conn:
            query = """SELECT date, open, close, high, low, volume FROM stock_data
                WHERE ticker = ? AND date BETWEEN ? and ?
                ORDER BY date
                """
            df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))

            if not df.empty:
                # Convert types and set index
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)

                # Rename all columns at once to match yfinance format
                df.columns = ['Open', 'Close', 'High', 'Low', 'Volume']

                # Ensure correct numeric types
                df['Volume'] = df['Volume'].astype(int)
                df[['Open', 'Close', 'High', 'Low']] = df[['Open', 'Close', 'High', 'Low']].astype(float)

            return df

    def data_available_in_cache(self, ticker, start_date, end_date, max_consecutive_missing=3):
        """
        Check cache for problematic gaps (>N consecutive missing weekdays)
        Returns True if cache is good enough for backtesting
        """
        with sqlite3.connect(self.path) as conn:
            # Get cached dates sorted
            dates = pd.to_datetime([
                row[0] for row in conn.execute(
                    "SELECT date FROM stock_data WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
                    (ticker, start_date, end_date))
            ])

            if len(dates) < 2:  # Not enough data to check gaps
                return False

            # Calculate day differences between consecutive dates
            day_diffs = dates.to_series().diff().dt.days.fillna(1)

            # Find consecutive missing weekdays (gaps >1 day, ignoring weekends)
            consecutive_missing = []
            current_gap = 0

            for diff in day_diffs:
                if diff > 1:  # Found a gap
                    # Only count weekdays in the gap (diff=2 could be weekend)
                    gap_weekdays = max(0, diff - 2)  # Subtract weekend days
                    current_gap += gap_weekdays
                else:
                    if current_gap > 0:
                        consecutive_missing.append(current_gap)
                    current_gap = 0

            # Check if any gap exceeds our threshold
            problematic_gaps = [g for g in consecutive_missing if g > max_consecutive_missing]

            if problematic_gaps:
                print(f"Found {len(problematic_gaps)} problematic gaps (> {max_consecutive_missing} weekdays missing)")
                return False
            return True