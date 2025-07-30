import yfinance as yf
import sqlite3
import pandas as pd
import sys
from termcolor import colored, cprint
import pandas as pd
import requests
from io import StringIO

from tqdm import tqdm


class DataLoader:

    def __init__(self, path = 'yfinance_cache.db'):
        self.path = path
        self._setup_db()

    def fetch_data(self, ticker, start_date, end_date, interval='1d'):
        """
        Smart data fetcher that uses cached data when available,
        otherwise downloads fresh data and caches it.
        Returns data formatted for backtesting.py
        """
        # First check cache (only for daily data)
        if interval == '1d' and self._data_available_in_cache(ticker, start_date, end_date):
            print(f"Using cached data for {ticker}")
            data = self._get_cached_data(ticker, start_date, end_date)
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
                self._cache_data(ticker, start_date, end_date, data)

        return data

    def get_all_symbols(self):
        try:
            nasdaq = self._get_nasdaq_symbols()
            nyse = self._get_nyse_symbols()
            print(f"Found {len(nasdaq.union(nyse))} symbols")
            return nasdaq.union(nyse)
        except Exception as e:
            print(f"Error fetching symbols: {e}")
            return set()

    def filter_good_tickers(self, tickers, save_path="good_tickers.csv"):
        """Returns DataFrame of valid tickers with their max timespans"""
        results = []
        for ticker in tqdm(tickers, desc="Screening tickers"):
            has_data, start_date, end_date = self._check_ticker_data_quality(ticker)
            if end_date is not None and start_date is not None:
                duration_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
            else:
                duration_days = None
            if has_data:
                results.append({'ticker': ticker, 'start_date': start_date, 'end_date': end_date,
                                'duration_days': duration_days})

        df = pd.DataFrame(results)

        if save_path:
            df.to_csv(save_path, index=False)
            print(f"Saved {len(df)} good tickers to {save_path}")

        return df

    # --- Private methods below ---

    def _setup_db(self):
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

    def _cache_data(self, ticker: str, start_date, end_date, data):
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

    def _get_cached_data(self, ticker, start_date, end_date):
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

    def _data_available_in_cache(self, ticker, start_date, end_date, max_consecutive_missing=7):
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
                print("Not enough data points to analyze gaps")
                return False

            # Convert to DataFrame for easier analysis
            df = pd.DataFrame({'date': dates})
            df['day_diff'] = df['date'].diff().dt.days.fillna(1)

            # Find all gaps > 1 day
            gaps = df[df['day_diff'] > 1].copy()

            # Add gap information
            if not gaps.empty:
                gaps['gap_start'] = gaps['date'].shift(1)
                gaps['gap_end'] = gaps['date']
                gaps['weekday_gap'] = gaps['day_diff'] - 2  # Subtract weekend days

                # Find largest gap
                largest_gap = gaps.loc[gaps['day_diff'].idxmax()]
                print(f"Largest gap was {largest_gap['day_diff']} calendar days "
                      f"from {largest_gap['gap_start'].date()} to {largest_gap['gap_end'].date()} "
                      f"({max(0, largest_gap['weekday_gap'])} weekdays missing)")

            # Find consecutive missing weekdays (gaps >1 day, ignoring weekends)
            consecutive_missing = []
            current_gap = 0

            for diff in df['day_diff']:
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

    def _get_nasdaq_symbols(self):
        """Get current NASDAQ-listed symbols"""
        url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        data = response.json()
        return {item['symbol'] for item in data['data']['table']['rows']}

    def _get_nyse_symbols(self):
        """Get current NYSE-listed symbols"""
        url = "https://www.nyse.com/api/quotes/filter"
        params = {
            'instrumentType': 'EQUITY',
            'pageNumber': 1,
            'sortColumn': 'NORMALIZED_TICKER',
            'sortOrder': 'ASC',
            'maxResultsPerPage': 10000
        }
        response = requests.post(url, json=params)
        return {item['symbolTicker'] for item in response.json()}

    def _check_ticker_data_quality(self, ticker):
        """Returns (has_data, max_timespan) tuple"""
        try:
            # Get metadata first (fast check)
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(period="max", interval="1d", prepost=False)

            # Essential checks
            if hist.empty:
                return (False, None, None)

            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in hist.columns for col in required_cols):
                return (False, None, None)

            # Check data completeness
            threshold = 0.95  # 95% non-null values required
            if hist[required_cols].isnull().mean().max() > (1 - threshold):
                return (False, None, None)

            # Determine maximum timespan
            start_date = hist.index[0].strftime('%Y-%m-%d')
            end_date = hist.index[-1].strftime('%Y-%m-%d')

            return (True, start_date, end_date)

        except Exception:
            return (False, None, None)


