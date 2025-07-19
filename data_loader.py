import yfinance as yf
import sqlite3
import pandas as pd
import sys
from termcolor import colored, cprint


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

    def data_available_in_cache(self, ticker, start_date, end_date, max_consecutive_missing=7):
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

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go  # For visualization
from typing import List, Dict, Union  # Optional but recommended for type hints


class CacheForensics:
    def __init__(self, db_path='yfinance_cache.db'):
        self.db_path = db_path

    def get_gap_analysis(self, ticker, start_date, end_date):
        """Comprehensive analysis of data gaps in cache"""
        with sqlite3.connect(self.db_path) as conn:
            # Get all cached dates
            dates = pd.to_datetime([
                row[0] for row in conn.execute(
                    "SELECT date FROM stock_data WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
                    (ticker, start_date, end_date))
            ])

            if len(dates) < 2:
                return {"error": "Insufficient data"}

            # Generate complete business day range
            full_range = pd.date_range(start=dates.min(), end=dates.max(), freq='B')
            missing_dates = set(full_range) - set(dates)

            # Convert to DataFrames for analysis
            df_dates = pd.DataFrame({'date': dates, 'available': True})
            df_missing = pd.DataFrame({'date': sorted(missing_dates)})

            # Calculate gap statistics
            df_dates['day_diff'] = df_dates['date'].diff().dt.days.fillna(1)
            gaps = df_dates[df_dates['day_diff'] > 1].copy()
            gaps['gap_length'] = gaps['day_diff'] - 1  # Simple gap count

            # Weekend-adjusted gap analysis
            gaps['weekend_adjusted'] = gaps['day_diff'].apply(
                lambda x: max(0, (x - 2))  # Subtract 2 days for weekends
            )

            # Find missing date clusters
            df_missing['gap_group'] = (df_missing['date'].diff() > timedelta(days=3)).cumsum()
            gap_clusters = df_missing.groupby('gap_group').agg(
                start_date=('date', 'min'),
                end_date=('date', 'max'),
                missing_days=('date', 'count')
            ).reset_index(drop=True)

            # Holiday detection
            us_holidays = self.get_us_holidays()
            df_missing['is_holiday'] = df_missing['date'].isin(us_holidays)

            return {
                'summary_stats': {
                    'total_days': len(full_range),
                    'available_days': len(dates),
                    'missing_days': len(missing_dates),
                    'completeness_ratio': len(dates) / len(full_range),
                    'largest_gap': gaps['day_diff'].max(),
                    'largest_weekday_gap': gaps['weekend_adjusted'].max()
                },
                'gap_details': gaps.to_dict('records'),
                'missing_date_clusters': gap_clusters.to_dict('records'),
                'holiday_breakdown': {
                    'total_holidays': df_missing['is_holiday'].sum(),
                    'missing_on_holidays': df_missing[df_missing['is_holiday']].shape[0],
                    'unexplained_missing': df_missing[~df_missing['is_holiday']].shape[0]
                },
                'raw_missing_dates': [d.strftime('%Y-%m-%d') for d in sorted(missing_dates)]
            }

    def get_us_holidays(self, years=range(2000, 2026)):
        """Generate US market holidays"""
        holidays = []
        for year in years:
            # New Year's
            holidays.append(f"{year}-01-01")
            # MLK Day (3rd Monday)
            holidays.append(f"{year}-01-{15 + (pd.Timestamp(f'{year}-01-15').dayofweek == 0)}")
            # ... (add other holidays as shown in previous examples)
        return pd.to_datetime(holidays).date

    def visualize_gaps(self, ticker, start_date, end_date):
        """Visual representation of data gaps"""
        analysis = self.get_gap_analysis(ticker, start_date, end_date)

        if 'error' in analysis:
            print(analysis['error'])
            return

        # Timeline visualization
        fig = go.Figure()

        # Available dates
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(analysis['raw_missing_dates']),
            y=[1] * len(analysis['raw_missing_dates']),
            mode='markers',
            marker=dict(color='red', size=8),
            name='Missing Dates'
        ))

        # Add holiday markers
        holidays = [d for d in analysis['raw_missing_dates']
                    if d in self.get_us_holidays()]
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(holidays),
            y=[1.1] * len(holidays),
            mode='markers',
            marker=dict(color='orange', size=10, symbol='star'),
            name='Market Holidays'
        ))

        # Layout
        fig.update_layout(
            title=f"Data Gap Analysis for {ticker}",
            yaxis=dict(visible=False, range=[0.9, 1.2]),
            xaxis_title='Date',
            showlegend=True
        )

        fig.show()

        # Print summary
        print("\n=== Gap Analysis Summary ===")
        print(f"Data Completeness: {analysis['summary_stats']['completeness_ratio']:.1%}")
        print(f"Largest Gap: {analysis['summary_stats']['largest_gap']} days")
        print(f"Largest Weekday Gap: {analysis['summary_stats']['largest_weekday_gap']} days")
        print(f"\nTop Missing Date Clusters:")
        for cluster in sorted(analysis['missing_date_clusters'],
                              key=lambda x: x['missing_days'], reverse=True)[:3]:
            print(f"- {cluster['start_date']} to {cluster['end_date']}: {cluster['missing_days']} days")

        print("\nHoliday Analysis:")
        print(f"Total Market Holidays: {analysis['holiday_breakdown']['total_holidays']}")
        print(f"Missing on Holidays: {analysis['holiday_breakdown']['missing_on_holidays']}")
        print(f"Unexplained Missing Days: {analysis['holiday_breakdown']['unexplained_missing']}")