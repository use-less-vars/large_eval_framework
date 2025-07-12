from datetime import datetime
import os
import json
from typing import List, Optional
from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd
import sqlite3


@dataclass
class TradeMetaData:
    strategy_id: str
    ticker: str
    start_date: str
    end_date: str
    parameters: Dict[str, Any]
    metrics: Dict[str, Any] = None


@dataclass
class Trade:
    def __init__(self, strategy_id, entry_time, entry_price  ):
        self.strategy_id = strategy_id
        #self.ticker = "Unknown"
        self.entry_time = entry_time
        self.exit_time = None
        self.entry_price = entry_price
        self.exit_price = None
        #quantity = size,
        self.pnl = None
        self.duration = None     #(self.exit_time - self.entry_time).days,
        #self.parameters = params,
        #regime = trade.regime,
        #tags = get_stock_tags(trade.ticker)
        print(f"New Trade started: {strategy_id} ")

    def close(self, exit_time, exit_price):
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.pnl = (self.exit_price-self.entry_price)/self.entry_price*100 #profit in percent
        self.duration = (self.exit_time-self.entry_time).days

    #def add_ticker(self, ticker = "Unknown"):
    #    self.ticker = ticker
    def to_dict(self):
        return {
            'strategy_id':self.strategy_id,
            'entry_time':self._convert_time(self.entry_time),
            'exit_time':self._convert_time(self.exit_time),
            'entry_price':float(self.entry_price),
            'exit_price':float(self.exit_price),
            'pnl': float(self.pnl),
            'duration': int(self.duration)
        }
    def _convert_time(self, time):
        if time is None:
            return None
        else:
            ret = time.isoformat()
        return ret

    def __repr__(self):
        """Machine-readable representation"""
        return (f"<Trade {self.strategy_id}||"
                f"IN:{self.entry_time.strftime('%Y-%m-%d')}@{self.entry_price:.2f}|"
                f"OUT:{self.exit_time.strftime('%Y-%m-%d') if self.exit_time else 'OPEN'}@{self.exit_price or 0:.2f}>")

class TradeTracker:
    def __init__(self, json_file = "backtest_results.json", db_path = "trades.db"):
        self.trades : List[Trade] = []
        self.current_trade = None
        self.metadata = None
        self.total_trades_made = 0
        self.json_file = json_file
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

        if not os.path.exists(self.json_file):
            with open(self.json_file, 'w') as f:
                json.dump({"backtests":[]},f)
            print("created JSON file to track trades")
        else:
            print("JSON file already exists, let's go")


    def _create_tables(self):
        """Create Database"""
        with self.conn:
            self.conn.executescript("""CREATE TABLE IF NOT EXISTS processed_tickers(
                strategy_id TEXT NOT NULL, 
                ticker TEXT NOT NULL,
                PRIMARY KEY (strategy_id, ticker)
            );
            
            CREATE TABLE IF NOT EXISTS backtests(
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                strategy_id TEXT NOT NULL, 
                ticker TEXT NOT NULL, 
                start_date TEXT NOT NULL, 
                end_date TEXT NOT NULL, 
                parameters TEXT NOT NULL, 
                FOREIGN KEY (strategy_id, ticker)
                    REFERENCES processed_tickers(strategy_id, ticker)
            );
            
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id INTEGER NOT NULL, 
                entry_time TEXT NOT NULL, 
                exit_time TEXT NOT NULL, 
                entry_price REAL NOT NULL, 
                exit_price REAL NOT NULL, 
                pnl REAL NOT NULL, 
                duration INTEGER NOT NULL, 
                FOREIGN KEY (backtest_id) REFERENCES backtests(id)
            );
            """)

    def finalize_backtest_to_db(self):
        """Save completed backtest results to db"""
        if not self.metadata:
            return
            #raise ValueError("No backtest to finalize")

        try:
            with self.conn:
                self.conn.execute("""
                    INSERT OR IGNORE INTO processed_tickers
                    (strategy_id, ticker) VALUES(?, ?)
                """, (self.metadata.strategy_id, self.metadata.ticker))

                cursor = self.conn.execute("""
                    INSERT INTO backtests
                    (strategy_id, ticker, start_date, end_date, parameters) VALUES(?,?,?,?,?)
                    RETURNING id
                    """, (self.metadata.strategy_id,
                          self.metadata.ticker,
                          self.metadata.start_date,
                          self.metadata.end_date,
                          json.dumps(self.metadata.parameters)))
                backtest_id = cursor.fetchone()[0]
                trade_data = [
                    (
                        backtest_id,
                        trade.entry_time.isoformat(),
                        trade.exit_time.isoformat(),
                        trade.entry_price,
                        trade.exit_price,
                        trade.pnl,
                        trade.duration
                    )
                    for trade in self.trades
                ]
                self.conn.executemany("""
                    INSERT INTO trades (backtest_id, entry_time, exit_time, entry_price, exit_price, pnl, duration)
                    VALUES(?,?,?,?,?,?,?)
                    """,trade_data)

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise
    def check_if_already_ran(self, strategy_id: str, ticker: str):
        """Check if ticker and strategy have already been run

            strategy_id: Which strategy (e.g. "Darvas_01")
            ticker: Which ticker(e.g. "MSFT")

            return: True if already processed, else false
            """
        try:
            cursor = self.conn.execute("""SELECT 1 FROM processed_tickers WHERE strategy_id = ? AND ticker = ?""",
                                       (strategy_id, ticker))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"Database error checking processed tickers: {e}")
            return False

    def start_tracking(self, strategy_id, ticker, start_date, end_date, params):
        self.metadata = TradeMetaData(strategy_id=strategy_id,
                                      ticker=ticker,
                                      start_date=start_date,
                                      end_date=end_date,
                                      parameters=params)
        self.current_trade = None
        self.trades.clear()

    def open_trade(self, strategy_id, entry_time, entry_price):
        if self.current_trade is None:
            self.current_trade = Trade(strategy_id, entry_time, entry_price)
        else:
            print("there is already a trade running")

    def close_trade(self, exit_time, exit_price):
        if self.current_trade is not None:
            self.current_trade.close(exit_time, exit_price)
            self.trades.append(self.current_trade)
            self.current_trade = None
            self.total_trades_made+=1
        else:
            print("there is not open trade to close")


    def append_trades_to_json(self):
        if self.metadata is None:
            raise ValueError("Set Metadata first")
        data = {
            "metadata": {
                **self.metadata.__dict__,
            },
            "trades":[t.to_dict() for t in self.trades]
        }
        with open(self.json_file, 'r') as f:
            all_data = json.load(f)

        all_data["backtests"].append(data)

        with open(self.json_file, 'w') as f:
            json.dump(all_data, f, indent=2)

        print(f"Appended {len(self.trades)} Trades to {self.json_file}")



    def get_total_trades_made(self):
        return self.total_trades_made
    def json_to_dataframe(self, json_path) -> pd.DataFrame:
        with open(json_path, 'r') as f:
            data = json.load(f)
        rows = []
        for run in data["backtests"]:
            for trade in run["trades"]:
                row = {
                    **trade,
                    **{f"meta_{k}":v for k,v in run["metadata"].items()}
                       }
                rows.append(row)

        return pd.DataFrame(rows)

    def append_trades_to_df(self):
        pass



    def load_from_db(self, path):
        # load db
        pass

    def strategy_already_done(self):
        #check if trades with same strategy_id, ticker, timespan has been run already
        pass

    def save_to_db(self):
        #somehow append self.trades to db
        pass

    def show(self):
        print("Trades made:")
        for i, trade in enumerate(self.trades, 1):
            print(f"{i}. {trade}")
