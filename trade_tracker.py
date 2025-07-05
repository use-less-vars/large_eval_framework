from datetime import datetime
from typing import List, Optional


class Trade:
    def __init__(self, strategy_id, entry_time, entry_price  ):
        self.strategy_id = strategy_id
        self.ticker = "Unknown"
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

    def close(self, exit_time, exit_price):
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.pnl = (self.exit_price-self.entry_price)/self.entry_price*100 #profit in percent
        self.duration = (self.exit_time-self.entry_time).days
    def add_ticker(self, ticker = "Unknown"):
        self.ticker = ticker

    def __repr__(self):
        """Machine-readable representation"""
        return (f"<Trade {self.strategy_id}|{self.ticker}|"
                f"IN:{self.entry_time.strftime('%Y-%m-%d')}@{self.entry_price:.2f}|"
                f"OUT:{self.exit_time.strftime('%Y-%m-%d') if self.exit_time else 'OPEN'}@{self.exit_price or 0:.2f}>")

class TradeTracker:
    def __init__(self):
        self.trades : List[Trade] = []
        self.current_trade = None
        self.db = None
        print("TradeTracker is there")

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
        else:
            print("there is not open trade to close")
    def add_ticker_to_trades(self,ticker = "Unknown"):
        for t in self.trades:
            t.add_ticker(ticker)

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
