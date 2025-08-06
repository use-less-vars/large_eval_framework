from . import trade_tracker as tt
from . import strategy as strat
from . import data_loader as dl
import pandas as pd
import numpy as np
from backtesting import Backtest


def plot_trade(trade_id, preceding_days: int, trailing_days: int):
    # Trade lookup and validation
    trade = tt.lookup_trade(trade_id)
    print("-------------------------------")
    print(f"Plotting trade: {trade}")
    print(f"Entry time: {trade.entry_time}")
    print(f"Exit time: {trade.exit_time}")

    # Date range calculation with validation
    start_date = trade.entry_time - pd.Timedelta(days=400)
    end_date = (trade.exit_time if trade.exit_time else pd.Timestamp.now()) + pd.Timedelta(days=trailing_days)
    print(f"Requested data range: {start_date} to {end_date}")

    # Data loading with debug output
    data_loader = dl.DataLoader()
    data = data_loader.fetch_data(trade.ticker, start_date, end_date)
    print("\nData validation:")
    print(f"Total rows: {len(data)}")
    print(f"First date: {data.index[0]}")
    print(f"Last date: {data.index[-1]}")
    print(f"Missing dates: {pd.date_range(start=data.index[0], end=data.index[-1]).difference(data.index)}")

    # Backtest execution
    storage = strat.StrategyResults()
    bt = Backtest(data, strat.DarvasJojo, commission=.002, exclusive_orders=True)
    bt.run(**trade.parameters, storage=storage)

    # Strategy results validation
    print("\nStrategy results validation:")
    print(f"Storage dates count: {len(storage.date)}")
    print(f"First strategy date: {storage.date[0]}")
    print(f"Last strategy date: {storage.date[-1]}")

    # Plot range calculation
    plot_start = trade.entry_time - pd.Timedelta(days=preceding_days)
    plot_end = (trade.exit_time if trade.exit_time else pd.Timestamp.now()) + pd.Timedelta(days=trailing_days)
    print(f"\nPlot range: {plot_start} to {plot_end}")

    # Verify data coverage for plotting
    if plot_start < data.index[0]:
        print(f"WARNING: Missing data before {data.index[0]} (need {plot_start})")
    if plot_end > data.index[-1]:
        print(f"WARNING: Missing data after {data.index[-1]} (need {plot_end})")

    # Actual plotting
    print("\nPlotting data points:")
    filtered_data = data.loc[plot_start:plot_end]
    print(f"Plotting {len(filtered_data)} bars from {filtered_data.index[0]} to {filtered_data.index[-1]}")

    strat.plot_trade(
        df=filtered_data,
        storage=storage,
        start_date=plot_start,
        end_date=plot_end
    )
    print("------------------------")