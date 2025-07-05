
from backtesting import Backtest


import data_loader as dl
import trade_tracker as tt

import talib
import strategy as strat

import config



"""
high = np.array([1, 2, 3, 5, 9, 6, 7, 8, 8, 10, 9, 8, 9, 10, 9, 9, 12, 10,  9, 9, 8, 9])
low  = np.array([0, 1, 2, 4, 4, 5, 6, 7, 6,  9, 8, 7,  8,  7, 8, 8, 10,  8,  7, 7, 6, 7])
vol  = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 1, 2,  2,  3, 3, 3,  5,  5,  5, 5, 4, 6])

high_bounds, low_bounds, box_status, ma_volume = darvas_boxes(high, low, vol, box_period = formation_window, lookback_period=4, volume_lookback=5)
for i in range(0, len(high_bounds)):
    print(i,box_status[i],low_bounds[i],high_bounds[i])

import matplotlib.pyplot as plt
import numpy as np

#sys.exit()
"""


if __name__ == "__main__":
    # Fetch data
    ticker = 'NVDA'
    start_date = '2003-7-01'
    end_date = '2024-08-01'
    print(talib.get_functions())
    data = dl.fetch_data(ticker, start_date, end_date)
    #gute Zeiträume:  start_date = '2009-02-01' bis '2010-04-01'
    # auch 10-11
    # seltsam später stop-loss 2018-7 bis 2019-08 (50 Tage hoch einstellen)

    # Verify data
    print("Data columns:", data.columns)
    print("Data types:", data.dtypes)
    print("Data head:\n", data.head())

    # Run backtest

    #timeseries = data['Volume']
    #print(timeseries)
    #timeseries.plot(figsize=(10, 6), title='Trading Volume Over Time', ylabel='Volume')
    #plt.show()

    tracker = tt.TradeTracker()

    conf = config.load_strategy_params("darvas_config.json", "Darvas")

    bt = Backtest(data, strat.DarvasJojo, commission=.002, exclusive_orders=True)
    stats = bt.run(**conf["Darvas_01"], trade_tracker = tracker, strategy_id = "Darvas_01")

    tracker.add_ticker_to_trades(ticker)
    tracker.show()
    print(f"listen up you motherfuckers")
    print(stats)
    #bt.plot()


    #strat.plot_indicator(numpy_highs, numpy_lows, state, hb, lb )
    numpy_highs = data['High'].values
    numpy_lows = data['Low'].values
    numpy_volume = data['Volume'].values
    hb,lb,state,avg_vol = strat.darvas_boxes(numpy_highs, numpy_lows, numpy_volume)
    print([strat.REVERSE_STATE_MAP[s] for s in state[190:202]], hb[190:202], numpy_highs[190:202])
