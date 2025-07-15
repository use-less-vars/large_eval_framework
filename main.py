import pandas as pd
from backtesting import Backtest


import data_loader as dl
import trade_tracker as tt

import talib
import strategy as strat

import config





if __name__ == "__main__":

    tracker = tt.TradeTracker()
    loader = dl.DataLoader()

    tickers_and_timespan = pd.read_csv("enhanced_tickers.csv")
    print(tickers_and_timespan.head())
    start_index = 2579 #not always start from the beginning.
    for index, row in tickers_and_timespan.iloc[start_index:].iterrows():
        ticker = row['ticker']
        start_date = row['first_date']
        end_date = row['last_date']
        if row['duration_days'] < 300:
            continue

        print(f"Processing {ticker} from {start_date} to {end_date}, ticker number is {index}")


        conf = config.load_strategy_params("darvas_config.json", "Darvas")
        for strategy_id in conf.keys():

            if tracker.check_if_already_ran(strategy_id, ticker):
                print(f"combo of strategy {strategy_id} and ticker {ticker} already ran")
                #continue

            #if combo has not been run, fetch data
            data = loader.fetch_data(ticker, start_date, end_date)

            print(f"Current strategy: {strategy_id}, ticker: {ticker}")
            tracker.start_tracking(strategy_id, ticker, start_date, end_date, conf[strategy_id])
            bt = Backtest(data, strat.DarvasJojo, commission=.002, exclusive_orders=True)
            stats = bt.run(**conf[strategy_id], trade_tracker = tracker, strategy_id = strategy_id)
            print(stats)
            tracker.show()
            tracker.finalize_backtest_to_db()
            if ticker == "NVDA":
                bt.plot()
        #bt.plot()
        print(f"total trades made so far: {tracker.get_total_trades_made()}")

    #strat.plot_indicator(numpy_highs, numpy_lows, state, hb, lb )
    numpy_highs = data['High'].values
    numpy_lows = data['Low'].values
    numpy_volume = data['Volume'].values
    hb,lb,state,avg_vol = strat.darvas_boxes(numpy_highs, numpy_lows, numpy_volume)
    print([strat.REVERSE_STATE_MAP[s] for s in state[190:202]], hb[190:202], numpy_highs[190:202])
