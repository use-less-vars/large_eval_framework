import pandas as pd
from backtesting import Backtest
import visualization as vis

import data_loader as dl
import trade_tracker as tt

import talib
import strategy as strat

import config





if __name__ == "__main__":

    tracker = tt.TradeTracker()
    loader = dl.DataLoader()

    symbols = loader.get_all_symbols()
    loader.filter_good_tickers(symbols)

    tickers_and_timespan = pd.read_csv("good_tickers.csv")

    start_index = 0#not always start from the beginning.
    for index, row in tickers_and_timespan.iloc[start_index:].iterrows():
        ticker = row['ticker']
        start_date = row['start_date']
        end_date = row['end_date']
        if row['duration_days'] < 300:
            continue

        print(f"Processing {ticker} from {start_date} to {end_date}, ticker number is {index}")


        conf = config.load_strategy_params("darvas_config.json")
        for strategy_id in conf.keys():

            if tracker.check_if_already_ran(strategy_id, ticker):
                print(f"combo of strategy {strategy_id} and ticker {ticker} already ran")
                #continue

            #if combo has not been run, fetch data
            data = loader.fetch_data(ticker, start_date, end_date)
            storage = strat.StrategyResults()
            print(f"Current strategy: {strategy_id}, ticker: {ticker}")
            tracker.start_tracking(strategy_id, ticker, start_date, end_date, conf[strategy_id])
            bt = Backtest(data, strat.DarvasJojo, commission=.002, exclusive_orders=True)
            stats = bt.run(**conf[strategy_id], trade_tracker = tracker, strategy_id = strategy_id, storage = storage)
            #print(stats)

            print(conf[strategy_id])
            print(len(storage.stop_values),len(storage.low_bounds),len(storage.date),len(storage.box_status))
            tracker.show()
            tracker.finalize_backtest_to_db()
            if ticker == "MODG" and strategy_id == "Darvas_01":
                #bt.plot()
                for index in range(95,103):
                    vis.plot_trade(index,30,5)
                #strat.plot_trade(data,storage,"2018-02-01", "2018-12-25")
        #bt.plot()
            trade = tt.lookup_trade(99)
            print(trade)
        print(f"total trades made so far: {tracker.get_total_trades_made()}")

    #strat.plot_indicator(numpy_highs, numpy_lows, state, hb, lb )
    numpy_highs = data['High'].values
    numpy_lows = data['Low'].values
    numpy_volume = data['Volume'].values
    hb,lb,state,avg_vol = strat.darvas_boxes(numpy_highs, numpy_lows, numpy_volume)
    print([strat.REVERSE_STATE_MAP[s] for s in state[190:202]], hb[190:202], numpy_highs[190:202])

