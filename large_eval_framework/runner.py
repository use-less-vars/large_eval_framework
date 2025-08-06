import pandas as pd
from . import trade_tracker as tt
from . import data_loader as dl
from . import strategy as strat
from backtesting import Backtest
from . import config


def run_strategy_on_tickers(csv_path = "good_tickers.csv", start_index = 0, run_again = True):
    tracker = tt.TradeTracker()
    loader = dl.DataLoader()

    #symbols = loader.get_all_symbols()
    #loader.filter_good_tickers(symbols)

    tickers_and_timespan = pd.read_csv(csv_path)

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
                if not run_again:
                    continue

            #fetch data
            data = loader.fetch_data(ticker, start_date, end_date)
            if data is None or data.empty:
                print(f"[SKIP] No data for {ticker} from {start_date} to {end_date}, skipping.")
                continue
            print(f"Current strategy: {strategy_id}, ticker: {ticker}")
            tracker.start_tracking(strategy_id, ticker, start_date, end_date, conf[strategy_id])
            bt = Backtest(data, strat.DarvasJojo, commission=.002, exclusive_orders=True)
            bt.run(**conf[strategy_id], trade_tracker = tracker, strategy_id = strategy_id)
            tracker.show()
            tracker.finalize_backtest_to_db()
            
        print(f"total trades made so far: {tracker.get_total_trades_made()}")
