from backtesting import Backtest, Strategy
import matplotlib.pyplot as plt
from backtesting.lib import crossover
import numpy as np
import pandas as pd
import talib
import trade_tracker

from datetime import timedelta
from bokeh.plotting import figure, show
from bokeh.models import BoxAnnotation

#1. Darvas Strategy

STATE_MAP = {
    "NO_BOX":0,
    "NEW_BOX":1,
    "BOX_FORMING":2,
    "IN_BOX":3,
    "BOX_CANCELED":4
}


class StrategyResults:
    def __init__(self):
        self.date = None
        self.high_bounds = None
        self.low_bounds = None
        self.box_status = None
        self.stop_values = None

REVERSE_STATE_MAP ={v:k for k,v in STATE_MAP.items() }

def darvas_boxes(high, low, volume, lookback_period=252, box_period=3,
                 volume_lookback=20):
    high_bounds = np.full_like(high, 0)
    low_bounds = np.full_like(low, 0)
    ma_volume = np.full_like(volume, 0)
    box_status = np.full(len(volume), "NO_BOX", dtype = '<U20')
    remaining_day_forming = box_period
    high_dbg = 0
    low_dbg = 0
    state_dbg = "NO_BOX"
    for i in range(volume_lookback - 1, len(volume)):
        ma_volume[i] = np.mean(volume[i - volume_lookback + 1: i + 1])

    for i in range(lookback_period, len(high)):
        box_status[i] = box_status[i - 1]
        high_bounds[i] = high_bounds[i - 1]
        low_bounds[i] = low_bounds[i - 1]
        upward_days = 0
        high_dbg = high[i]
        low_dbg = low[i]
        state_dbg = box_status[i]

        if high[i] == max(high[i - lookback_period:i + 1]):
            high_bounds[i] = high[i]
            low_bounds[i] = low[i]
            remaining_day_forming = box_period
            box_status[i] = "NEW_BOX"
            continue

        if box_status[i] == "BOX_FORMING" or  box_status[i] == "NEW_BOX":
            if low_bounds[i] > low[i]:
                low_bounds[i] = low[i]
                remaining_day_forming = box_period
            else:
                remaining_day_forming -= 1

            box_status[i] = "BOX_FORMING" if remaining_day_forming > 0 else "IN_BOX"
            continue

        if box_status[i] == "IN_BOX":
            box_status[i] = "BOX_CANCELED" if low[i] < low_bounds[i] else "IN_BOX"
            continue

        if box_status[i] == "BOX_CANCELED":
            box_status[i] = "NO_BOX"
            high_bounds[i] = 0
            low_bounds[i] = 0

    numeric_status = np.array([STATE_MAP[s] for s in box_status])

    return high_bounds, low_bounds, numeric_status, ma_volume

class DarvasJojo(Strategy):
    strategy_id : str= "Default Params"
    volume_multiplier : float = 3
    lookback_period : int = 252
    box_period : int = 3
    volume_lookback : int= 20
    atr_factor : float = 3
    trade_tracker : 'trade_tracker.TradeTracker' = None
    storage :'StrategyResults' = None

    def init(self):

        self.hb, self.lb, self.status, self.ma_vol = self.I(darvas_boxes,  self.data.High, self.data.Low, self.data.Volume,
                                                            self.lookback_period, self.box_period,
                                                            self.volume_lookback,
                                                            plot=True, overlay= False)
        self.entry_price = 0.0
        self.stop_val_arr = np.full(len(self.hb), 0, dtype=np.float64)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, timeperiod=14)
        self.last_day = self.data.df.index[-1]
        print("last day = ", self.last_day)

    def next(self):
        current_idx = len(self.data) - 1
        #print(self.status[-1], self.hb[-1], self.lb[-1], end="\n")
        if not self.position and self.status[-2] == STATE_MAP["IN_BOX"]:
            if self.data.Volume[-1] >= self.volume_multiplier * self.ma_vol[-2]:
                if self.data.High[-1] >= self.hb[-2]:
                    self.custom_buy()
                    self.stop_price = (self.hb[-2] + self.lb[-2])/2

        elif self.position:
            # Simple volatility-adjusted trailing stop
            self.stop_val = max(self.stop_price, float(self.data.High[-1])-self.atr_factor  * float(self.atr[-1]))
            self.stop_val_arr[current_idx] = self.stop_val
            # Never lower the stop
            #self.stop_val = max(self.stop_val, self.entry_price * 0.93)  # Floor at 7%

            # Exit condition
            if self.data.Close[-1] < self.stop_val or self.data.df.index[-1] == self.last_day:
                self.custom_close()
        if self.data.df.index[-1] == self.last_day:
            self.finalize()


    def custom_buy(self):
        self.buy()
        if self.trade_tracker is not None:
            #print("OPEN TRADE")
            self.trade_tracker.open_trade(self.strategy_id,self.data.index[-1],self.data.Open[-1])

    def custom_close(self):
        self.position.close()
        if self.trade_tracker is not None:
            #print("CLOSE TRADE")
            self.trade_tracker.close_trade(self.data.index[-1], self.data.Close[-1])

    def finalize(self):
        if self.storage:
            self.storage.date = self.data.df.index
            self.storage.high_bounds = self.hb
            self.storage.low_bounds = self.lb
            self.storage.box_status = self.status
            self.storage.stop_values = self.stop_val_arr

def plot_trade(df: pd.DataFrame, storage: StrategyResults = None, start_date = None, end_date = None):
    plot_df = df.copy()
    print(len(plot_df))
    if not isinstance(plot_df.index, pd.DatetimeIndex):
        plot_df.index = pd.to_datetime(plot_df.index)

    if start_date and end_date:
        plot_df = plot_df.loc[start_date:end_date]

    plot_df["date"] = plot_df.index

    inc = plot_df.Close > plot_df.Open
    dec = plot_df.Open > plot_df.Close
    w = 16 * 60 * 60 * 1000  # milliseconds

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    p = figure(x_axis_type="datetime", tools=TOOLS, width=1400, height=700,
               title=" Candlestick", background_fill_color="#efefef")

    # Add to Bokeh plot


    p.xaxis.major_label_orientation = 0.8  # radians

    p.segment(plot_df.date, plot_df.High, plot_df.date, plot_df.Low, color="black")

    p.vbar(plot_df.date[dec], w, plot_df.Open[dec], plot_df.Close[dec], color="#eb3c40")
    p.vbar(plot_df.date[inc], w, plot_df.Open[inc], plot_df.Close[inc], fill_color="green",
           line_color="#1c7c55", line_width=2)
    # Convert dates to pandas Timestamp (safe conversion)
    start_date = pd.to_datetime(start_date) if start_date is not None else None
    end_date = pd.to_datetime(end_date) if end_date is not None else None

    # Filter price data (safe with None dates)

    # Get all boxes (your existing box creation method)

    boxes = get_boxes(storage)

    # Add only boxes fully contained in time frame
    for box in boxes:
        box_start = pd.to_datetime(box['start_date'])
        box_end = pd.to_datetime(box['end_date'])

        # Check if box is fully within time frame (handles None dates)
        if ((start_date is None or box_start >= start_date) and
                (end_date is None or box_end <= end_date)):
            p.add_layout(BoxAnnotation(
                left=box['start_date']-timedelta(hours=12),
                right=box['end_date']+timedelta(hours=12),
                bottom=box['low_val']*0.999,
                top=box['high_val']*1.001,
                fill_alpha=0.1,
                fill_color="navy",
                line_color="blue",
                line_width=0.5
            ))

    # First ensure we have datetime indices
    stop_dates = pd.to_datetime(storage.date)
    stop_values = pd.Series(storage.stop_values, index=stop_dates)

    # Filter stop values to match plot_df's range
    if start_date or end_date:
        mask = (stop_dates >= pd.to_datetime(start_date)) if start_date else True
        mask &= (stop_dates <= pd.to_datetime(end_date)) if end_date else True
        stop_values = stop_values[mask]

    # Now plot with aligned data
    p.line(stop_values.index, stop_values.values,
           line_width=2, color="red",
           legend_label="Stop Loss",
           line_alpha=0.8)
    show(p)


def get_boxes(storage: StrategyResults):
    boxes = []
    box = None
    for idx in range(1,len(storage.date)):
        if storage.box_status[idx-1] == STATE_MAP["NEW_BOX"] and storage.box_status[idx] == STATE_MAP['BOX_FORMING']:
            box = {
                'start_date': storage.date[idx-1],
                'end_date': None,
                'high_val': storage.high_bounds[idx-1],
                'low_val': None,
            }
        if storage.box_status[idx-1] == STATE_MAP['IN_BOX'] and storage.box_status[idx] != STATE_MAP['IN_BOX']:
            box['end_date'] = storage.date[idx-1]
            box['low_val'] = storage.low_bounds[idx-1]
            boxes.append(box)
            box = None

    return boxes




def plot_indicator(high,low, box_status,high_bounds,low_bounds):
    import matplotlib.pyplot as plt
    # 1. Basic Setup
    plt.figure(figsize=(15, 8))
    plt.title('Darvas Box Visualization')
    plt.xlabel('Days')
    plt.ylabel('Price')
    box_formation_counter = 0
    # 2. Plot Price Range (like your existing code)
    for i, (h, l) in enumerate(zip(high, low)):
        plt.plot([i, i], [l, h], color='grey', linewidth=4)  # Thin lines for price range

        color_map = {0:'grey', 1:'green', 2:'lime', 3:'blue', 4:'red'}
        plt.plot([i,i], [0,box_status[i]], color=color_map[box_status[i]], linewidth=4 )
    # 3. Highlight Boxes
    for i in range(len(box_status)):
        if box_status[i] == STATE_MAP["NEW_BOX"]:
            box_formation_counter = 1
            continue
        if box_status[i] == STATE_MAP["BOX_FORMING"]:
            box_formation_counter += 1
            continue
        if box_status[i] == STATE_MAP["BOX_CANCELED"]:
            box_formation_counter = 0
            continue
        if box_status[i] == STATE_MAP["IN_BOX"]:
            # Draw horizontal lines for box boundaries
            plt.hlines(y=high_bounds[i], xmin=i - box_formation_counter, xmax=i + 0.5,
                       colors='pink', linestyles='solid', linewidth=2)
            plt.hlines(y=low_bounds[i], xmin=i - box_formation_counter, xmax=i + 0.5,
                       colors='pink', linestyles='solid', linewidth=2)
            # Optional: Fill the box
            plt.fill_between([i - 0.5, i + 0.5],
                             low_bounds[i], high_bounds[i],
                             color='pink', alpha=0.2)


    # 4. Mark Breakouts

    # 5. Finalize
    plt.gcf().canvas.toolbar.pan()  # Pan tool
    plt.gcf().canvas.toolbar.zoom()
    plt.grid(True)
    plt.show()

    #2. SMA Strategy

    class SMACrossover(Strategy):
        n1 = 5  # fast moving average
        n2 = 20  # slow moving average

        def init(self):
            print("Listen up motherfuckers!", self.data.Close, self.data.Volume)
            self.sma1 = self.I(SMA, self.data.Close, self.n1)
            self.sma2 = self.I(SMA, self.data.Close, self.n2)

        def next(self):
            if crossover(self.sma1, self.sma2):
                self.buy()
            elif crossover(self.sma2, self.sma1):
                self.sell()

    def SMA(values, n):
        print(pd.Series(values).rolling(n).mean())
        return pd.Series(values).rolling(n).mean()
