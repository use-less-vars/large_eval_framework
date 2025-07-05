from backtesting import Backtest, Strategy
import matplotlib.pyplot as plt
from backtesting.lib import crossover
import numpy as np
import pandas as pd
import talib
import trade_tracker


#1. Darvas Strategy

STATE_MAP = {
    "NO_BOX":0,
    "NEW_BOX":1,
    "BOX_FORMING":2,
    "IN_BOX":3,
    "BOX_CANCELED":4
}

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

    def init(self):

        self.hb, self.lb, self.status, self.ma_vol = self.I(darvas_boxes,  self.data.High, self.data.Low, self.data.Volume,
                                                            self.lookback_period, self.box_period,
                                                            self.volume_lookback,
                                                            plot=True, overlay= False)
        self.entry_price = 0.0
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, timeperiod=14)
        self.last_day = self.data.df.index[-2]
        print("last day = ", self.last_day)
    def next(self):
        #print(self.status[-1], self.hb[-1], self.lb[-1], end="\n")
        if not self.position and self.status[-2] == STATE_MAP["IN_BOX"]:
            if self.data.Volume[-1] >= self.volume_multiplier * self.ma_vol[-2]:
                if self.data.High[-1] >= self.hb[-2]:
                    #self.buy()
                    self.custom_buy()
                    print(f"bought at {self.data.index[-1]} ")
                    self.stop_price = (self.hb[-2] + self.lb[-2])/2
                    #self.stop_price = self.hb[-2]-1.5*self.atr[-1]
                    self.stop_price += 0.01 #debug
        elif self.position:
            # Simple volatility-adjusted trailing stop

            self.stop_val = max(self.stop_price, self.data.High[-1] - self.atr_factor  * self.atr[-1])

            # Never lower the stop
            self.stop_val = max(self.stop_val, self.entry_price * 0.93)  # Floor at 7%

            # Exit condition
            if self.data.Close[-1] < self.stop_val or self.data.df.index[-1] == self.last_day:
                #self.position.close()
                self.custom_close()
                print(f"Exited at data {self.data.index[-1]} at {self.data.Close[-1]:.2f}, Stop: {self.stop_val:.2f}")
        #if self.data.Low[-1] < self.current_stop:
          #  self.position.close()

    def custom_buy(self):
        self.buy()
        if self.trade_tracker is not None:
            print("OPEN TRADE")
            self.trade_tracker.open_trade(self.strategy_id,self.data.index[-1],self.data.Open[-1])

    def custom_close(self):
        self.position.close()
        if self.trade_tracker is not None:
            print("CLOSE TRADE")
            self.trade_tracker.close_trade(self.data.index[-1], self.data.Close[-1])


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
