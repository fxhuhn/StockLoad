import pandas as pd

def rsi(close : pd.Series, period : int = 7) -> pd.DataFrame:
    # Get rid of the first row, which is NaN since it did not have a previous
    # row to calculate the differences
    delta = close.diff(1)

    # Make the positive gains (up) and negative gains (down) Series
    up = delta.where(delta > 0, 0.0)
    down = -delta.where(delta < 0, 0.0)

    # Calculate the EWMA
    roll_up = up.ewm(min_periods=period, adjust=False, alpha=(1/period)).mean()
    roll_down = down.ewm(min_periods=period, adjust=False, alpha=1/period).mean()

    # Calculate the RSI based on EWMA
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))

    return round(rsi, 0)