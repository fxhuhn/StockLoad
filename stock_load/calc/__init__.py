from .rsi import rsi
from .swings import swings
from .trading_range import trading_range

__all__ = ['rsi', 'swings', 'trading_range']

import pandas as pd

def ema(close : pd.Series, period : int = 200) -> pd.Series:
    return round(close.ewm(span=period, min_periods=period, adjust=False, ignore_na=False).mean(), 2)


def sma(close : pd.Series, period : int = 200) -> pd.Series:
    return round(close.rolling(period).mean(), 2)


def resample_week(df: pd.DataFrame) -> pd.DataFrame:
    # expected columns Date, Open, High, Low, Close    

    # TODO work with indices 
    df.reset_index(inplace=True)
    
    df.Date = df['Date'].astype('datetime64[ns]')    

    #note calendar week and year    
    df['week'] = df['Date'].dt.strftime("%y-%W")    
    
    return df.groupby("week").agg( 
                            Date=("Date","last"), 
                            Low=("Low", "min"), 
                            High=("High", "max"), 
                            Open=("Open", "first"), 
                            Close=("Close", "last"))
    