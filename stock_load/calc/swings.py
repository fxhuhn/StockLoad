from scipy.signal import argrelextrema
import numpy as np
import pandas as pd


def swings(df:pd.DataFrame) -> pd.DataFrame:
    def signal_count(band: np):
        for i in range(2, len(band)):
            if np.isnan(band[i]):
                band[i] = band[i - 1]
        return band


    # local extrema
    df_swing_low = df.iloc[argrelextrema(data=df['Low'].values, comparator=np.less_equal)[0]][['Date', 'Low']]
    df_swing_high = df.iloc[argrelextrema(data=df['High'].values, comparator=np.greater_equal)[0]][['Date', 'High']]
    df_swings = pd.concat([df_swing_low, df_swing_high]).sort_values(by='Date', ascending=False)
    df_swings['prev_high'] = df_swings.shift(-1).High
    df_swings['prev_low'] = df_swings.shift(-1).Low
    df_swings['next_high'] = df_swings.shift(1).High
    df_swings['next_low'] = df_swings.shift(1).Low

    df_swings.loc[((df_swings.High.notna()) & (df_swings.next_high.notna())) & (df_swings.High <= df_swings.next_high), 'High'] = np.nan
    df_swings.loc[((df_swings.Low.notna()) & (df_swings.prev_low.notna())) & (df_swings.Low >= df_swings.prev_low), 'Low'] = np.nan
    df['swing_high_b'] = df.loc[df.Date.isin(df_swings[df_swings.High.notna()]['Date'].values)]['High']
    df['swing_low_b'] = df.loc[df.Date.isin(df_swings[df_swings.Low.notna()]['Date'].values)]['Low']

    df['swing_low'] = df.iloc[argrelextrema(data=df['Low'].values, comparator=np.less_equal, order=3)[0]]['Low']
    df['swing_high'] = df.iloc[argrelextrema(data=df['High'].values, comparator=np.greater_equal, order=3)[0]]['High']

    df['swing_low_count'] = signal_count(df['swing_low'].copy().to_numpy())
    df['swing_high_count'] = signal_count(df['swing_high'].copy().to_numpy())

    return df