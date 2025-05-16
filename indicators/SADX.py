
import pandas as pd
import numpy as np

def calculate_sadx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    required_cols = {'O', 'H', 'L', 'C', 'timestamp'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Input DataFrame must contain columns: {required_cols}")

    df = df.copy()
    df['Prev Close'] = df['C'].shift(1)
    df['TR'] = np.maximum(df['H'] - df['L'],
                          np.maximum(abs(df['H'] - df['Prev Close']),
                                     abs(df['L'] - df['Prev Close'])))

    df['Prev High'] = df['H'].shift(1)
    df['Prev Low'] = df['L'].shift(1)

    df['+DM'] = np.where(df['H'] - df['Prev High'] > df['Prev Low'] - df['L'],
                         np.maximum(df['H'] - df['Prev High'], 0), 0)
    df['-DM'] = np.where(df['Prev Low'] - df['L'] > df['H'] - df['Prev High'],
                         np.maximum(df['Prev Low'] - df['L'], 0), 0)

    df['SATR'] = df['TR'].rolling(window=period).mean()
    df['+SADM'] = df['+DM'].rolling(window=period).mean()
    df['-SADM'] = df['-DM'].rolling(window=period).mean()

    for i in range(period, len(df)):
        df.loc[i, 'SATR'] = (df.loc[i-1, 'SATR'] * (period - 1) + df.loc[i, 'TR']) / period
        df.loc[i, '+SADM'] = (df.loc[i-1, '+SADM'] * (period - 1) + df.loc[i, '+DM']) / period
        df.loc[i, '-SADM'] = (df.loc[i-1, '-SADM'] * (period - 1) + df.loc[i, '-DM']) / period

    df['+DI'] = (df['+SADM'] / df['SATR']) * 100
    df['-DI'] = (df['-SADM'] / df['SATR']) * 100
    df['DX'] = (np.abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])) * 100
    df['ADX'] = df['DX'].rolling(window=period).mean()

    df['SADX'] = np.nan
    df.loc[period-1, 'SADX'] = df['DX'].iloc[:period].mean()

    for i in range(period, len(df)):
        df.loc[i, 'SADX'] = (df.loc[i-1, 'SADX'] * (period - 1) + df.loc[i, 'DX']) / period

    return df[['timestamp', 'SADX']]