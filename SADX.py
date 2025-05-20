import pandas as pd
import numpy as np

# Read the CSV file
# df = pd.read_csv('pure_ce.csv')
def SADX(df):
    # Column names
    low_column = 'L'
    high_column = 'H'
    close_column = 'C'
    open_column = 'O'
    timestamp_column = 'timestamp'

    # Period for calculation
    period = 14

    # Calculate True Range (TR)
    df['Prev Close'] = df[close_column].shift(1)
    df['TR'] = np.maximum(df[high_column] - df[low_column], 
                        np.maximum(abs(df[high_column] - df[close_column].shift(1)), 
                                    abs(df[low_column] - df[close_column].shift(1))))

    # Calculate +DM and -DM (Directional Movement Indicators)
    df['Prev High'] = df[high_column].shift(1)
    df['Prev Low'] = df[low_column].shift(1)

    df['+DM'] = np.where(df[high_column] - df['Prev High'] > df['Prev Low'] - df[low_column],
                        np.maximum(df[high_column] - df['Prev High'], 0), 0)

    df['-DM'] = np.where(df['Prev Low'] - df[low_column] > df[high_column] - df['Prev High'],
                        np.maximum(df['Prev Low'] - df[low_column], 0), 0)

    # Calculate True Range (TR) and ATR (Average True Range) - Rolling mean over the period
    df['ATR'] = df['TR'].rolling(window=period).mean()

    # Initialize SATR, +SADM, and -SADM columns with NaN
    df['SATR'] = np.nan
    df['+SADM'] = np.nan
    df['-SADM'] = np.nan

    # Calculate the first SATR, +SADM, and -SADM manually based on the first 'period' values
    df.loc[period-1, 'SATR'] = np.mean(df['TR'].iloc[:period])  # First SATR value
    df.loc[period-1, '+SADM'] = np.mean(df['+DM'].iloc[:period])  # First +SADM value
    df.loc[period-1, '-SADM'] = np.mean(df['-DM'].iloc[:period])  # First -SADM value

    # Apply incremental smoothing for SATR, +SADM, and -SADM for subsequent values
    for i in range(period, len(df)):
        df.loc[i, 'SATR'] = (df.loc[i-1, 'SATR'] * (period - 1) + df.loc[i, 'TR']) / period
        df.loc[i, '+SADM'] = (df.loc[i-1, '+SADM'] * (period - 1) + df.loc[i, '+DM']) / period
        df.loc[i, '-SADM'] = (df.loc[i-1, '-SADM'] * (period - 1) + df.loc[i, '-DM']) / period

    # Calculate +DI and -DI (Directional Indicators)
    df['+DI'] = (df['+SADM'] / df['SATR']) * 100
    df['-DI'] = (df['-SADM'] / df['SATR']) * 100

    # Calculate DX (Directional Index)
    df['DX'] = (np.abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])) * 100

    # Calculate ADX (Average Directional Index)
    df['ADX'] = df['DX'].rolling(window=period).mean()

    # Calculate SADX (Smoothed ADX)
    df['SADX'] = np.nan

    # Calculate the first SADX manually based on the first 'period' values
    df.loc[period-1, 'SADX'] = np.mean(df['DX'].iloc[:period])  # First SATR value

    # Apply incremental smoothing for SATR, +SADM, and -SADM for subsequent values
    for i in range(period, len(df)):
        df.loc[i, 'SADX'] = (df.loc[i-1, 'SADX'] * (period - 1) + df.loc[i, 'DX']) / period
    return df['SADX']
# Print the relevant columns
# print(df[[timestamp_column, 'TR', 'ATR', 'SATR', 'ADX', 'SADX', '+SADM', '-SADM']])
