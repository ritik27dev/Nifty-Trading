from condition import base
from indicators import sadx
from strategies import order_ce as ord

def buy_signal_condition(row, prev_row):
    return (
        row['O'] > row['C'] and
        -25 < row['Mom'] < 25 and
        row['rsi'] > prev_row['rsi'] and
        row['ADX_14'] > 25
    )

def run():
    df = base.load_dataframe_with_adx(sadx)
    if df is None:
        return

    last_idx, prev_idx = base.get_last_indices(df)
    if last_idx is None:
        return

    latest, previous = df.iloc[last_idx], df.iloc[prev_idx]

    print(f"[CE] Checking buy signal at: {latest['timestamp']}")
    print("Latest:", latest)
    print("Previous:", previous)

    if buy_signal_condition(latest, previous):
        print(f"[CE] Buy signal at {latest['timestamp']}")
        ord.main(latest['C'])
        ord.placeOrder()
    else:
        print("[CE] No buy signal conditions met.")
