import redis
import pickle
import SADX as sadx
from datetime import datetime, timedelta
import order_ce 

r = redis.StrictRedis(host='localhost', port=6379, db=0)
open_column = 'O'
close_column = 'C'
momentum_column = 'Mom'
adx='ADX_14'
rsi='rsi'
timestamp_column = 'timestamp'



# def buy_signal_condition(latest_row, prev_latest_row):
#     print(f"Open: {latest_row[open_column]}, Close: {latest_row[close_column]}")
#     print(f"Momentum: {latest_row[momentum_column]}")
#     print(f"RSI Latest: {latest_row[rsi]}, RSI Prev: {prev_latest_row[rsi]}")
#     print(f"ADX: {latest_row[adx]}")
#     condition1 = latest_row[open_column] > latest_row[close_column]  # or < for PE
#     condition2 = -25 < latest_row[momentum_column] < 25
#     condition3 = (latest_row[rsi] > prev_latest_row[rsi])  # or < for PE
#     condition4 = latest_row[adx] > 25
#     print(f"Conditions: {condition1}, {condition2}, {condition3}, {condition4}")
#     return condition1 and condition2 and condition3 and condition4

def buy_signal_condition(latest_row, prev_latest_row):
    # TEMPORARILY force True for testing
    print("Buy signal condition forcibly set to True for testing.")
    return True


def log_buy_signal(price):
    order_ce.placeOrder(price)


def condition():
    global df
    retrived_df = r.get('data')
    df = pickle.loads(retrived_df)
    df['ADX_14'] = sadx.SADX(df)

    last_timestamp = df['timestamp'].iloc[-1]
    last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
    now = datetime.now()

    if now - last_timestamp_dt > timedelta(minutes=3):
        last_index = len(df) - 1
        prev_last_index = len(df) - 2
    else:
        last_index = len(df) - 2
        prev_last_index = len(df) - 3

    if buy_signal_condition(df.iloc[last_index], df.iloc[prev_last_index]):
        buy_signal_timestamp = df.iloc[last_index][timestamp_column]
        print(f"Buy signal timestamp: {buy_signal_timestamp}")
        log_buy_signal(df.iloc[last_index][close_column])
        order_ce.placeOrder(df.iloc[last_index][close_column])  # make sure this is your order placement function
    else:
        print("[CE] --> No buy signal conditions met.")