import redis
import pickle
import SADX as sadx
from datetime import datetime, timedelta
import order_pe 


r = redis.StrictRedis(host='localhost', port=6379, db=0)
open_column = 'O'
close_column = 'C'
momentum_column = 'Mom'
adx='ADX_14'
rsi='rsi'
timestamp_column = 'timestamp'


def buy_signal_condition(latest_row, prev_latest_row):
    print(f"Open: {latest_row[open_column]}, Close: {latest_row[close_column]}")
    print(f"Momentum: {latest_row[momentum_column]}")
    print(f"RSI Latest: {latest_row[rsi]}, RSI Prev: {prev_latest_row[rsi]}")
    print(f"ADX: {latest_row[adx]}")
    condition1 = latest_row[open_column] > latest_row[close_column]  # or < for PE
    condition2 = -25 < latest_row[momentum_column] < 25
    condition3 = (latest_row[rsi] > prev_latest_row[rsi])  # or < for PE
    condition4 = latest_row[adx] > 25
    print(f"Conditions: {condition1}, {condition2}, {condition3}, {condition4}")
    return condition1 and condition2 and condition3 and condition4



def log_buy_signal(price):
        order_pe.placeOrder(price)



def condition():
    global df
    retrived_df = r.get('data')
    df = pickle.loads(retrived_df)
    df['ADX_14'] = sadx.SADX(df)
    
    last_timestamp = df['timestamp'].iloc[-1]
    last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
    # print(last_timestamp_dt)
    #Get current time
    now = datetime.now()
    # Check if last timestamp is older than current time by more than 3 minutes
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
        # subprocess.run(["python","order2.py"])
        order_pe.placeOrder(df.iloc[last_index][close_column])
    else:
        print("[PE] --> No buy signal conditions met.")