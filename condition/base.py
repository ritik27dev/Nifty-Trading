import redis
import pickle
from datetime import datetime, timedelta

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def load_dataframe_with_adx(sadx_module):
    try:
        data = r.get('data')
        if data is None:
            print("No data found in Redis.")
            return None
        df = pickle.loads(data)
        df['ADX_14'] = sadx_module.SADX(df)
        return df
    except Exception as e:
        print("Error loading data:", e)
        return None

def get_last_indices(df, timestamp_column='timestamp'):
    try:
        last_ts = df[timestamp_column].iloc[-1]
        last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Timestamp format error: {last_ts}")
        return None, None

    now = datetime.now()
    if now - last_dt > timedelta(minutes=3):
        return len(df) - 1, len(df) - 2
    else:
        return len(df) - 2, len(df) - 3
