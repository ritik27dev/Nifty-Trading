# a.py
import importlib
import subprocess
from SmartApi import SmartConnect
import pandas as pd
import pyotp
import time
import requests
from datetime import timedelta, datetime
import pandas_ta as ta
import threading
import pandas_market_calendars as mcal
import redis
import pickle

obj = None
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def start_redis_server():
    try:
        result = subprocess.run(["redis-cli", "ping"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.stdout.strip() == "PONG":
            print("Redis server is already running.")
        else:
            raise Exception("Redis server not running.")
    except Exception as e:
        print("Starting Redis server...")
        subprocess.Popen(["redis-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def init_smartconnect(api_key, client_id, password, totp_key):
    global obj
    obj = SmartConnect(api_key=api_key)
    data = obj.generateSession(client_id, password, pyotp.TOTP(totp_key).now())
    return data

def getLtp():
    Ltp = obj.ltpData('NSE', 'Nifty 50', 99926000)
    return Ltp['data']['ltp']

def storeTokens(real_date):
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    d = requests.get(url).json()
    token_df = pd.DataFrame.from_dict(d)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'], format="mixed").apply(lambda x: x.date())
    token_df = token_df.astype({'strike': float})    
    Ltp = int(getLtp())
    Ltp = int(Ltp / 100) * 100 + 50
    strikes = [Ltp + (i * 50) for i in range(-10, 11)]

    for strike in strikes:
        symbol_ce = f"NIFTY{real_date}{strike}CE"
        symbol_pe = f"NIFTY{real_date}{strike}PE"
        token_ce_df = token_df[token_df['symbol'] == symbol_ce]
        token_pe_df = token_df[token_df['symbol'] == symbol_pe]
        token_ce = token_ce_df['token'].iloc[0] if not token_ce_df.empty else None
        token_pe = token_pe_df['token'].iloc[0] if not token_pe_df.empty else None
        if token_ce:
            r.hset("token_data", symbol_ce, token_ce)
        if token_pe:
            r.hset("token_data", symbol_pe, token_pe)

def get_trading_day(date_str):
    nse = mcal.get_calendar('NSE')
    date = pd.to_datetime(date_str, format='%d%b%y')
    month_start = date.replace(day=1)
    month_end = month_start + pd.DateOffset(months=1)
    trading_days = nse.schedule(start_date=month_start, end_date=month_end)

    if date in trading_days.index:
        return date.strftime('%d%b%y').upper()
    else:
        prev_trading_day = trading_days[trading_days.index < date].index[-1]
        return prev_trading_day.strftime('%d%b%y').upper()

def history(token, interval="FIVE_MINUTE", to_date=None, from_date=None):
    if not to_date:
        to_date = datetime.now()
    if not from_date:
        from_date = to_date - timedelta(days=7)

    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")

    try:
        historicParam = {
            "exchange": "NSE",
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date_format,
            "todate": to_date_format
        }
        return obj.getCandleData(historicParam)
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        return None

def get_previous_trading_days(num_days=2):
    now = datetime.now()
    nse = mcal.get_calendar('NSE')
    schedule = nse.schedule(start_date=now - timedelta(days=30), end_date=now)
    trading_days = schedule.index.date
    return list(trading_days[-num_days:])

def calculate_previous_day_from_date(remaining_candles):
    trading_days = get_previous_trading_days(num_days=5)
    required_minutes = remaining_candles * 5
    total_minutes_available = 0

    market_open = timedelta(hours=9, minutes=15)
    market_close = timedelta(hours=15, minutes=30)
    market_duration = (market_close - market_open).seconds // 60

    for previous_trading_day in reversed(trading_days):
        total_minutes_available += market_duration

        if total_minutes_available >= required_minutes:
            minutes_to_fetch_from_day = required_minutes - (total_minutes_available - market_duration)
            start_time = datetime.combine(previous_trading_day, datetime.min.time()) + market_close - timedelta(
                minutes=minutes_to_fetch_from_day
            )
            return start_time

    raise ValueError("Not enough trading hours in the past trading days to fetch the required candles.")


# Script Mode (when run directly)
if __name__ == "__main__":
    start_redis_server()
    real_date = input("Expiry Date format -'01JAN25' : ")
    r.hset("date", "expiry", real_date)
    init_smartconnect("92XQzi0N", "Y119175", "1990", "ZDWHGV4NBSWFSJXJGJAZRAVCJQ")
    storeTokens(real_date)