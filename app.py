import subprocess
import time
import pyotp
import requests
import pandas as pd
import redis
import pandas_market_calendars as mcal
from datetime import datetime, timedelta
from SmartApi import SmartConnect
import condition_ce as ce
import condition_pe as pe

# --- CONFIGURATION ---
CONFIG = {
    "api_key": "92XQzi0N",
    "client_id": "Y119175",
    "password": "1990",
    "totp_secret": "ZDWHGV4NBSWFSJXJGJAZRAVCJQ",
    "redis": {
        "host": "localhost",
        "port": 6379,
        "db": 0
    },
    "symbol": "Nifty 50",
    "exchange": "NSE",
    "ltp_token": "99926000",
    "token_url": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
}

# --- INIT SMARTAPI + REDIS ---
smart_api = SmartConnect(api_key=CONFIG['api_key'])
redis_client = redis.StrictRedis(**CONFIG['redis'])

# --- REDIS CHECK ---
def ensure_redis_running():
    try:
        if redis_client.ping():
            print("✅ Redis is running.")
            return
    except redis.ConnectionError:
        pass

    print("🚀 Starting Redis server...")
    subprocess.Popen(["redis-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(10):
        try:
            if redis_client.ping():
                print("✅ Redis started successfully.")
                return
        except redis.ConnectionError:
            time.sleep(1)
    print("❌ Failed to start Redis.")
    exit(1)

# --- SMARTAPI LOGIN ---
def login():
    totp = pyotp.TOTP(CONFIG['totp_secret']).now()
    try:
        session = smart_api.generateSession(CONFIG['client_id'], CONFIG['password'], totp)
        if session.get("status") in ["Ok", "success", True]:
            print("✅ Logged in successfully.")
            return session
        else:
            raise Exception(f"Unexpected response: {session}")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        exit(1)

# --- FETCH LTP ---
def fetch_ltp():
    try:
        ltp_data = smart_api.ltpData(CONFIG['exchange'], CONFIG['symbol'], CONFIG['ltp_token'])
        ltp = ltp_data['data']['ltp']
        print(f"📈 Current LTP: {ltp}")
        return int(ltp)
    except Exception as e:
        print(f"❌ Failed to fetch LTP: {e}")
        exit(1)

# --- FETCH LTP ---
def fetch_ltp():
    try:
        ltp_data = smart_api.ltpData(CONFIG['exchange'], CONFIG['symbol'], CONFIG['ltp_token'])
        ltp = ltp_data['data']['ltp']
        print(f"📈 Current LTP: {ltp}")
        return int(ltp)
    except Exception as e:
        print(f"❌ Failed to fetch LTP: {e}")
        exit(1)

# --- EXPIRY SELECTION ---
def choose_expiry():
    cal = mcal.get_calendar('NSE')
    upcoming = cal.valid_days(
        start_date=datetime.now().strftime('%Y-%m-%d'),
        end_date=(datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')
    )
    dates = [d.strftime('%d%b%y').upper() for d in upcoming[:5]]
    print("📆 Available expiry dates:")
    for idx, date in enumerate(dates, 1):
        print(f"{idx}. {date}")

    while True:
        try:
            choice = int(input("Select expiry (1-5): "))
            if 1 <= choice <= len(dates):
                return dates[choice - 1]
        except:
            pass
        print("❌ Invalid input. Please try again.")
# --- STORE OPTION TOKENS TO REDIS ---
def store_option_tokens(expiry):
    print(f"📦 Fetching and storing tokens for expiry: {expiry}")
    try:
        response = requests.get(CONFIG['token_url'])
        df = pd.DataFrame(response.json())

        # Parse expiry column safely
        df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')
        df = df[df['expiry'].notna()]
        df['expiry'] = df['expiry'].dt.strftime('%d%b%y').str.upper()

        df = df.astype({'strike': float})
        df = df[df['expiry'] == expiry]

        ltp = fetch_ltp()
        base_strike = round(ltp / 50) * 50
        strikes = [base_strike + i * 50 for i in range(-10, 11)]

        for option_type in ['CE', 'PE']:
            for strike in strikes:
                symbol = f"NIFTY {expiry} {int(strike)} {option_type}"
                match = df[df['name'] == symbol]
                if not match.empty:
                    token = match.iloc[0]['token']
                    redis_client.set(symbol, token)
                    print(f"✅ Stored {symbol} -> {token}")
                else:
                    print(f"⚠️ Token not found for {symbol}")

    except Exception as e:
        print(f"❌ Error storing tokens: {e}")
        exit(1)

# --- RUN CE & PE STRATEGY ---
def run_strategy():
    print("▶️ Running CE condition...")
    try:
        ce.condition()
    except Exception as e:
        print(f"❌ CE condition error: {e}")

    print("▶️ Running PE condition...")
    try:
        pe.condition()
    except Exception as e:
        print(f"❌ PE condition error: {e}")

# --- MAIN DRIVER ---
if __name__ == "__main__":
    print("🚀 Starting Angel One Nifty Option Trading Bot...")
    ensure_redis_running()
    login()
    expiry = choose_expiry()
    redis_client.hset("date", "expiry", expiry)
    store_option_tokens(expiry)
    run_strategy()