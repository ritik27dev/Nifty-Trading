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
        
        # Debug: Print the first few rows to understand the format
        print("Sample of token data:")
        print(df.iloc[:5][['name', 'token', 'expiry']].head())
        
        # Check if we need to format the expiry date differently
        print("Unique expiry formats in data:")
        print(df['expiry'].dropna().unique()[:10])  # Print first 10 unique values
        
        # Try different ways to match the expiry
        # Method 1: Direct string match with various formats
        expiry_date = datetime.strptime(expiry, '%d%b%y')
        possible_formats = [
            expiry,                                # "21MAY25"
            expiry_date.strftime('%d%b%Y').upper(),  # "21MAY2025"
            expiry_date.strftime('%Y-%m-%d')         # "2025-05-21"
        ]
        
        print(f"Looking for expiry dates matching: {possible_formats}")
        
        # Create a temporary collection of possible tokens
        token_candidates = []
        
        # First, try exact string matches
        for format in possible_formats:
            matches = df[df['expiry'].astype(str).str.contains(format, case=False)]
            if not matches.empty:
                print(f"Found {len(matches)} matches using format: {format}")
                token_candidates.append(matches)
        
        # If no matches, try parsing dates
        if not token_candidates:
            # Try to parse the expiry column if it's not already a datetime
            if df['expiry'].dtype != 'datetime64[ns]':
                try:
                    df['expiry_date'] = pd.to_datetime(df['expiry'], errors='coerce')
                    # Match on the parsed date
                    matches = df[df['expiry_date'].dt.date == expiry_date.date()]
                    if not matches.empty:
                        print(f"Found {len(matches)} matches using parsed dates")
                        token_candidates.append(matches)
                except Exception as e:
                    print(f"Error parsing dates: {e}")
        
        # If we have candidates, use the first set
        if token_candidates:
            matching_df = pd.concat(token_candidates).drop_duplicates()
        else:
            # Fallback: Look for options with "NIFTY" and the expiry date in the name
            print("Fallback: Looking for options with NIFTY and expiry in the name")
            matching_df = df[df['name'].str.contains(f"NIFTY.*{expiry}", case=False, na=False)]
            if matching_df.empty:
                print("Still no matches. Trying broader search with just 'NIFTY'")
                matching_df = df[df['name'].str.contains("NIFTY", case=False, na=False)]
        
        # Extract options
        ltp = fetch_ltp()
        base_strike = round(ltp / 50) * 50
        strikes = [base_strike + i * 50 for i in range(-10, 11)]
        
        # Debug: Print some matching names to understand the format
        print("Sample option names from data:")
        print(matching_df['name'].sample(min(5, len(matching_df))).tolist())
        
        # Store tokens for both CE and PE options
        tokens_found = 0
        for option_type in ['CE', 'PE']:
            for strike in strikes:
                # Try multiple formats for symbol matching
                possible_symbols = [
                    f"NIFTY {expiry} {int(strike)} {option_type}",
                    f"NIFTY{expiry}{int(strike)}{option_type}",
                    f"NIFTY-{expiry}-{int(strike)}-{option_type}",
                    f"NIFTY {int(strike)} {option_type}"
                ]
                
                found = False
                for symbol in possible_symbols:
                    # Try exact match
                    match = matching_df[matching_df['name'] == symbol]
                    
                    # If no exact match, try contains
                    if match.empty:
                        match = matching_df[matching_df['name'].str.contains(f"{int(strike)}.*{option_type}", case=False, na=False)]
                    
                    if not match.empty:
                        token = match.iloc[0]['token']
                        redis_client.set(f"NIFTY {expiry} {int(strike)} {option_type}", token)
                        redis_client.set(f"format:NIFTY {expiry} {int(strike)} {option_type}", match.iloc[0]['name'])
                        print(f"✅ Stored {symbol} -> {token} (Original name: {match.iloc[0]['name']})")
                        tokens_found += 1
                        found = True
                        break
                
                if not found:
                    print(f"⚠️ Token not found for NIFTY {expiry} {int(strike)} {option_type}")
        
        print(f"Total tokens found and stored: {tokens_found}")
        if tokens_found == 0:
            print("❌ No tokens found. Please check the expiry date format or try a different date.")
            return False
        return True

    except Exception as e:
        print(f"❌ Error storing tokens: {e}")
        import traceback
        traceback.print_exc()
        return False

# --- RUN CE & PE STRATEGY ---
def run_strategy(expiry):
    print("▶️ Running CE condition...")
    try:
        # Pass the expiry to the condition function
        ce.condition(expiry=expiry)
    except Exception as e:
        print(f"❌ CE condition error: {e}")
        import traceback
        traceback.print_exc()

    print("▶️ Running PE condition...")
    try:
        # Pass the expiry to the condition function
        pe.condition(expiry=expiry)
    except Exception as e:
        print(f"❌ PE condition error: {e}")
        import traceback
        traceback.print_exc()

# --- MAIN DRIVER ---
if __name__ == "__main__":
    print("🚀 Starting Angel One Nifty Option Trading Bot...")
    ensure_redis_running()
    login()
    expiry = choose_expiry()
    redis_client.hset("date", "expiry", expiry)
    
    # Try to store tokens, retry with a different date if it fails
    if not store_option_tokens(expiry):
        print("Would you like to try with a different expiry date? (y/n)")
        if input().lower() == 'y':
            expiry = choose_expiry()
            redis_client.hset("date", "expiry", expiry)
            if not store_option_tokens(expiry):
                print("Still unable to find tokens. Please check API response format.")
                exit(1)
    
    # Run strategy with the expiry date
    run_strategy(expiry)