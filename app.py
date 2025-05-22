import subprocess
import time
import pyotp
import requests
import pandas as pd
import redis
import json
from datetime import datetime, timedelta
from SmartApi import SmartConnect
import http.client # For potential debug of SmartApi's internal HTTP
import socket
import uuid
import re

# Import your custom modules
import conditions
import orders

# --- GLOBAL CONFIG ---
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
SYMBOL = "Nifty 50"
EXCHANGE = "NSE"
LTP_TOKEN = "99926000" # Token for Nifty 50 index (Nifty 50 index is usually this for NSE)
TOKEN_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# --- INIT REDIS (Global to app.py) ---
redis_client = redis.StrictRedis(**REDIS_CONFIG)

# --- UTILITY FUNCTIONS FOR IP/MAC ---
# These are often used by SmartAPI internally for headers
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        response.raise_for_status()
        return response.json()['ip']
    except Exception:
        return '127.0.0.1'

def get_mac_address():
    mac = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
    return mac.upper()

# --- REDIS MANAGEMENT ---
def clear_redis_cache():
    """Clears all data from the Redis database."""
    try:
        redis_client.flushdb()
        print("🧹 Redis cache cleared successfully.")
    except redis.RedisError as e:
        print(f"❌ Failed to clear Redis cache: {e}")

def ensure_redis_running():
    """Ensures the Redis server is running, attempts to start it if not."""
    try:
        if redis_client.ping():
            print("✅ Redis is running.")
            return
    except redis.ConnectionError:
        pass # Redis not running, try to start it

    print("🚀 Attempting to start Redis server...")
    # This command works on Linux/macOS. For Windows, you might need a different path
    subprocess.Popen(["redis-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Give Redis a moment to start
    for _ in range(10): # Try for up to 10 seconds
        try:
            if redis_client.ping():
                print("✅ Redis started successfully.")
                return
        except redis.ConnectionError:
            time.sleep(1)
    print("❌ Failed to start Redis. Please ensure Redis is installed and runnable.")
    exit(1)

# --- USER AUTHENTICATION ---
def login_user(user_config):
    """
    Logs in a single user using their credentials and returns an authenticated SmartConnect object.
    
    Args:
        user_config (dict): Dictionary containing 'api_key', 'client_id', 'pin', 'totp'.
        
    Returns:
        SmartConnect: An authenticated SmartConnect object, or None if login fails.
    """
    try:
        # Generate TOTP dynamically
        totp = pyotp.TOTP(user_config['totp']).now()
        
        # Initialize SmartConnect for this user
        smart_api_instance = SmartConnect(api_key=user_config['api_key'])
        
        # You might need to set local/public IP and MAC if SmartAPI doesn't handle it
        # smart_api_instance.set
        
        # Generate session
        session = smart_api_instance.generateSession(user_config['client_id'], user_config['pin'], totp)
        
        # Debugging the full session response can be helpful
        # print(f"🔍 Debug: Full session response for {user_config['username']}: {json.dumps(session, indent=2)}")

        # Check session status
        if session.get("status") in [True, "Ok", "success"]:
            print(f"✅ {user_config['username']} logged in successfully.")
            return smart_api_instance
        else:
            raise Exception(f"Login failed: {session.get('message', 'Unknown error')} (Error Code: {session.get('errorCode', 'N/A')})")
    except Exception as e:
        print(f"❌ Login failed for {user_config['username']}: {e}")
        return None

# --- EXPIRY SELECTION ---
def choose_expiry():
    """
    Prompts the user to select an Nifty expiry date from available options.
    Fetches scrip master to get valid expiries.
    """
    try:
        response = requests.get(TOKEN_URL)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        
        if not data:
            print("❌ API returned empty scrip master data.")
            exit(1)
        
        df = pd.DataFrame(data)
        
        # Ensure 'expiry' column exists and is parsed correctly
        if 'expiry' in df.columns:
            df['expiry'] = df['expiry'].astype(str) 
            # Use errors='coerce' to turn unparseable dates into NaT
            df['expiry_date'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce')
        else:
            print("❌ No 'expiry' column found in scrip master data. Check API response format.")
            exit(1)
        
        # Filter for Nifty index options on NFO exchange
        df_options = df[
            (df['exch_seg'].str.upper() == 'NFO') &
            (df['instrumenttype'].str.upper() == 'OPTIDX') &
            (df['symbol'].str.startswith('NIFTY', na=False)) & 
            (df['expiry_date'].notna()) # Exclude rows where expiry parsing failed
        ].copy() # Use .copy() to avoid SettingWithCopyWarning
        
        if df_options.empty:
            print("❌ No NIFTY options found in the scrip master.")
            exit(1)
        
        today = datetime.now().date()
        # Get unique and sorted expiry dates
        valid_expiries = sorted(df_options['expiry_date'].dt.date.dropna().unique())
        
        # Filter for future expiries
        future_expiries = [d for d in valid_expiries if d >= today]
        if not future_expiries:
            print("❌ No future expiry dates found for NIFTY options.")
            exit(1)
        
        print("\n🔢 Please select an expiry date:")
        
        display_options = []
        # Display up to 10 future expiries for user choice
        for i, expiry_date_obj in enumerate(future_expiries[:10]):
            expiry_str = expiry_date_obj.strftime('%d%b%y').upper()
            display_options.append((expiry_str, expiry_date_obj))
            print(f"  {i+1}. {expiry_str}")

        if not display_options:
            print("❌ No future expiry dates available to choose from.")
            exit(1)

        while True:
            try:
                choice = int(input("Enter the number of your choice: ").strip())
                if 1 <= choice <= len(display_options):
                    selected_expiry_str = display_options[choice - 1][0]
                    print(f"📅 You selected expiry date: {selected_expiry_str}")
                    return selected_expiry_str
                else:
                    print("❌ Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("❌ Invalid input. Please enter a number.")
    except requests.RequestException as e:
        print(f"❌ Network error fetching token data from {TOKEN_URL}: {e}")
        exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred in choose_expiry: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

# --- FETCH LTP ---
def fetch_ltp(smart_api_conn):
    """
    Fetches the Last Traded Price (LTP) for the Nifty 50 index.
    
    Args:
        smart_api_conn: An authenticated SmartConnect object.
        
    Returns:
        int: The LTP of Nifty 50, or None if fetching fails.
    """
    try:
        ltp_data = smart_api_conn.ltpData(EXCHANGE, SYMBOL, LTP_TOKEN)
        # Check for 'data' key and then 'ltp' within it
        if ltp_data and ltp_data.get('data') and ltp_data['data'].get('ltp') is not None:
            ltp = ltp_data['data']['ltp']
            print(f"📈 Current Nifty LTP: {ltp}")
            return int(ltp)
        else:
            print(f"❌ LTP data not found in response: {ltp_data}")
            return None
    except Exception as e:
        print(f"❌ Error fetching Nifty LTP: {e}")
        import traceback
        traceback.print_exc()
        return None

# --- TOKEN STORAGE ---
def store_option_tokens(smart_api_conn, expiry, username):
    """
    Fetches Nifty option details for a given expiry and stores them in Redis.
    """
    print(f"📦 Storing Nifty option tokens for {username} and expiry {expiry}")
    try:
        response = requests.get(TOKEN_URL)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("❌ Scrip master JSON is empty.")
            return False

        df = pd.DataFrame(data)

        required_columns = ['token', 'symbol', 'lotsize', 'expiry', 'exch_seg', 'instrumenttype', 'strike']
        if not all(col in df.columns for col in required_columns):
            print(f"❌ Missing required columns in scrip master: {required_columns}")
            return False

        df['expiry'] = df['expiry'].astype(str)
        df['expiry_date'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce')

        df['strike_numeric'] = pd.to_numeric(df['strike'], errors='coerce')
        df['token'] = df['token'].astype(str)
        df['lotsize'] = df['lotsize'].astype(str)

        df_filtered_options = df[
            (df['exch_seg'].str.upper() == 'NFO') &
            (df['instrumenttype'].str.upper() == 'OPTIDX') &
            (df['symbol'].str.startswith('NIFTY', na=False)) &
            (df['expiry_date'].notna()) &
            (df['strike_numeric'].notna()) &
            (df['strike_numeric'] > 0)
        ].copy()

        if df_filtered_options.empty:
            print(f"❌ No NIFTY option data found in scrip master for any expiry.")
            return False

        try:
            expiry_obj = datetime.strptime(expiry, '%d%b%y').date()
        except ValueError:
            print(f"❌ Invalid expiry format: {expiry}. Expected DDMMMYY (e.g., 29MAY25).")
            return False

        df_filtered_options = df_filtered_options[
            df_filtered_options['expiry_date'].dt.date == expiry_obj
        ]

        if df_filtered_options.empty:
            print(f"❌ No NIFTY options found for expiry {expiry}. Available expiries: {sorted(df['expiry_date'].dt.date.dropna().unique())}")
            return False

        df_filtered_options['strike_scaled'] = df_filtered_options['strike_numeric'] / 100

        ltp = fetch_ltp(smart_api_conn)
        if not ltp:
            print(f"❌ Failed to fetch LTP for {SYMBOL}. Cannot determine ATM strikes.")
            return False

        redis_client.set(f"{username}:NIFTY_LTP", ltp)

        base_strike = round(ltp / 50) * 50
        strikes = df_filtered_options['strike_scaled'].unique()
        strikes_in_range = [s for s in strikes if base_strike - 1500 <= s <= base_strike + 1500]
        strikes_in_range = sorted(strikes_in_range)

        if not strikes_in_range:
            print(f"⚠️ No strikes found within ±1500 of LTP ({ltp}) for expiry {expiry}.")
            return False

        stored = 0
        for option_type in ['CE', 'PE']:
            for strike_val in strikes_in_range:
                strike_int = int(strike_val)
                pattern = rf"NIFTY{expiry}{strike_int}{option_type}"

                match_row = df_filtered_options[
                    df_filtered_options['symbol'].str.match(pattern, na=False)
                ]

                if not match_row.empty:
                    match_data = match_row.iloc[0]
                    token = match_data['token']
                    trading_symbol = match_data['symbol']
                    lotsize = match_data['lotsize']

                    redis_symbol_key = f"NIFTY {expiry} {strike_int} {option_type}"

                    redis_client.set(f"{username}:{redis_symbol_key}", token)
                    redis_client.set(f"{username}:format:{redis_symbol_key}", trading_symbol)
                    redis_client.set(f"{username}:lotsize:{redis_symbol_key}", lotsize)
                    print(f"✅ Stored {username}:{redis_symbol_key} -> Token: {token}, Trading Symbol: {trading_symbol}, Lot Size: {lotsize}")
                    stored += 1
                else:
                    print(f"⚠️ No match found for {option_type} strike {strike_int} for expiry {expiry}. Pattern: {pattern}")

        if stored == 0:
            print(f"❌ No tokens stored for {username} and expiry {expiry}. Check scrip master or pattern matching.")
            return False

        return True

    except requests.RequestException as e:
        print(f"❌ Network error fetching token data from {TOKEN_URL}: {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred during token storage for {username}: {e}")
        import traceback
        traceback.print_exc()
        return False

# --- STRATEGY EXECUTION ---
def run_strategy(user_config, expiry, smart_api_conn):
    """
    Executes the trading strategy for a given user.
    
    Args:
        user_config (dict): User's configuration details.
        expiry (str): The selected expiry date.
        smart_api_conn: The authenticated SmartConnect object for the user.
    """
    username = user_config['username']
    print(f"▶️ Running strategies for {username} with expiry {expiry}")
    
    # Verify Redis connectivity
    try:
        redis_client.ping()
    except redis.ConnectionError as e:
        print(f"❌ Redis connection failed for {username}: {e}")
        return

    # --- CE Order Logic ---
    try:
        ce_symbols = conditions.get_ce_symbols_from_condition(expiry=expiry, username=username)
        print(f"📋 CE symbols identified by condition: {ce_symbols}")
        
        for symbol in ce_symbols:
            if expiry not in symbol:
                print(f"⚠️ Symbol {symbol} has incorrect expiry, expected {expiry}. Skipping.")
                continue
            
            # Convert trading symbol to Redis key format (e.g., NIFTY29MAY2524500CE -> NIFTY 29MAY25 24500 CE)
            match = re.match(r"NIFTY(\d{2}\w{3}\d{2})(\d+)(CE|PE)", symbol)
            if not match:
                print(f"⚠️ Invalid symbol format: {symbol}. Skipping.")
                continue
            expiry_part, strike, option_type = match.groups()
            redis_symbol_key = f"NIFTY {expiry_part} {int(strike)} {option_type}"
            
            token = redis_client.get(f"{username}:{redis_symbol_key}")
            if not token:
                print(f"⚠️ No token found for {symbol} in Redis (key: {username}:{redis_symbol_key}). Skipping.")
                redis_keys = redis_client.keys(f"{username}:NIFTY*")
                print(f"🔍 Available Redis keys for {username}: {[k.decode('utf-8') for k in redis_keys]}")
                continue
            
            for attempt in range(3):
                success, result = orders.place_option_order(smart_api_conn, username, symbol, quantity_multiplier=1, transaction_type="BUY")
                
                if success:
                    print(f"✅ CE order placed for {symbol}: Order ID {result}")
                    break
                elif result == "AG8001" or (isinstance(result, str) and "0521" in result):
                    print(f"🔄 Encountered session error ({result}) for {symbol}. Attempt {attempt + 1}/3. Re-logging in...")
                    new_smart_api_conn = login_user(user_config)
                    if new_smart_api_conn:
                        smart_api_conn = new_smart_api_conn
                        print(f"✅ Re-login successful for {username}.")
                    else:
                        print(f"❌ Re-login failed for {username}. Skipping order for {symbol}.")
                        break
                else:
                    print(f"⚠️ Failed to place CE order for {symbol}: {result}")
                    if attempt < 2:
                        print(f"🔄 Retrying order placement for {symbol} (Attempt {attempt + 2}/3)...")
                        time.sleep(1)
                    else:
                        print(f"❌ Max retries reached for {symbol}. Skipping.")
    except Exception as e:
        print(f"❌ An unexpected error occurred in CE order logic for {username}: {e}")
        import traceback
        traceback.print_exc()

    # --- PE Order Logic ---
    try:
        pe_symbols = conditions.get_pe_symbols_from_condition(expiry=expiry, username=username)
        print(f"📋 PE symbols identified by condition: {pe_symbols}")
        
        for symbol in pe_symbols:
            if expiry not in symbol:
                print(f"⚠️ Symbol {symbol} has incorrect expiry, expected {expiry}. Skipping.")
                continue
            
            # Convert trading symbol to Redis key format
            match = re.match(r"NIFTY(\d{2}\w{3}\d{2})(\d+)(CE|PE)", symbol)
            if not match:
                print(f"⚠️ Invalid symbol format: {symbol}. Skipping.")
                continue
            expiry_part, strike, option_type = match.groups()
            redis_symbol_key = f"NIFTY {expiry_part} {int(strike)} {option_type}"
            
            token = redis_client.get(f"{username}:{redis_symbol_key}")
            if not token:
                print(f"⚠️ No token found for {symbol} in Redis (key: {username}:{redis_symbol_key}). Skipping.")
                redis_keys = redis_client.keys(f"{username}:NIFTY*")
                print(f"🔍 Available Redis keys for {username}: {[k.decode('utf-8') for k in redis_keys]}")
                continue
            
            for attempt in range(3):
                success, result = orders.place_option_order(smart_api_conn, username, symbol, quantity_multiplier=1, transaction_type="BUY")
                
                if success:
                    print(f"✅ PE order placed for {symbol}: Order ID {result}")
                    break
                elif result == "AG8001" or (isinstance(result, str) and "0521" in result):
                    print(f"🔄 Encountered session error ({result}) for {symbol}. Attempt {attempt + 1}/3. Re-logging in...")
                    new_smart_api_conn = login_user(user_config)
                    if new_smart_api_conn:
                        smart_api_conn = new_smart_api_conn
                        print(f"✅ Re-login successful for {username}.")
                    else:
                        print(f"❌ Re-login failed for {username}. Skipping order for {symbol}.")
                        break
                else:
                    print(f"⚠️ Failed to place PE order for {symbol}: {result}")
                    if attempt < 2:
                        print(f"🔄 Retrying order placement for {symbol} (Attempt {attempt + 2}/3)...")
                        time.sleep(1)
                    else:
                        print(f"❌ Max retries reached for {symbol}. Skipping.")
    except Exception as e:
        print(f"❌ An unexpected error occurred in PE order logic for {username}: {e}")
        import traceback
        traceback.print_exc()

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    print("🚀 Starting Angel One Multi-User Nifty Bot")
    ensure_redis_running()
    clear_redis_cache() # Clear cache at the start to ensure fresh data

    # Load users from user_credentials.json
    try:
        with open('user_credentials.json', 'r') as f:
            loaded_users = json.load(f)['users']
            if not loaded_users:
                print("❌ No users found in user_credentials.json. Please run manage_users.py to add users.")
                exit(1)
    except FileNotFoundError:
        print("❌ user_credentials.json not found. Please run manage_users.py to add users.")
        exit(1)
    except json.JSONDecodeError:
        print("❌ Error decoding user_credentials.json. Check file format. Make sure it's valid JSON.")
        exit(1)
    
    users = loaded_users

    # Choose expiry date (or retrieve from Redis if already set)
    try:
        expiry = redis_client.hget("date", "expiry")
        if expiry:
            expiry = expiry.decode('utf-8')
            print(f"📅 Using previously selected expiry: {expiry}")
        else:
            expiry = choose_expiry()
            redis_client.hset("date", "expiry", expiry)
    except Exception as e:
        print(f"❌ Error with expiry selection/retrieval: {e}. Attempting to choose a new one.")
        expiry = choose_expiry() # Fallback to choosing if error
        redis_client.hset("date", "expiry", expiry)


    # Iterate through each user and run their strategies
    for user in users:
        print(f"\n=== 👤 Processing user: {user['username']} ===")
        # Login the user to get an authenticated SmartConnect object
        smart_api_conn = login_user(user)
        if not smart_api_conn:
            print(f"Skipping {user['username']} due to login failure.")
            continue

        # Store necessary tokens for the chosen expiry for this user
        if not store_option_tokens(smart_api_conn, expiry, user['username']):
            print(f"⚠️ Failed to store tokens for {user['username']}. Skipping strategy run.")
            continue

        # Run the trading strategy for the user
        run_strategy(user, expiry, smart_api_conn)