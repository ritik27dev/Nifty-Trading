import subprocess
import time
import pyotp
import requests
import pandas as pd
import redis
import json
from datetime import datetime, timedelta
from SmartApi import SmartConnect
import condition_ce as ce
import condition_pe as pe
import http.client

# --- GLOBAL CONFIG ---
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
SYMBOL = "Nifty 50"
EXCHANGE = "NSE"
LTP_TOKEN = "99926000" # Token for Nifty 50 index
TOKEN_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json" # As confirmed by user

# --- INIT REDIS ---
redis_client = redis.StrictRedis(**REDIS_CONFIG)

# --- CLEAR REDIS CACHE ---
def clear_redis_cache():
    try:
        redis_client.flushdb()
        print("🧹 Redis cache cleared successfully.")
    except redis.RedisError as e:
        print(f"❌ Failed to clear Redis cache: {e}")
        exit(1)

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

# --- LOGIN USER ---
def login_user(user_config):
    totp = pyotp.TOTP(user_config['totp']).now()
    smart_api = SmartConnect(api_key=user_config['api_key'])
    try:
        # Generate the session and get the full session object
        session = smart_api.generateSession(user_config['client_id'], user_config['pin'], totp)
        if session.get("status") in ["Ok", "success", True]:
            print(f"✅ {user_config['username']} logged in.")
            # Store the full session object for later use
            smart_api.full_session_data = session # Storing the session data for access
            return smart_api
        else:
            raise Exception(f"Login failed: {session}")
    except Exception as e:
        print(f"❌ Login failed for {user_config['username']}: {e}")
        return None

# --- CHOOSE EXPIRY ---
def choose_expiry():
    try:
        response = requests.get(TOKEN_URL)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print("❌ API returned empty data.")
            exit(1)
        
        df = pd.DataFrame(data)
        print(f"📊 Total records in API response: {len(df)}")
        
        if 'expiry' in df.columns:
            df['expiry'] = df['expiry'].astype(str) 
            df['expiry_date'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce')
            invalid_expiries = df[df['expiry_date'].isna()]['expiry'].unique()[:5]
            if len(invalid_expiries) > 0:
                print(f"⚠️ Sample of invalid expiry values: {list(invalid_expiries)}")
        else:
            print("❌ No 'expiry' column in API response.")
            exit(1)
        
        df_options = df[
            (df['exch_seg'].str.upper() == 'NFO') &
            (df['instrumenttype'].str.upper() == 'OPTIDX') &
            (df['symbol'].str.startswith('NIFTY', na=False)) & 
            (df['expiry_date'].notna()) 
        ].copy()
        
        print(f"📊 NIFTY options after initial filtering: {len(df_options)}")
        if df_options.empty:
            print("❌ No NIFTY options found. Check API data for 'NFO', 'OPTIDX', and 'NIFTY' in 'symbol'.")
            sample_df_debug = df[
                (df['instrumenttype'].str.upper() == 'OPTIDX') &
                (df['symbol'].str.contains('NIFTY', case=False, na=False))
            ].head(5)
            if not sample_df_debug.empty:
                print("\n📋 Sample of NIFTY OPTIDX entries (before expiry filtering):")
                print(sample_df_debug[['symbol', 'expiry', 'strike', 'instrumenttype', 'exch_seg', 'token']].to_string())
            exit(1)
        
        today = datetime.now().date()
        valid_expiries = sorted(df_options['expiry_date'].dt.date.dropna().unique())
        
        future_expiries = [d for d in valid_expiries if d >= today]
        if not future_expiries:
            print("❌ No future expiry dates found for NIFTY options.")
            exit(1)
        
        print("\n🔢 Please select an expiry date:")
        
        display_options = []
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
        print(f"❌ Error in choose_expiry: {e}")
        exit(1)

# --- PLACE OPTION ORDER ---
def place_option_order(smart_api, username, symbol, token, quantity=50, transaction_type="BUY"):
    try:
        trading_symbol_from_redis = redis_client.get(f"{username}:format:{symbol}")
        if not trading_symbol_from_redis:
            print(f"❌ Trading symbol format not found for {symbol} in Redis.")
            return None
        
        payload = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol_from_redis.decode('utf-8'),
            "symboltoken": str(token),
            "transactiontype": transaction_type,
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": str(quantity),
            "price": "0",
            "squareoff": "0",
            "stoploss": "0"
        }
        
        # --- FIX: Access JWT token correctly from the stored session data ---
        # The session data is stored in smart_api.full_session_data after login
        jwt_token = smart_api.full_session_data.get('data', {}).get('jwtToken')
        
        if not jwt_token:
            print(f"❌ JWT token not found in SmartConnect session data for {username}.")
            return None

        headers = {
            'Authorization': f'Bearer {jwt_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-UserType': 'USER',
            'X-SourceID': 'WEB',
            'X-ClientLocalIP': '127.0.0.1', # Placeholder, might need actual IP or leave as is
            'X-ClientPublicIP': '127.0.0.1', # Placeholder, might need actual IP or leave as is
            'X-MACAddress': '00:00:00:00:00:00', # Placeholder, might need actual MAC or leave as is
            'X-PrivateKey': smart_api.api_key
        }

        conn = http.client.HTTPSConnection("apiconnect.angelone.in")
        conn.request("POST", "/rest/secure/angelbroking/order/v1/placeOrder", json.dumps(payload), headers)
        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))
        conn.close()

        if data.get("status"):
            print(f"✅ Order placed for {trading_symbol_from_redis.decode('utf-8')}: Order ID {data['data']['orderid']}")
            return data['data']['orderid']
        else:
            print(f"❌ Order placement failed for {trading_symbol_from_redis.decode('utf-8')}: {data.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"❌ Order placement error for {symbol}: {e}")
        return None

# --- FETCH LTP ---
def fetch_ltp(smart_api):
    try:
        ltp_data = smart_api.ltpData(EXCHANGE, SYMBOL, LTP_TOKEN)
        ltp = ltp_data['data']['ltp']
        print(f"📈 LTP: {ltp}")
        return int(ltp)
    except Exception as e:
        print(f"❌ LTP fetch error: {e}")
        return None

# --- STORE TOKENS ---
# --- STORE TOKENS ---
def store_option_tokens(smart_api, expiry, username):
    print(f"📦 Storing tokens for {username} and expiry {expiry}")
    try:
        response = requests.get(TOKEN_URL)
        response.raise_for_status()
        df = pd.DataFrame(response.json())

        expiry_obj = datetime.strptime(expiry, '%d%b%y').date()
        df['expiry'] = df['expiry'].astype(str) 
        df['expiry_date'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce')

        print(f"📊 Total records from {TOKEN_URL}: {len(df)}")
        
        df['strike_numeric'] = pd.to_numeric(df['strike'], errors='coerce')
        # Ensure 'token' column is treated as string to prevent numpy.float64 conversion
        df['token'] = df['token'].astype(str) # <--- ADD THIS LINE
        
        df_filtered_options = df[
            (df['exch_seg'].str.upper() == 'NFO') &
            (df['instrumenttype'].str.upper() == 'OPTIDX') &
            (df['symbol'].str.startswith('NIFTY', na=False)) & 
            (df['expiry_date'].dt.date == expiry_obj) & 
            (df['strike_numeric'].notna()) & 
            (df['strike_numeric'] > 0)
        ].copy() 

        if df_filtered_options.empty:
            print(f"❌ No NIFTY option data found for expiry {expiry} after detailed filter from {TOKEN_URL}.")
            print("\n📋 Debug Info: Checking counts for NIFTY OPTIDX by expiry:")
            debug_df = df[
                (df['exch_seg'].str.upper() == 'NFO') &
                (df['instrumenttype'].str.upper() == 'OPTIDX') &
                (df['symbol'].str.contains('NIFTY', case=False, na=False))
            ].copy()
            debug_df['expiry_date'] = pd.to_datetime(debug_df['expiry'], format='%d%b%Y', errors='coerce').dt.date
            print(debug_df.groupby('expiry_date').size().reset_index(name='count').to_string())
            
            return False

        df_filtered_options['strike_scaled'] = df_filtered_options['strike_numeric'] / 100 
        
        print(f"📊 Filtered NIFTY options for {expiry}: {len(df_filtered_options)} records found.")
        print(f"📋 Sample of filtered option symbols: {df_filtered_options['symbol'].sample(min(5, len(df_filtered_options))).tolist()}")
        print(f"📋 Available strikes for {expiry} (scaled): {sorted(df_filtered_options['strike_scaled'].unique())[:10]}...") 

        ltp = fetch_ltp(smart_api)
        if not ltp:
            print(f"❌ Failed to fetch LTP for {SYMBOL}")
            return False

        redis_client.set(f"{username}:NIFTY_LTP", ltp)
        print(f"✅ Stored LTP for {username}: {ltp}")

        base_strike = round(ltp / 50) * 50
        
        strikes = df_filtered_options['strike_scaled'].unique()
        strikes_in_range = [s for s in strikes if base_strike - 1500 <= s <= base_strike + 1500] 
        strikes_in_range = sorted(strikes_in_range)
        
        if not strikes_in_range:
            print(f"⚠️ No relevant strikes found within ±1500 of LTP ({ltp}) for expiry {expiry}. Please check strike data or adjust range.")
            print(f"📋 All available strikes for {expiry}: {sorted(strikes)}")
            return False
            
        print(f"📋 Attempting to store tokens for strikes around LTP ({ltp}): {strikes_in_range}")

        stored = 0
        stored_symbols = []
        for option_type in ['CE', 'PE']:
            for strike_val in strikes_in_range:
                expected_trading_symbol = f"NIFTY{expiry}{int(strike_val)}{option_type}"
                
                match_row = df_filtered_options[
                    (df_filtered_options['symbol'] == expected_trading_symbol)
                ]
                
                if not match_row.empty:
                    match_data = match_row.iloc[0] 
                    token = match_data['token'] # This 'token' is now guaranteed to be a string
                    trading_symbol = match_data['symbol'] 
                    
                    redis_symbol_key = f"NIFTY {expiry} {int(strike_val)} {option_type}"

                    redis_client.set(f"{username}:{redis_symbol_key}", token) # Store it as string directly
                    redis_client.set(f"{username}:format:{redis_symbol_key}", trading_symbol)
                    print(f"✅ Stored {username}:{redis_symbol_key} -> Token: {token}, Trading Symbol: {trading_symbol}")
                    stored_symbols.append(redis_symbol_key)
                    stored += 1
        print(f"🔢 Total tokens stored for {username}: {stored}")
        print(f"📋 Stored symbols: {stored_symbols}")
        return stored > 0
    except requests.RequestException as e:
        print(f"❌ Network error fetching token data from {TOKEN_URL}: {e}")
        return False
    except Exception as e:
        print(f"❌ Token storage error for {username}: {e}")
        return False

# --- PLACE OPTION ORDER ---
def place_option_order(smart_api, username, symbol, token, quantity=50, transaction_type="BUY"):
    try:
        trading_symbol_from_redis = redis_client.get(f"{username}:format:{symbol}")
        if not trading_symbol_from_redis:
            print(f"❌ Trading symbol format not found for {symbol} in Redis.")
            return None
        
        payload = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol_from_redis.decode('utf-8'),
            "symboltoken": token, # This 'token' should now already be a string integer
            "transactiontype": transaction_type,
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": str(quantity),
            "price": "0",
            "squareoff": "0",
            "stoploss": "0"
        }
        
        jwt_token = smart_api.full_session_data.get('data', {}).get('jwtToken')
        
        if not jwt_token:
            print(f"❌ JWT token not found in SmartConnect session data for {username}.")
            return None

        # Debug prints you added are good! Keep them for further troubleshooting if needed.
        print(f"🔍 Debug: JWT Token for {username}: {jwt_token[:30]}...") 
        print(f"🔍 Debug: Payload for {username}: {json.dumps(payload, indent=2)}")


        headers = {
            'Authorization': f'Bearer {jwt_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-UserType': 'USER',
            'X-SourceID': 'WEB',
            'X-ClientLocalIP': '127.0.0.1', 
            'X-ClientPublicIP': '127.0.0.1', 
            'X-MACAddress': '00:00:00:00:00:00', 
            'X-PrivateKey': smart_api.api_key
        }

        conn = http.client.HTTPSConnection("apiconnect.angelone.in")
        conn.request("POST", "/rest/secure/angelbroking/order/v1/placeOrder", json.dumps(payload), headers)
        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))
        conn.close()

        if data.get("status"):
            print(f"✅ Order placed for {trading_symbol_from_redis.decode('utf-8')}: Order ID {data['data']['orderid']}")
            return data['data']['orderid']
        else:
            print(f"❌ Order placement failed for {trading_symbol_from_redis.decode('utf-8')}: {data.get('message', 'Unknown error')}")
            # --- IMPORTANT: PRINT THE FULL ERROR RESPONSE ---
            print(f"❌ Full API Error Response: {json.dumps(data, indent=2)}")
            return None
    except Exception as e:
        print(f"❌ Order placement error for {symbol}: {e}")
        return None
# --- RUN STRATEGIES ---
def run_strategy(username, expiry, smart_api):
    print(f"▶️ Running strategies for {username} with expiry {expiry}")
    try:
        ce_symbols = ce.condition(expiry=expiry, username=username)
        print(f"📋 CE symbols from condition: {ce_symbols}")
        for symbol in ce_symbols:
            if expiry not in symbol:
                print(f"⚠️ Symbol {symbol} has incorrect expiry, expected {expiry}")
                continue
            token = redis_client.get(f"{username}:{symbol}")
            if token:
                order_id = place_option_order(smart_api, username, symbol, token.decode('utf-8'), quantity=50, transaction_type="BUY")
                if order_id:
                    print(f"✅ CE order placed for {symbol}: Order ID {order_id}")
                else:
                    print(f"⚠️ Failed to place CE order for {symbol}")
            else:
                print(f"⚠️ No token found for {symbol} in Redis")
    except Exception as e:
        print(f"❌ CE error for {username}: {e}")

    try:
        pe_symbols = pe.condition(expiry=expiry, username=username)
        print(f"📋 PE symbols from condition: {pe_symbols}")
        for symbol in pe_symbols:
            if expiry not in symbol:
                print(f"⚠️ Symbol {symbol} has incorrect expiry, expected {expiry}")
                continue
            token = redis_client.get(f"{username}:{symbol}")
            if token:
                order_id = place_option_order(smart_api, username, symbol, token.decode('utf-8'), quantity=50, transaction_type="BUY")
                if order_id:
                    print(f"✅ PE order placed for {symbol}: Order ID {order_id}")
                else:
                    print(f"⚠️ Failed to place PE order for {symbol}")
            else:
                print(f"⚠️ No token found for {symbol} in Redis")
    except Exception as e:
        print(f"❌ PE error for {username}: {e}")

# --- MAIN DRIVER ---
if __name__ == "__main__":
    print("🚀 Starting Angel One Multi-User Nifty Bot")
    ensure_redis_running()
    clear_redis_cache()

    with open("user_credentials.json", "r") as f:
        users = json.load(f)['users']

    expiry = choose_expiry()
    redis_client.hset("date", "expiry", expiry)

    for user in users:
        print(f"\n=== 👤 Processing {user['username']} ===")
        smart_api = login_user(user)
        if not smart_api:
            continue

        if not store_option_tokens(smart_api, expiry, user['username']):
            print(f"⚠️ Failed to store tokens for {user['username']}")
            continue

        run_strategy(user['username'], expiry, smart_api)