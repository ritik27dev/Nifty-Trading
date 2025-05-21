import redis
import pyotp
from datetime import datetime
from SmartApi import SmartConnect

# --- CONFIG ---
API_KEY = "92XQzi0N"
CLIENT_ID = "Y119175"
PASSWORD = "1990"
TOTP_SECRET = "ZDWHGV4NBSWFSJXJGJAZRAVCJQ"

# --- INIT REDIS ---
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# --- LOGIN ---
def login():
    smart_api = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    session = smart_api.generateSession(CLIENT_ID, PASSWORD, totp)
    if session.get("status") not in ["Ok", "success", True]:
        raise Exception("Login failed")
    print("✅ Logged in")
    return smart_api

# --- MAIN CONDITION ---
def condition(expiry, username):
    import redis
    REDIS_CONFIG = {
        "host": "localhost",
        "port": 6379,
        "db": 0
    }
    redis_client = redis.StrictRedis(**REDIS_CONFIG)
    
    # Get LTP from stored tokens (assume it's stored during store_option_tokens)
    ltp_key = f"{username}:NIFTY_LTP"
    ltp = redis_client.get(ltp_key)
    if not ltp:
        print(f"⚠️ No LTP found for {username}")
        return []
    
    ltp = int(float(ltp.decode('utf-8')))
    base_strike = round(ltp / 50) * 50
    
    # Select at-the-money (ATM) CE strike
    atm_strike = base_strike
    symbol = f"NIFTY {expiry} {atm_strike} CE"
    
    # Verify token exists
    if redis_client.get(f"{username}:{symbol}"):
        return [symbol]
    else:
        print(f"⚠️ CE condition: No token for {symbol}")
        return []
# --- PLACE ORDER ---
def place_order(smart_api, symbol, token, order_type, price, expiry):
    print(f"📝 Placing {order_type} order for {symbol} at {price}")
    try:
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": order_type,
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": "50"
        }

        print("📦 Order Params:", order_params)
        order_response = smart_api.placeOrder(order_params)
        print(f"✅ Order Placed: {order_response}")

        order_id = order_response.get('data', {}).get('orderid')
        if order_id:
            order_key = f"order:{symbol}:{datetime.now().strftime('%Y%m%d%H%M%S')}"
            order_data = {
                "order_id": order_id,
                "symbol": symbol,
                "token": token,
                "type": order_type,
                "price": price,
                "quantity": 50,
                "status": "PLACED",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "expiry": expiry
            }
            for k, v in order_data.items():
                redis_client.hset(order_key, k, v)
            print(f"🧠 Stored order in Redis: {order_key}")
    except Exception as e:
        print(f"❌ Error placing order: {e}")
        import traceback
        traceback.print_exc()

# --- ENTRY POINT ---
if __name__ == "__main__":
    expiry = redis_client.hget("date", "expiry").decode()
    condition(expiry)