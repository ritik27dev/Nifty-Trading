from SmartApi import SmartConnect
import pyotp
import redis
import time

# --- CONFIGURATION ---
API_KEY = "92XQzi0N"
CLIENT_ID = "Y119175"
PASSWORD = "1990"
TOTP_SECRET = "ZDWHGV4NBSWFSJXJGJAZRAVCJQ"

# --- INITIALIZE CLIENT + REDIS ---
smart_api = SmartConnect(api_key=API_KEY)
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# --- LOGIN ---
def login():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    session = smart_api.generateSession(CLIENT_ID, PASSWORD, totp)
    if session.get("status") not in ["Ok", "success", True]:
        raise Exception("Login failed")
    print("✅ Logged in to SmartAPI")

# --- PLACE CE ORDER ---
def place_ce_order(strike_price: int, expiry: str, quantity: int = 50):
    key = f"NIFTY {expiry} {strike_price} CE"
    token = r.get(key)

    if not token:
        print(f"❌ Token not found for: {key}")
        return

    try:
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": key,
            "symboltoken": token.decode(),
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": quantity
        }

        order_id = smart_api.placeOrder(order_params)
        print(f"✅ CE Order placed: {order_id}")
        return order_id
    except Exception as e:
        print(f"❌ Error placing CE order: {e}")