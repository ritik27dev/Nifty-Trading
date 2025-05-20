from SmartApi import SmartConnect
import json
import threading
import pyotp
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_token_for_price(price):
    real_date = r.hget("date", "expiry")
    if not real_date:
        print("❌ Expiry date not found in Redis.")
        return None

    real_date = real_date.decode()
    strike = int(round(price / 50.0) * 50)

    for key in r.hkeys("token_data"):
        sym = key.decode()
        if sym.startswith("NIFTY") and sym.endswith("PE") and real_date in sym and str(strike) in sym:
            token = r.hget("token_data", key).decode()
            print(f"✅ Found PE token for {sym}: {token}")
            return sym, token

    print(f"❌ PE Token not found for strike {strike} and expiry {real_date}")
    return None

def load_credentials():
    with open('user_credentials.json', 'r') as file:
        return json.load(file)['users']

def execute_order_for_user(user, symbol, token, price):
    try:
        obj = SmartConnect(api_key=user['api_key'])
        session = obj.generateSession(
            clientCode=user['client_id'],
            password=user['pin'],
            totp=pyotp.TOTP(user['totp']).now()
        )

        if session.get("status") == "Ok":
            print(f"[{user['client_id']}] ✅ Logged in. Placing PE order...")

            params = {
                "variety": "ROBO",
                "tradingsymbol": symbol,
                "symboltoken": token,
                "transactiontype": "BUY",
                "exchange": "NFO",
                "ordertype": "LIMIT",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": price - 1,
                "squareoff": price + 39,
                "stoploss": price - 13,
                "trailingstoploss": 20,
                "quantity": "75"
            }

            response = obj.placeOrder(params)
            print(f"[{user['client_id']}] ✅ PE Order response: {response}")
        else:
            print(f"[{user['client_id']}] ❌ Session error: {session}")
    except Exception as e:
        print(f"[{user['client_id']}] ❌ Exception at order PE: {e}")

def placeOrder(price):
    print(f"🚀 Executing PE order for price: {price}")
    users = load_credentials()
    result = get_token_for_price(price)
    if not result:
        print("⚠️ Aborting PE order due to missing token.")
        return

    symbol, token = result
    threads = [threading.Thread(target=execute_order_for_user, args=(user, symbol, token, price)) for user in users]

    for t in threads: t.start()
    for t in threads: t.join()

    print("✅ All PE orders processed.")

if __name__ == "__main__":
    import sys
    input_price = int(sys.argv[1]) if len(sys.argv) > 1 else 22800
    placeOrder(input_price)
