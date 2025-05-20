from SmartApi import SmartConnect
import json
import threading
import pyotp
import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)

# def get_token_for_price(price):
#     real_date = r.hget("date", "expiry")
#     if not real_date:
#         print("❌ Expiry date not found in Redis.")
#         return None

#     real_date = real_date.decode()
#     strike = int(round(price / 50.0) * 50)

#     print(f"Looking for token with expiry: {real_date}, strike: {strike}")
#     all_keys = [key.decode() for key in r.hkeys("token_data")]
#     print(f"All token keys in Redis (sample 10): {all_keys[:10]}")

#     for key in r.hkeys("token_data"):
#         sym = key.decode()
#         if sym.startswith("NIFTY") and sym.endswith("CE") and real_date in sym and str(strike) in sym:
#             token = r.hget("token_data", key).decode()
#             print(f"✅ Found CE token for {sym}: {token}")
#             return sym, token

#     print(f"❌ CE Token not found for strike {strike} and expiry {real_date}")
#     return None
def get_token_for_price(price, expiry):
    strike = int(round(price / 50.0) * 50)

    print(f"Looking for token with expiry: {expiry}, strike: {strike}")
    all_keys = [key.decode() for key in r.hkeys("token_data")]
    print(f"All token keys in Redis (sample 10): {all_keys[:10]}")

    for key in r.hkeys("token_data"):
        sym = key.decode()
        # Check if token symbol contains the expiry and strike
        if sym.startswith("NIFTY") and sym.endswith("CE") and expiry in sym and str(strike) in sym:
            token = r.hget("token_data", key).decode()
            print(f"✅ Found CE token for {sym}: {token}")
            return sym, token

    print(f"❌ CE Token not found for strike {strike} and expiry {expiry}")
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

        # Check multiple possible status formats
        if session.get("status") in [True, "Ok", "SUCCESS", "Success"]:
            print(f"[{user['client_id']}] ✅ Logged in. Placing CE order...")

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
            print(f"[{user['client_id']}] ✅ CE Order response: {response}")

        else:
            print(f"[{user['client_id']}] ❌ Session error: {session}")

    except Exception as e:
        print(f"[{user['client_id']}] ❌ Exception at order CE: {e}")

def placeOrder(price, expiry):
    print(f"🚀 Executing CE order for price: {price} with expiry: {expiry}")
    users = load_credentials()
    result = get_token_for_price(price, expiry)
    if not result:
        print("⚠️ Aborting CE order due to missing token.")
        return

    symbol, token = result

    threads = []
    for user in users:
        t = threading.Thread(target=execute_order_for_user, args=(user, symbol, token, price))
        threads.append(t)
        t.start()
        time.sleep(1)  # Add delay between starts to reduce rate limiting

    for t in threads:
        t.join()

    print("✅ All CE orders processed.")


if __name__ == "__main__":
    import sys
    input_price = int(sys.argv[1]) if len(sys.argv) > 1 else 22800
    # Pass expiry as second argument or default to '20MAY25'
    input_expiry = sys.argv[2] if len(sys.argv) > 2 else "20MAY25"
    placeOrder(input_price, input_expiry)
