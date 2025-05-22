# orders.py
import redis
from SmartApi.smartConnect import SmartConnect
import re

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
redis_client = redis.StrictRedis(**REDIS_CONFIG)

def place_option_order(smart_api_conn: SmartConnect, username: str, symbol: str, quantity_multiplier: int, transaction_type: str) -> tuple:
    """
    Places an option order using the SmartAPI.
    """
    try:
        # Convert trading symbol to Redis key format
        match = re.match(r"NIFTY(\d{2}\w{3}\d{2})(\d+)(CE|PE)", symbol)
        if not match:
            return False, f"Invalid symbol format: {symbol}"
        expiry_part, strike, option_type = match.groups()
        redis_symbol_key = f"NIFTY {expiry_part} {int(strike)} {option_type}"

        token = redis_client.get(f"{username}:{redis_symbol_key}")
        lotsize = redis_client.get(f"{username}:lotsize:{redis_symbol_key}")

        if not token or not lotsize:
            return False, f"Token or lot size not found for {symbol} in Redis (key: {username}:{redis_symbol_key})"

        token = token.decode('utf-8')
        lotsize = int(lotsize.decode('utf-8'))
        quantity = lotsize * quantity_multiplier

        if transaction_type not in ["BUY", "SELL"]:
            return False, f"Invalid transaction type: {transaction_type}"

        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": transaction_type,
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": quantity,
            "price": 0,
            "squareoff": 0,
            "stoploss": 0
        }

        print(f"📋 Placing order for {username}: {order_params}")
        response = smart_api_conn.placeOrder(order_params)
        print(f"🔍 Raw API response for {symbol}: {response}")

        if isinstance(response, dict):
            if response.get("status") is True and response.get("data", {}).get("orderid"):
                return True, response["data"]["orderid"]
            else:
                return False, response.get("message", "Unknown error") + f" (Error Code: {response.get('errorcode', 'N/A')})"
        elif isinstance(response, str):
            if response.isdigit():
                return True, response
            else:
                return False, f"Unexpected string response: {response}"
        else:
            return False, f"API_RESPONSE_UNEXPECTED_TYPE: {type(response)}"

    except Exception as e:
        print(f"❌ Error placing order for {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e)