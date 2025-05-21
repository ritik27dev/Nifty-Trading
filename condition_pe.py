import redis
import time
from datetime import datetime
from SmartApi import SmartConnect

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def condition(expiry, username):
    import redis
    REDIS_CONFIG = {
        "host": "localhost",
        "port": 6379,
        "db": 0
    }
    redis_client = redis.StrictRedis(**REDIS_CONFIG)
    
    # Get LTP from stored tokens
    ltp_key = f"{username}:NIFTY_LTP"
    ltp = redis_client.get(ltp_key)
    if not ltp:
        print(f"⚠️ No LTP found for {username}")
        return []
    
    ltp = int(float(ltp.decode('utf-8')))
    base_strike = round(ltp / 50) * 50
    
    # Select at-the-money (ATM) PE strike
    atm_strike = base_strike
    symbol = f"NIFTY {expiry} {atm_strike} PE"
    
    # Verify token exists
    if redis_client.get(f"{username}:{symbol}"):
        return [symbol]
    else:
        print(f"⚠️ PE condition: No token for {symbol}")
        return []
def place_order(smart_api, symbol, token, order_type, price, expiry):
    """
    Place an order through SmartAPI
    
    Args:
        smart_api: SmartAPI instance
        symbol: Option symbol
        token: Option token
        order_type: "BUY" or "SELL"
        price: Option price
        expiry: Expiry date in format "21MAY25"
    """
    print(f"Placing {order_type} order for {symbol} at {price}")
    
    try:
        # Determine exchange based on symbol
        exchange = "NFO"  # National Stock Exchange F&O
        
        # Determine quantity (lot size for Nifty is typically 50)
        quantity = 50
        
        # Order parameters
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": order_type,
            "exchange": exchange,
            "ordertype": "MARKET",  # Market order
            "producttype": "INTRADAY",  # Intraday order
            "duration": "DAY",
            "price": "0",  # For market orders
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(quantity)
        }
        
        print("Order parameters:", order_params)
        
        # Place the order
        order_response = smart_api.placeOrder(order_params)
        print(f"Order placed successfully: {order_response}")
        
        # Store the order details in Redis for tracking
        order_id = order_response.get('data', {}).get('orderid')
        if order_id:
            order_key = f"order:{symbol}:{datetime.now().strftime('%Y%m%d%H%M%S')}"
            order_data = {
                "order_id": order_id,
                "symbol": symbol,
                "token": token,
                "type": order_type,
                "price": price,
                "quantity": quantity,
                "status": "PLACED",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "expiry": expiry
            }
            # Store as hash
            for k, v in order_data.items():
                redis_client.hset(order_key, k, v)
            
            print(f"Order details stored in Redis with key: {order_key}")
            
    except Exception as e:
        print(f"Error placing order: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # For testing the module directly
    expiry = redis_client.hget("date", "expiry").decode('utf-8')
    condition(expiry=expiry)