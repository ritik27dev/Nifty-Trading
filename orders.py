# orders.py

import json
import redis
from datetime import datetime

# Redis client initialization (can be shared or re-initialized if this file runs independently)
# For this setup, assuming app.py initiates the run, this re-initialization is fine.
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
redis_client = redis.StrictRedis(**REDIS_CONFIG)

def place_option_order(smart_api_conn, username, symbol_key_format, quantity_multiplier=1, transaction_type="BUY"):
    """
    Places an option order using the provided SmartConnect instance and user details.
    This function expects an already authenticated SmartConnect object.
    
    Args:
        smart_api_conn: An **authenticated** SmartConnect object for the user.
        username (str): The username for Redis keys (e.g., 'yogesh').
        symbol_key_format (str): The Redis key format for the symbol 
                                 (e.g., "NIFTY 25MAY22 17500 CE").
        quantity_multiplier (int): How many lots to trade (e.g., 1 for 1 lot, 2 for 2 lots).
        transaction_type (str): "BUY" or "SELL".
        
    Returns:
        tuple: (True, order_id) on success, (False, error_code/message) on failure.
    """
    # Retrieve necessary details from Redis
    trading_symbol_from_redis = redis_client.get(f"{username}:format:{symbol_key_format}")
    token = redis_client.get(f"{username}:{symbol_key_format}")
    lot_size_str = redis_client.get(f"{username}:lotsize:{symbol_key_format}")

    # Validate that all required data is present
    if not trading_symbol_from_redis or not token or not lot_size_str:
        print(f"❌ Order data (trading symbol, token, or lot size) not found in Redis for: {symbol_key_format}")
        return False, "MISSING_DATA_IN_REDIS"

    try:
        # Decode and convert lot size to integer
        lot_size = int(lot_size_str.decode('utf-8'))
        total_quantity = lot_size * quantity_multiplier

        # Construct order parameters dictionary
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol_from_redis.decode('utf-8'),
            "symboltoken": token.decode('utf-8'),
            "transactiontype": transaction_type,
            "exchange": "NFO", # Nifty options are traded on NFO
            "ordertype": "MARKET", # Placing market orders for simplicity
            "producttype": "INTRADAY", # Could be 'CNC' for delivery, 'NRML' for carry forward
            "duration": "DAY", # Order valid for the day
            "quantity": str(total_quantity), # Quantity must be a string for Angel One API
            "price": "0", # Price is 0 for market orders
            "squareoff": "0", # For bracket/cover orders, not used in normal market order
            "stoploss": "0" # For bracket/cover orders
        }
        
        print(f"🔍 Debug: Order Payload for {username} ({symbol_key_format}): {json.dumps(order_params, indent=2)}")

        # Place the order using the provided SmartConnect instance
        response = smart_api_conn.placeOrder(order_params)
        
        # --- CRITICAL ERROR HANDLING FOR ANGEL ONE API RESPONSES ---
        # Angel One API can return raw strings for some rejections instead of JSON
        if isinstance(response, str):
            # Known direct error strings (like session issues or immediate rejections)
            if response in ["AG8001", "052192f1cc61AO", "052145583c8bAO", "05218ee5ffedAO"]: # Add any other specific error strings you encounter
                print(f"❌ API returned direct error string: {response}. This is often a session or immediate validation error.")
                return False, response
            try:
                # If it's a string but not a known direct error, try to parse it as JSON
                response = json.loads(response)
            except json.JSONDecodeError:
                print(f"❌ placeOrder returned unparseable string (not recognized as direct error): {response}")
                return False, f"UNPARSEABLE_RESPONSE: {response}"
        
        # After attempting to parse, ensure response is now a dictionary
        if response is None or not isinstance(response, dict):
            print(f"❌ API returned unexpected response type for order placement (not a dict): {response}")
            return False, "API_RESPONSE_UNEXPECTED_TYPE"

        # Check the 'status' key in the response dictionary for success
        # Angel One API can return True, "Ok", or "success" for status
        if response.get('status') in [True, "Ok", "success"]:
            # Successfully placed order, extract order ID
            order_id = response.get('data', {}).get('orderid')
            if order_id:
                print(f"✅ Order placed successfully for {trading_symbol_from_redis.decode('utf-8')}: Order ID {order_id}")
                
                # Store relevant order details in Redis for tracking
                order_key = f"order:{order_id}"
                order_data = {
                    "order_id": order_id,
                    "symbol": symbol_key_format, # Original format used for Redis key
                    "token": token.decode('utf-8'),
                    "type": transaction_type,
                    "quantity": str(total_quantity), # Actual quantity placed
                    "status": "PLACED",
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "expiry": symbol_key_format.split(' ')[1], # Extract expiry from key format
                    "username": username # Store username for multi-user tracking
                }
                for k, v in order_data.items():
                    redis_client.hset(order_key, k, v)
                print(f"🧠 Stored order details in Redis: {order_key}")
                return True, order_id
            else:
                error_message = response.get('message', 'Order ID not found in success response.')
                error_code = response.get('errorCode', 'NO_ORDER_ID')
                print(f"❌ Order placed but no order ID found in response: {error_message} (Code: {error_code})")
                return False, error_code
        else:
            # Order placement failed as per API response
            error_message = response.get('message', 'Unknown error during order placement.')
            error_code = response.get('errorCode', 'UNKNOWN_ERROR_CODE')
            print(f"❌ Order placement failed for {trading_symbol_from_redis.decode('utf-8')}: {error_message} (Code: {error_code})")
            return False, error_code
    except Exception as e:
        print(f"❌ An unexpected error occurred while trying to place order for {symbol_key_format}: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e)

# No __main__ block or direct execution logic here.