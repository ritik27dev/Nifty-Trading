import redis
import time
from datetime import datetime
from SmartApi import SmartConnect

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def condition(expiry):
    print("Starting PE condition check...")
    
    # Get SmartAPI instance from global scope or recreate it
    smart_api = SmartConnect(api_key="92XQzi0N")
    
    # For testing purposes, force buy signal to True
    buy_signal = True
    print("Buy signal condition forcibly set to True for testing.")
    
    # Record buy timestamp
    buy_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Buy signal timestamp: {buy_timestamp}")
    
    if buy_signal:
        # Get the current market price for base underlying (Nifty)
        ltp_token = "99926000"  # Nifty 50 token
        try:
            ltp_data = smart_api.ltpData("NSE", "Nifty 50", ltp_token)
            nifty_ltp = int(ltp_data['data']['ltp'])
            atm_strike = round(nifty_ltp / 50) * 50
            print(f"Current Nifty price: {nifty_ltp}, ATM strike: {atm_strike}")
            
            # Choose the option strike to trade (ATM)
            strike = atm_strike
            option_key = f"NIFTY {expiry} {strike} PE"
            
            # Get the token for the option
            option_token = redis_client.get(option_key)
            if not option_token:
                print(f"❌ Token not found for {option_key}")
                
                # Try to check what's available in Redis
                print("Available keys in Redis:")
                for key in redis_client.keys("NIFTY*"):
                    print(f"- {key.decode('utf-8')}")
                
                # Try alternative formats if needed
                format_key = f"format:{option_key}"
                original_format = redis_client.get(format_key)
                if original_format:
                    option_key = original_format.decode('utf-8')
                    option_token = redis_client.get(option_key)
                    if option_token:
                        print(f"Found token using original format: {option_key}")
                else:
                    print("Cannot proceed without option token")
                    return
            
            option_token = option_token.decode('utf-8')
            
            # Get the current market price for the option
            option_ltp_data = smart_api.ltpData("NFO", option_key, option_token)
            option_ltp = option_ltp_data['data']['ltp']
            print(f"Option {option_key} current price: {option_ltp}")
            
            # Place the order
            place_order(smart_api, option_key, option_token, "BUY", option_ltp, expiry)
            
        except Exception as e:
            print(f"Error in PE condition: {e}")
            import traceback
            traceback.print_exc()
    
    print("PE condition check completed.")

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