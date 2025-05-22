# conditions.py
import redis
from datetime import datetime

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
redis_client = redis.StrictRedis(**REDIS_CONFIG)

def get_ce_symbols_from_condition(expiry: str, username: str) -> list:
    """
    Returns a list of CE option symbols based on strategy conditions.
    """
    try:
        ltp = redis_client.get(f"{username}:NIFTY_LTP")
        if not ltp:
            print("❌ LTP not found in Redis for CE symbol selection")
            return []
        ltp = float(ltp.decode('utf-8'))

        base_strike = round(ltp / 50) * 50
        ce_symbol_key = f"NIFTY {expiry} {int(base_strike)} CE"
        trading_symbol = redis_client.get(f"{username}:format:{ce_symbol_key}")

        if not trading_symbol:
            print(f"❌ No trading symbol found for {ce_symbol_key}")
            return []
        
        trading_symbol = trading_symbol.decode('utf-8')
        print(f"✅ Selected CE symbol: {trading_symbol}")
        return [trading_symbol]

    except Exception as e:
        print(f"❌ Error in get_ce_symbols_from_condition: {e}")
        return []

def get_pe_symbols_from_condition(expiry: str, username: str) -> list:
    """
    Returns a list of PE option symbols based on strategy conditions.
    """
    try:
        ltp = redis_client.get(f"{username}:NIFTY_LTP")
        if not ltp:
            print("❌ LTP not found in Redis for PE symbol selection")
            return []
        ltp = float(ltp.decode('utf-8'))

        base_strike = round(ltp / 50) * 50
        pe_symbol_key = f"NIFTY {expiry} {int(base_strike)} PE"
        trading_symbol = redis_client.get(f"{username}:format:{pe_symbol_key}")

        if not trading_symbol:
            print(f"❌ No trading symbol found for {pe_symbol_key}")
            return []
        
        trading_symbol = trading_symbol.decode('utf-8')
        print(f"✅ Selected PE symbol: {trading_symbol}")
        return [trading_symbol]

    except Exception as e:
        print(f"❌ Error in get_pe_symbols_from_condition: {e}")
        return []