# conditions.py

import redis

# Redis client initialization (can be shared or re-initialized if this file runs independently)
# For this setup, assuming app.py initiates the run, this re-initialization is fine.
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}
redis_client = redis.StrictRedis(**REDIS_CONFIG)

def get_ce_symbols_from_condition(expiry, username):
    """
    Determines CE symbols to trade based on a condition (e.g., ATM strike).
    Fetches required data (LTP) from Redis.
    
    Args:
        expiry (str): The expiry date in 'DDMONYY' format.
        username (str): The username (for Redis key prefixing).
        
    Returns:
        list: A list of symbol key formats (e.g., ["NIFTY 25MAY22 17500 CE"])
              that meet the CE condition.
    """
    ltp_key = f"{username}:NIFTY_LTP"
    ltp_bytes = redis_client.get(ltp_key)
    
    if not ltp_bytes:
        print(f"⚠️ No LTP found in Redis for {username} for CE condition. Please ensure LTP was fetched and stored.")
        return []
    
    try:
        ltp = int(float(ltp_bytes.decode('utf-8')))
    except ValueError:
        print(f"❌ Invalid LTP value stored in Redis for {username}: {ltp_bytes.decode('utf-8')}. Skipping CE condition.")
        return []

    # Calculate ATM strike (assuming Nifty strikes are multiples of 50)
    atm_strike = round(ltp / 50) * 50
    
    # Construct the symbol key format used in Redis (e.g., "NIFTY 25MAY22 17500 CE")
    symbol_format = f"NIFTY {expiry} {atm_strike} CE"
    
    # Check if this specific symbol's token exists in Redis
    if redis_client.exists(f"{username}:{symbol_format}"):
        print(f"✅ CE condition met: Identified {symbol_format}")
        return [symbol_format]
    else:
        print(f"⚠️ CE condition not met: Token for {symbol_format} not found in Redis.")
        return []

def get_pe_symbols_from_condition(expiry, username):
    """
    Determines PE symbols to trade based on a condition (e.g., ATM strike).
    Fetches required data (LTP) from Redis.
    
    Args:
        expiry (str): The expiry date in 'DDMONYY' format.
        username (str): The username (for Redis key prefixing).
        
    Returns:
        list: A list of symbol key formats (e.g., ["NIFTY 25MAY22 17500 PE"])
              that meet the PE condition.
    """
    ltp_key = f"{username}:NIFTY_LTP"
    ltp_bytes = redis_client.get(ltp_key)
    
    if not ltp_bytes:
        print(f"⚠️ No LTP found in Redis for {username} for PE condition. Please ensure LTP was fetched and stored.")
        return []
    
    try:
        ltp = int(float(ltp_bytes.decode('utf-8')))
    except ValueError:
        print(f"❌ Invalid LTP value stored in Redis for {username}: {ltp_bytes.decode('utf-8')}. Skipping PE condition.")
        return []

    # Calculate ATM strike (assuming Nifty strikes are multiples of 50)
    atm_strike = round(ltp / 50) * 50
    
    # Construct the symbol key format used in Redis
    symbol_format = f"NIFTY {expiry} {atm_strike} PE"
    
    # Check if this specific symbol's token exists in Redis
    if redis_client.exists(f"{username}:{symbol_format}"):
        print(f"✅ PE condition met: Identified {symbol_format}")
        return [symbol_format]
    else:
        print(f"⚠️ PE condition not met: Token for {symbol_format} not found in Redis.")
        return []

# No __main__ block or direct execution logic here.
# This file is purely for functions to be imported.