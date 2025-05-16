# # app.py

import a

def run_app():
    a.start_redis_server()
    expiry = "01JAN25"
    a.r.hset("date", "expiry", expiry)

    # Initialize SmartConnect
    a.init_smartconnect("92XQzi0N", "Y119175", "1990", "ZDWHGV4NBSWFSJXJGJAZRAVCJQ")

    # Store tokens
    a.storeTokens(expiry)

    # Example: fetch history
    token = a.r.hget("token_data", f"NIFTY{expiry}22000CE")
    if token:
        token = token.decode()
        data = a.history(token)
        print(data)

if __name__ == "__main__":
    run_app()