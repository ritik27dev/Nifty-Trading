import json
import os
import pyotp
from SmartApi.smartConnect import SmartConnect

CREDENTIALS_FILE = 'user_credentials.json'

def load_credentials():
    if os.path.exists(CREDENTIALS_FILE) and os.path.getsize(CREDENTIALS_FILE) > 0:
        with open(CREDENTIALS_FILE, 'r') as file:
            return json.load(file)
    return {"users": []}

def save_credentials(credentials):
    with open(CREDENTIALS_FILE, 'w') as file:
        json.dump(credentials, file, indent=4)

def user_exists(credentials, client_id):
    return any(user['client_id'] == client_id for user in credentials['users'])

def validate_credentials(api_key, client_id, pin, totp):
    try:
        obj = SmartConnect(api_key=api_key)
        otp = pyotp.TOTP(totp).now()
        data = obj.generateSession(client_id, pin, otp)
        return data.get("status", False)
    except Exception as e:
        print(f"Credential validation failed: {e}")
        return False

def add_user(credentials):
    username = input("Enter username: ")
    client_id = input("Enter client_id: ")
    if user_exists(credentials, client_id):
        print(f"User {client_id} already exists!\n")
        return

    pin = input("Enter PIN: ")
    api_key = input("Enter API Key: ")
    totp = input("Enter TOTP: ")

    if validate_credentials(api_key, client_id, pin, totp):
        credentials['users'].append({
            "username": username,
            "client_id": client_id,
            "pin": pin,
            "api_key": api_key,
            "totp": totp
        })
        save_credentials(credentials)
        print(f"User {client_id} added successfully!\n")
    else:
        print(f"Failed to add user {client_id}: Invalid credentials.\n")

def main():
    credentials = load_credentials()
    while True:
        print("\nMenu:")
        print("1. Add User")
        print("2. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            add_user(credentials)
        elif choice == '2':
            print("Exiting...")
            break
        else:
            print("Invalid choice! Please try again.")

if __name__ == "__main__":
    main()