import json
import os
import pyotp
from SmartApi import SmartConnect  # Import SmartConnect from Angel Broking API

def load_credentials():
    if os.path.exists('user_credentials.json') and os.path.getsize('user_credentials.json') > 0:
        with open('user_credentials.json', 'r') as file:
            return json.load(file)
    else:
        return {"users": []}

def save_credentials(credentials):
    with open('user_credentials.json', 'w') as file:
        json.dump(credentials, file, indent=4)

def user_exists(credentials, client_id):
    for user in credentials['users']:
        if user['client_id'] == client_id:
            return True
    return False

def validate_credentials(api_key, client_id, pin, totp_secret): # Renamed totp to totp_secret for clarity
    try:
        obj = SmartConnect(api_key=api_key)
        # Generate TOTP using the secret for validation
        generated_totp = pyotp.TOTP(totp_secret).now()
        data = obj.generateSession(client_id, pin, generated_totp)
        if data.get("status") == True or data.get("status") == "Ok": # Check for "Ok" as well
            print(f"✅ Credentials validated successfully for Client ID: {client_id}")
            return True
        else:
            print(f"❌ Credential validation failed for Client ID {client_id}: {data.get('message', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"❌ Credential validation failed due to an exception: {e}")
        return False

def add_user(credentials):
    username = input("Enter username: ")
    client_id = input("Enter client_id: ")
    
    if user_exists(credentials, client_id):
        print(f"User with Client ID {client_id} already exists!\n")
        return

    pin = input("Enter PIN: ")
    api_key = input("Enter API Key: ")
    totp_secret = input("Enter TOTP Secret: ") # Prompt for the secret, not a one-time code

    # Validate credentials before saving
    if validate_credentials(api_key, client_id, pin, totp_secret):
        credentials['users'].append({
            "username" : username,
            "client_id": client_id,
            "pin": pin,
            "api_key": api_key,
            "totp": totp_secret # Store the TOTP secret
        })

        save_credentials(credentials)
        print(f"User {client_id} added successfully!\n")
    else:
        print(f"Failed to add user {client_id}: Invalid credentials or an error occurred during validation.\n")

def main():
    credentials = load_credentials()

    while True:
        print("Menu:")
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