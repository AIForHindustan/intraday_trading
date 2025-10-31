# CRITICAL: This is a core configuration file - NEVER REMOVE
# Used by the trading system for Zerodha API configuration

from kiteconnect import KiteConnect
import os
from dotenv import load_dotenv
import sys
import json
from datetime import datetime
from pathlib import Path

load_dotenv()

class ZerodhaConfig:
    @staticmethod
    def get_kite_instance():
        # Load token data directly from JSON file
        try:
            with open(ZerodhaConfig.TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
                api_key = token_data.get('api_key')
                access_token = token_data.get('access_token')
        except FileNotFoundError:
            print(f"Token file not found: {ZerodhaConfig.TOKEN_FILE}")
            print("Please run 'python config/zerodha_config.py login' to generate one.")
            sys.exit(1)

        if not access_token or access_token == "YOUR_ACCESS_TOKEN":
            print("Access token not found or is a placeholder.")
            print("Please run 'python config/zerodha_config.py login' to generate one.")
            sys.exit(1)

        kite = KiteConnect(api_key=api_key)
        try:
            kite.set_access_token(access_token)
            # Check if the access token is valid
            kite.profile()
        except Exception as e:
            print(f"Error setting access token: {e}")
            print("Your access token might be expired or invalid.")
            print("Please run 'python config/zerodha_config.py login' to generate a new one.")
            sys.exit(1)

        return kite

    @staticmethod
    def generate_login_url():
        api_key = os.environ.get("ZERODHA_API_KEY")
        if not api_key or api_key == "your_api_key_here":
            print("ZERODHA_API_KEY not found or is a placeholder in your .env file.")
            sys.exit(1)
        kite = KiteConnect(api_key=api_key)
        print("Please open the following URL in your browser and log in:")
        print(kite.login_url())

    @staticmethod
    def generate_access_token(request_token):
        """
        Generate access token from request token and update JSON file.

        Uses credentials from existing zerodha_token.json file.

        Args:
            request_token (str): Request token obtained from Zerodha login flow
        """
        # Load credentials from existing zerodha_token.json
        try:
            with open(ZerodhaConfig.TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            api_key = token_data.get('api_key')
            api_secret = token_data.get('api_secret')
        except Exception as e:
            print(f"Failed to load credentials from zerodha_token.json: {e}")
            sys.exit(1)

        if not api_key or not api_secret:
            print("API credentials not found in zerodha_token.json")
            sys.exit(1)

        kite = KiteConnect(api_key=api_key)
        try:
            data = kite.generate_session(request_token, api_secret=api_secret)
            access_token = data["access_token"]
            user_id = data.get("user_id", "Unknown")
            print(f"Access token generated successfully: {access_token}")

            # Update the JSON file only (we're not using .env)
            token_data = {
                "access_token": access_token,
                "api_key": api_key,
                "api_secret": api_secret,
                "created_at": datetime.now().isoformat(),
                "user_id": user_id
            }

            token_path = ZerodhaConfig.TOKEN_FILE
            with open(token_path, 'w') as f:
                json.dump(token_data, f, indent=2)
            print("✅ Successfully updated the zerodha_token.json file with the new access token.")

        except Exception as e:
            print(f"Error generating access token: {e}")

    @staticmethod
    def get_token_data():
        """Load token data from zerodha_token.json"""
        try:
            with open(ZerodhaConfig.TOKEN_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load token data: {e}")
            return None

    TOKEN_FILE = Path(__file__).parent / "zerodha_token.json"

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "login":
            ZerodhaConfig.generate_login_url()
        elif sys.argv[1] == "token" and len(sys.argv) > 2:
            ZerodhaConfig.generate_access_token(sys.argv[2])
        else:
            print("Invalid command. Usage:")
            print("  python config/zerodha_config.py login")
            print("  python config/zerodha_config.py token <request_token>")
    else:
        print("Invalid command. Usage:")
        print("  python config/zerodha_config.py login")
        print("  python config/zerodha_config.py token <request_token>")
