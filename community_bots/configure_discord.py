#!/usr/bin/env python3
"""
Helper script to configure Discord bot token and channel ID
"""

import json
from pathlib import Path

def configure_discord():
    """Interactive configuration of Discord bot"""
    config_path = Path(__file__).parent.parent / "alerts" / "config" / "discord_config.json"
    
    print("=" * 60)
    print("Discord Bot Configuration")
    print("=" * 60)
    print("\nTo get your Discord bot token:")
    print("1. Go to https://discord.com/developers/applications")
    print("2. Select your application (App ID: 1434422808016064584)")
    print("3. Go to 'Bot' section")
    print("4. Click 'Reset Token' or 'Copy' to get your bot token")
    print("\nTo get your Discord channel ID:")
    print("1. Enable Developer Mode in Discord (User Settings > Advanced > Developer Mode)")
    print("2. Right-click on your Discord channel")
    print("3. Click 'Copy ID'")
    print("\n" + "=" * 60)
    
    # Load existing config
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except:
        config = {
            "app_id": "1434422808016064584",
            "public_key": "b7ef420d47630a19d859e14216e15723c804865670d1c85a658bc0cf5b62f3f2",
            "bot_token": "",
            "channel_id": "",
            "description": "AION Discord Bot for community updates and alerts"
        }
    
    # Get bot token
    current_token = config.get('bot_token', '')
    if current_token:
        print(f"\nCurrent bot token: {current_token[:10]}...{current_token[-5:]}")
        use_current = input("Use current token? (y/n): ").strip().lower()
        if use_current != 'y':
            bot_token = input("\nEnter Discord bot token: ").strip()
            if bot_token:
                config['bot_token'] = bot_token
    else:
        bot_token = input("\nEnter Discord bot token: ").strip()
        if bot_token:
            config['bot_token'] = bot_token
    
    # Get channel ID
    current_channel = config.get('channel_id', '')
    if current_channel:
        print(f"\nCurrent channel ID: {current_channel}")
        use_current = input("Use current channel ID? (y/n): ").strip().lower()
        if use_current != 'y':
            channel_id = input("\nEnter Discord channel ID: ").strip()
            if channel_id:
                config['channel_id'] = channel_id
    else:
        channel_id = input("\nEnter Discord channel ID: ").strip()
        if channel_id:
            config['channel_id'] = channel_id
    
    # Save config
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print("\n✅ Discord configuration saved!")
    print(f"   Bot Token: {'✓ Set' if config.get('bot_token') else '✗ Not set'}")
    print(f"   Channel ID: {'✓ Set' if config.get('channel_id') else '✗ Not set'}")
    print("\nYou can now run: python community_bots/post_kow_signal.py --kow-signal")

if __name__ == "__main__":
    configure_discord()

