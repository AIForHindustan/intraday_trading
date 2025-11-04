"""
AION Discord Bot
Sends alerts and updates to Discord channels
"""

import asyncio
import aiohttp
import json
import logging
import os
from typing import Dict, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)

class AIONDiscordBot:
    def __init__(self):
        # Load Discord configuration
        config_path = Path(__file__).parent.parent / "alerts" / "config" / "discord_config.json"
        
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            self.app_id = self.config.get('app_id')
            self.public_key = self.config.get('public_key')
            self.bot_token = self.config.get('bot_token')  # Bot token for API calls
            self.channel_id = self.config.get('channel_id', '')  # Default channel ID
        except Exception as e:
            logger.error(f"Could not load Discord config: {e}")
            self.config = {}
            self.app_id = None
            self.public_key = None
            self.bot_token = None
            self.channel_id = ''
        
        # Check environment variables as fallback
        if not self.bot_token:
            self.bot_token = os.getenv('DISCORD_TOKEN') or os.getenv('DISCORD_BOT_TOKEN')
        
        if not self.channel_id:
            self.channel_id = os.getenv('DISCORD_CHANNEL_ID', '')
        
        if not self.bot_token:
            logger.warning("No bot token found in discord_config.json or environment variables")
            logger.info("To configure Discord bot:")
            logger.info("  1. Get bot token from https://discord.com/developers/applications")
            logger.info("  2. Add 'bot_token' to discord_config.json")
            logger.info("  3. Add 'channel_id' (right-click channel > Copy ID) to discord_config.json")
        
        self.base_url = "https://discord.com/api/v10"
        
    async def send_message(self, message: str, channel_id: Optional[str] = None) -> bool:
        """Send message to Discord channel"""
        if not self.bot_token:
            logger.error("No bot token configured")
            return False
            
        channel_id = channel_id or self.channel_id
        
        if not channel_id:
            logger.error("No channel ID provided")
            return False
        
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/channels/{channel_id}/messages"
                headers = {
                    "Authorization": f"Bot {self.bot_token}",
                    "Content-Type": "application/json"
                }
                data = {
                    "content": message
                }
                
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        logger.info(f"Message sent to Discord channel {channel_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to send Discord message: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error sending Discord message: {e}")
            return False
    
    async def send_embed(self, title: str, description: str, fields: List[Dict] = None, 
                         channel_id: Optional[str] = None, color: int = 0x00ff00) -> bool:
        """Send embedded message to Discord channel"""
        if not self.bot_token:
            logger.error("No bot token configured")
            return False
            
        channel_id = channel_id or self.channel_id
        
        if not channel_id:
            logger.error("No channel ID provided")
            return False
        
        try:
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields or [],
                "timestamp": None
            }
            
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/channels/{channel_id}/messages"
                headers = {
                    "Authorization": f"Bot {self.bot_token}",
                    "Content-Type": "application/json"
                }
                data = {
                    "embeds": [embed]
                }
                
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        logger.info(f"Embed sent to Discord channel {channel_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to send Discord embed: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error sending Discord embed: {e}")
            return False
    
    async def send_educational_content(self, content: Dict) -> bool:
        """Send educational content to Discord"""
        try:
            title = content.get('title', 'Educational Content')
            body = content.get('content', content.get('description', ''))
            
            # Discord has a 2000 character limit for messages
            # If content is too long, split into multiple messages
            max_length = 1900  # Leave some buffer
            
            if len(body) <= max_length:
                message = f"**{title}**\n\n{body}"
                return await self.send_message(message)
            else:
                # Send title first
                await self.send_message(f"**{title}**")
                
                # Split content into chunks
                chunks = [body[i:i+max_length] for i in range(0, len(body), max_length)]
                for chunk in chunks:
                    await self.send_message(chunk)
                    await asyncio.sleep(0.5)  # Rate limiting
                
                return True
                
        except Exception as e:
            logger.error(f"Error sending educational content to Discord: {e}")
            return False
    
    async def send_validation_result(self, validation_data: Dict) -> bool:
        """Send validation result to Discord channels"""
        try:
            symbol = validation_data.get('symbol', 'UNKNOWN')
            confidence = validation_data.get('confidence', 0.0)
            message = validation_data.get('message', '')
            
            if not message:
                message = f"Validation result for {symbol}: {confidence:.1%} confidence"
            
            # Format as embed
            fields = [
                {"name": "Symbol", "value": symbol, "inline": True},
                {"name": "Confidence", "value": f"{confidence:.1%}", "inline": True},
            ]
            
            color = 0x00ff00 if confidence >= 0.90 else 0xffaa00 if confidence >= 0.70 else 0xff0000
            
            return await self.send_embed(
                title=f"🎯 Validation Result: {symbol}",
                description=message[:2000] if len(message) <= 2000 else message[:1997] + "...",
                fields=fields,
                color=color
            )
            
        except Exception as e:
            logger.error(f"Error sending validation result to Discord: {e}")
            return False
    
    async def run_bot(self):
        """Run the Discord bot (placeholder for webhook/event handling)"""
        logger.info("Discord bot started")
        while True:
            await asyncio.sleep(60)  # Keep bot alive


if __name__ == "__main__":
    # Test Discord bot
    bot = AIONDiscordBot()
    asyncio.run(bot.send_message("Test message from AION Discord Bot"))

