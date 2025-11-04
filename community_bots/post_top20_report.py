#!/usr/bin/env python3
"""
Post Top 20 Alert Validation Report to Telegram and Reddit
Uses existing community_manager infrastructure
"""

import asyncio
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from community_bots.community_manager import AIONCommunityManager
from community_bots.telegram_bot import AIONTelegramBot
from community_bots.reddit_bot import AIONRedditBot
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def post_top20_report():
    """Post top 20 alert validation report to Telegram and Reddit"""
    try:
        # Load the formatted reports
        telegram_report_path = Path(__file__).parent / 'telegram_report.txt'
        reddit_report_path = Path(__file__).parent / 'reddit_report.txt'
        
        if not telegram_report_path.exists() or not reddit_report_path.exists():
            logger.error("Report files not found. Please generate them first.")
            return False
        
        # Read Telegram report
        with open(telegram_report_path, 'r') as f:
            telegram_message = f.read()
        
        # Read Reddit report
        with open(reddit_report_path, 'r') as f:
            reddit_content = f.read()
            lines = reddit_content.split('\n')
            reddit_title = lines[0] if lines else "Top 20 Alert Validation Report"
            reddit_body = '\n'.join(lines[2:]) if len(lines) > 2 else reddit_content
        
        logger.info("📤 Posting Top 20 Alert Validation Report...")
        
        # Initialize bots directly (using existing infrastructure)
        telegram_bot = AIONTelegramBot()
        reddit_bot = AIONRedditBot()
        
        # Post to Telegram
        telegram_success = False
        if telegram_bot.bot_token:
            telegram_success = await telegram_bot.send_message(telegram_message)
            if telegram_success:
                logger.info("✅ Posted to Telegram successfully")
            else:
                logger.warning("⚠️ Failed to post to Telegram")
        else:
            logger.warning("⚠️ Telegram bot token not configured")
        
        # Post to Reddit
        reddit_success = False
        if reddit_bot.config.get('client_id'):
            reddit_success = await reddit_bot.post_update(title=reddit_title, text=reddit_body, flair="Performance Report")
            if reddit_success:
                logger.info("✅ Posted to Reddit successfully")
            else:
                logger.warning("⚠️ Failed to post to Reddit")
        else:
            logger.warning("⚠️ Reddit bot not configured")
        
        # Summary
        if telegram_success or reddit_success:
            logger.info("=" * 70)
            logger.info("📊 REPORT POSTING SUMMARY:")
            logger.info(f"  Telegram: {'✅ Posted' if telegram_success else '❌ Failed'}")
            logger.info(f"  Reddit: {'✅ Posted' if reddit_success else '❌ Failed'}")
            logger.info("=" * 70)
            return True
        else:
            logger.error("❌ Failed to post report to any platform")
            return False
            
    except Exception as e:
        logger.error(f"Error posting report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = asyncio.run(post_top20_report())
    sys.exit(0 if success else 1)

