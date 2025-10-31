"""
AION Community Manager
Orchestrates Reddit and Telegram bots for comprehensive community management
"""

import asyncio
import json
import redis
import schedule
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import os

# Import bot modules
from reddit_bot import AIONRedditBot
from telegram_bot import AIONTelegramBot

# Community Manager Configuration
COMMUNITY_CONFIG = {
    'redis_host': 'localhost',
    'redis_port': 6379,
    'redis_db': 0,
    'sync_interval': 300,  # 5 minutes
    'performance_update_interval': 3600,  # 1 hour
    'educational_content_interval': 7200,  # 2 hours
    'market_update_interval': 1800,  # 30 minutes
}

class AIONCommunityManager:
    def __init__(self):
        # Redis connection
        self.redis_client = redis.Redis(
            host=COMMUNITY_CONFIG['redis_host'],
            port=COMMUNITY_CONFIG['redis_port'],
            db=COMMUNITY_CONFIG['redis_db']
        )
        
        # Initialize bots
        self.reddit_bot = AIONRedditBot()
        self.telegram_bot = AIONTelegramBot()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Community metrics
        self.community_metrics = {
            'total_alerts_sent': 0,
            'reddit_engagement': 0,
            'telegram_engagement': 0,
            'educational_content_shared': 0,
            'performance_reports_posted': 0
        }
    
    async def start_community_bots(self):
        """Start all community bots"""
        try:
            self.logger.info("Starting AION Community Manager...")
            
            # Start Reddit bot scheduler
            reddit_task = asyncio.create_task(self.run_reddit_scheduler())
            self.logger.info("Reddit bot scheduler started")
            
            # Start Telegram bot
            telegram_task = asyncio.create_task(self.telegram_bot.run_bot())
            self.logger.info("Telegram bot started")
            
            # Start community manager tasks
            manager_task = asyncio.create_task(self.run_community_manager())
            self.logger.info("Community manager started")
            
            # Start validation results consumer
            validation_task = asyncio.create_task(self.run_validation_consumer())
            self.logger.info("Validation results consumer started")
            
            # Wait for all tasks
            await asyncio.gather(reddit_task, telegram_task, manager_task, validation_task)
            
        except Exception as e:
            self.logger.error(f"Error starting community bots: {e}")
    
    async def run_reddit_scheduler(self):
        """Run Reddit bot scheduler"""
        try:
            while True:
                # Check for scheduled posts
                if schedule.jobs:
                    schedule.run_pending()
                
                await asyncio.sleep(60)  # Check every minute
                
        except Exception as e:
            self.logger.error(f"Error in Reddit scheduler: {e}")
    
    async def run_community_manager(self):
        """Run community manager tasks"""
        try:
            while True:
                # Sync data across platforms
                await self.sync_community_data()
                
                # Update performance metrics
                await self.update_performance_metrics()
                
                # Share educational content
                await self.share_educational_content()
                
                # Update market information
                await self.update_market_information()
                
                # Wait before next iteration
                await asyncio.sleep(COMMUNITY_CONFIG['sync_interval'])
                
        except Exception as e:
            self.logger.error(f"Error in community manager: {e}")
    
    async def run_validation_consumer(self):
        """Consume validation results from Redis streams and pub/sub (separate from scanner)"""
        try:
            # Method 1: Subscribe to pub/sub channels (for backwards compatibility)
            pubsub = self.redis_client.pubsub()
            validation_channels = [
                'telegram:validation_results',
                'telegram:high_confidence_alerts',
                'telegram:community_updates',
                'reddit:validation_results'
            ]
            
            for channel in validation_channels:
                pubsub.subscribe(channel)
                self.logger.info(f"📡 Subscribed to validation pub/sub channel: {channel}")
            
            # Method 2: Also consume from Redis stream (primary method for validation results with rolling windows)
            validation_stream = 'alerts:validation:results'
            last_id = '0'  # Start from beginning of stream
            
            self.logger.info("🎯 Validation results consumer started (separate from scanner)")
            self.logger.info(f"📊 Listening to Redis stream: {validation_stream}")
            self.logger.info("📡 Listening to pub/sub channels for validation results")
            
            # Run both consumers concurrently
            pubsub_task = asyncio.create_task(self._consume_pubsub(pubsub))
            stream_task = asyncio.create_task(self._consume_validation_stream(validation_stream, last_id))
            
            # Wait for both tasks
            await asyncio.gather(pubsub_task, stream_task)
                        
        except Exception as e:
            self.logger.error(f"Error in validation consumer: {e}")
    
    async def _consume_pubsub(self, pubsub):
        """Consume validation results from pub/sub channels"""
        try:
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        validation_data = json.loads(message['data'])
                        await self.process_validation_result(validation_data)
                    except Exception as e:
                        self.logger.error(f"Error processing pub/sub validation message: {e}")
        except Exception as e:
            self.logger.error(f"Error in pub/sub consumer: {e}")
    
    async def _consume_validation_stream(self, stream_name: str, last_id: str):
        """Consume validation results from Redis stream (includes rolling windows data)"""
        try:
            while True:
                try:
                    # Read from stream (non-blocking) - use asyncio.to_thread for sync Redis ops
                    import asyncio
                    messages = await asyncio.to_thread(
                        self.redis_client.xread,
                        {stream_name: last_id},
                        count=10,
                        block=1000
                    )
                    
                    if messages:
                        stream_messages = messages[0][1]  # [(id, data), ...]
                        
                        for msg_id, msg_data in stream_messages:
                            last_id = msg_id
                            
                            try:
                                # Convert stream message to validation_data format
                                validation_data = {
                                    'type': 'validation_result',
                                    'alert_id': msg_data.get('alert_id', ''),
                                    'symbol': msg_data.get('symbol', 'UNKNOWN'),
                                    'status': msg_data.get('status', 'INCONCLUSIVE'),
                                    'confidence': self._extract_confidence_from_stream(msg_data),
                                    'message': msg_data.get('human_readable', ''),
                                    'full_result': {
                                        'validation_result': {
                                            'is_valid': msg_data.get('status') == 'SUCCESS',
                                            'confidence_score': self._extract_confidence_from_stream(msg_data),
                                            'rolling_windows': msg_data.get('rolling_windows', {}),
                                            'price_movement_pct': msg_data.get('price_movement_pct', 0.0),
                                            'duration_minutes': msg_data.get('duration_minutes', 0),
                                            'max_move_pct': msg_data.get('max_move_pct', 0.0),
                                            'details': msg_data.get('details', ''),
                                        },
                                        'alert_id': msg_data.get('alert_id'),
                                        'symbol': msg_data.get('symbol'),
                                        'pattern': msg_data.get('pattern', 'unknown'),
                                    },
                                    'rolling_windows': msg_data.get('rolling_windows', {}),
                                    'timestamp': msg_data.get('validation_timestamp_ms', ''),
                                }
                                
                                await self.process_validation_result(validation_data)
                                
                            except Exception as e:
                                self.logger.error(f"Error processing stream validation message: {e}")
                
                except Exception as e:
                    # If stream doesn't exist yet or other errors, wait and retry
                    await asyncio.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"Error in stream consumer: {e}")
    
    def _extract_confidence_from_stream(self, msg_data: Dict) -> float:
        """Extract confidence score from stream message"""
        try:
            # Try to extract from rolling windows or calculate from status
            if 'rolling_windows' in msg_data:
                rolling_windows = msg_data['rolling_windows']
                if isinstance(rolling_windows, str):
                    rolling_windows = json.loads(rolling_windows)
                # Calculate confidence from window success rate
                if isinstance(rolling_windows, dict):
                    total_windows = len(rolling_windows)
                    if total_windows > 0:
                        success_count = sum(1 for w in rolling_windows.values() 
                                          if isinstance(w, dict) and w.get('status') == 'SUCCESS')
                        return success_count / total_windows
            
            # Fallback: confidence based on status
            if msg_data.get('status') == 'SUCCESS':
                return 0.9
            elif msg_data.get('status') == 'FAILURE':
                return 0.1
            else:
                return 0.5
        except:
            return 0.5
    
    async def process_validation_result(self, validation_data: Dict):
        """Process validation result with rolling windows data and distribute to communities"""
        try:
            validation_type = validation_data.get('type', '')
            symbol = validation_data.get('symbol', 'UNKNOWN')
            confidence = validation_data.get('confidence', 0.0)
            message = validation_data.get('message', '')
            full_result = validation_data.get('full_result', {})
            rolling_windows = validation_data.get('rolling_windows', {})
            
            # Extract confidence from full_result if not directly available
            if confidence == 0.0 and full_result:
                validation_result = full_result.get('validation_result', {})
                confidence = validation_result.get('confidence_score', 0.0)
                
                # If still 0, try forward_validation summary
                if confidence == 0.0:
                    forward_validation = validation_result.get('forward_validation', {})
                    summary = forward_validation.get('summary', {})
                    success_ratio = summary.get('success_ratio', 0.0)
                    if success_ratio > 0:
                        confidence = success_ratio
            
            # Only process validation results (90%+ confidence)
            if validation_type == 'validation_result' and confidence >= 0.90:
                self.logger.info(f"🎯 PROCESSING HIGH-CONFIDENCE VALIDATION: {symbol} ({confidence:.1%})")
                
                # Enhance validation_data with rolling windows info
                if rolling_windows and isinstance(rolling_windows, str):
                    try:
                        rolling_windows = json.loads(rolling_windows)
                    except:
                        pass
                
                validation_data['rolling_windows'] = rolling_windows
                
                # Distribute to Telegram
                await self.distribute_to_telegram(validation_data)
                
                # Distribute to Reddit
                await self.distribute_to_reddit(validation_data)
                
                # Update community metrics
                self.update_community_metrics('total_alerts_sent')
                
                self.logger.info(f"✅ Validation result distributed to all communities: {symbol}")
            else:
                self.logger.debug(f"Skipping validation result: {symbol} (confidence: {confidence:.1%})")
                
        except Exception as e:
            self.logger.error(f"Error processing validation result: {e}")
    
    async def distribute_to_telegram(self, validation_data: Dict):
        """Distribute validation result to Telegram channels with rolling windows data"""
        try:
            message = validation_data.get('message', '')
            symbol = validation_data.get('symbol', 'UNKNOWN')
            
            # Format message with rolling windows if not already formatted
            if not message or len(message) < 50:
                message = self._format_validation_message_with_rolling_windows(validation_data)
                validation_data['message'] = message
            
            # Send to main Telegram bot
            await self.telegram_bot.send_validation_result(validation_data)
            
            self.logger.info(f"📡 Validation result sent to Telegram: {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error distributing to Telegram: {e}")
    
    def _format_validation_message_with_rolling_windows(self, validation_data: Dict) -> str:
        """Format validation message with rolling windows analysis"""
        try:
            symbol = validation_data.get('symbol', 'UNKNOWN')
            full_result = validation_data.get('full_result', {})
            validation_result = full_result.get('validation_result', {})
            rolling_windows = validation_data.get('rolling_windows', {})
            
            # Parse rolling windows if string
            if isinstance(rolling_windows, str):
                try:
                    rolling_windows = json.loads(rolling_windows)
                except:
                    rolling_windows = {}
            
            # Extract forward validation data
            forward_validation = validation_result.get('forward_validation', {})
            summary = forward_validation.get('summary', {})
            windows_data = forward_validation.get('windows', {})
            
            is_valid = validation_result.get('is_valid', False)
            confidence_score = validation_result.get('confidence_score', 0.0)
            price_movement = validation_result.get('price_movement_pct', 0.0)
            
            status_emoji = "✅" if is_valid else "❌"
            status_text = "VALID" if is_valid else "REJECTED"
            
            message = f"""🎯 <b>FORWARD VALIDATION RESULT: {symbol}</b>

{status_emoji} <b>Status:</b> {status_text}
📊 <b>Confidence:</b> {confidence_score:.1%}
📈 <b>Price Movement:</b> {price_movement:+.2f}%

<b>Forward Testing Windows:</b>"""
            
            if summary:
                success_count = summary.get('success_count', 0)
                total_windows = summary.get('total_windows', 0)
                success_ratio = summary.get('success_ratio', 0.0)
                max_move = summary.get('max_directional_move_pct', 0.0)
                
                message += f"""
✅ Success: {success_count}/{total_windows} windows
📊 Success Rate: {success_ratio:.1%}
📈 Max Move: {max_move:+.2f}%"""
            
            # Add individual window details if available
            if windows_data:
                message += "\n\n<b>Window Analysis:</b>"
                for window_name, window_result in list(windows_data.items())[:5]:  # Show top 5
                    window_status = window_result.get('status', 'INCONCLUSIVE')
                    window_emoji = "✅" if window_status == "SUCCESS" else "❌" if window_status == "FAILURE" else "⏳"
                    movement = window_result.get('result', {}).get('directional_movement_pct', 0.0)
                    message += f"\n{window_emoji} {window_name}: {movement:+.2f}%"
            
            message += f"\n\n<i>Generated by AION Alert Validator (Forward Testing)</i>"
            
            return message
            
        except Exception as e:
            self.logger.error(f"Error formatting validation message: {e}")
            return f"Validation result for {validation_data.get('symbol', 'UNKNOWN')}"
    
    async def distribute_to_reddit(self, validation_data: Dict):
        """Distribute validation result to Reddit"""
        try:
            symbol = validation_data.get('symbol', 'UNKNOWN')
            confidence = validation_data.get('confidence', 0.0)
            
            # Create Reddit post for high-confidence validation
            reddit_post = self._format_reddit_validation_post(validation_data)
            
            # Post to Reddit (implementation depends on Reddit bot)
            # await self.reddit_bot.post_validation_result(reddit_post)
            
            self.logger.info(f"📡 Validation result prepared for Reddit: {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error distributing to Reddit: {e}")
    
    def _format_reddit_validation_post(self, validation_data: Dict) -> str:
        """Format validation result for Reddit post"""
        try:
            symbol = validation_data.get('symbol', 'UNKNOWN')
            confidence = validation_data.get('confidence', 0.0)
            full_result = validation_data.get('full_result', {})
            
            # Extract validation details
            validation_result = full_result.get('validation_result', {})
            is_valid = validation_result.get('is_valid', False)
            
            volume_validation = validation_result.get('volume_validation', {})
            expected_move_validation = validation_result.get('expected_move_validation', {})
            
            # Format Reddit post
            post = f"""
# 🎯 High-Confidence Alert Validation: {symbol}

**Confidence Level:** {confidence:.1%}
**Validation Status:** {'✅ VALID' if is_valid else '❌ REJECTED'}

## 📊 Validation Details

### Volume Analysis
- **Volume Confidence:** {volume_validation.get('confidence_score', 0):.1%}
- **Volume Valid:** {'Yes' if volume_validation.get('is_valid', False) else 'No'}

### Expected Move Analysis
- **Move Accuracy:** {expected_move_validation.get('accuracy', 0):.1%}
- **Move Confidence:** {expected_move_validation.get('confidence', 0):.1%}
- **Move Valid:** {'Yes' if expected_move_validation.get('is_valid', False) else 'No'}

### Rolling Windows Analyzed
- **Windows:** {validation_result.get('combined_metrics', {}).get('rolling_windows_analyzed', 0)}

---

*This validation result was automatically generated by the AION Alert Validation System with 90%+ confidence threshold.*

**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST
            """.strip()
            
            return post
            
        except Exception as e:
            self.logger.error(f"Error formatting Reddit validation post: {e}")
            return f"Validation result for {symbol}: {confidence:.1%} confidence"
    
    async def sync_community_data(self):
        """Sync data across all platforms"""
        try:
            # Get latest alerts from Redis
            latest_alerts = self.get_latest_alerts()
            
            
            # Sync to Telegram
            for alert in latest_alerts:
                await self.sync_alert_to_telegram(alert)
            
            # Sync to Reddit (for educational content)
            await self.sync_educational_content_to_reddit()
            
            self.logger.info(f"Synced {len(latest_alerts)} alerts across platforms")
            
        except Exception as e:
            self.logger.error(f"Error syncing community data: {e}")
    
    async def update_performance_metrics(self):
        """Update performance metrics across platforms"""
        try:
            # Get performance data
            performance_data = self.get_performance_data()
            
            
            # Update Telegram performance update
            await self.telegram_bot.send_performance_update(performance_data)
            
            # Update Reddit weekly report
            await self.update_reddit_performance_report(performance_data)
            
            self.community_metrics['performance_reports_posted'] += 1
            self.logger.info("Performance metrics updated across platforms")
            
        except Exception as e:
            self.logger.error(f"Error updating performance metrics: {e}")
    
    async def share_educational_content(self):
        """Share educational content across platforms"""
        try:
            # Get educational content
            educational_content = self.get_educational_content()
            
            
            # Share to Telegram
            await self.telegram_bot.send_educational_content(educational_content)
            
            # Share to Reddit
            await self.share_educational_to_reddit(educational_content)
            
            self.community_metrics['educational_content_shared'] += 1
            self.logger.info("Educational content shared across platforms")
            
        except Exception as e:
            self.logger.error(f"Error sharing educational content: {e}")
    
    async def update_market_information(self):
        """Update market information across platforms"""
        try:
            # Get market data
            market_data = self.get_market_data()
            
            
            # Update Telegram market update
            await self.telegram_bot.send_market_update(market_data)
            
            # Update Reddit daily analysis
            await self.update_reddit_market_analysis(market_data)
            
            self.logger.info("Market information updated across platforms")
            
        except Exception as e:
            self.logger.error(f"Error updating market information: {e}")
    
    # Helper Methods
    def get_latest_alerts(self) -> List[Dict]:
        """Get latest alerts from Redis"""
        try:
            alerts = []
            for i in range(10):  # Get last 10 alerts
                alert_key = f"alert:recent:{i}"
                alert_data = self.redis_client.get(alert_key)
                if alert_data:
                    alerts.append(json.loads(alert_data))
            return alerts
        except Exception as e:
            self.logger.error(f"Error getting latest alerts: {e}")
            return []
    
    def get_performance_data(self) -> Dict:
        """Get performance data from Redis"""
        try:
            performance_key = "performance:overall"
            performance_data = self.redis_client.get(performance_key)
            if performance_data:
                return json.loads(performance_data)
            return {}
        except Exception as e:
            self.logger.error(f"Error getting performance data: {e}")
            return {}
    
    def get_educational_content(self) -> Dict:
        """Get educational content from Redis"""
        try:
            content_key = "educational:latest"
            content_data = self.redis_client.get(content_key)
            if content_data:
                return json.loads(content_data)
            return {}
        except Exception as e:
            self.logger.error(f"Error getting educational content: {e}")
            return {}
    
    def get_market_data(self) -> Dict:
        """Get market data from Redis"""
        try:
            market_key = "market:current"
            market_data = self.redis_client.get(market_key)
            if market_data:
                return json.loads(market_data)
            return {}
        except Exception as e:
            self.logger.error(f"Error getting market data: {e}")
            return {}
    
    async def sync_alert_to_telegram(self, alert: Dict):
        """Sync alert to Telegram"""
        try:
            # This would integrate with Telegram bot
            # For now, just log
            self.logger.info(f"Syncing alert to Telegram: {alert.get('symbol', 'N/A')}")
        except Exception as e:
            self.logger.error(f"Error syncing alert to Telegram: {e}")
    
    async def sync_educational_content_to_reddit(self):
        """Sync educational content to Reddit"""
        try:
            # This would integrate with Reddit bot
            # For now, just log
            self.logger.info("Syncing educational content to Reddit")
        except Exception as e:
            self.logger.error(f"Error syncing educational content to Reddit: {e}")
    
    async def update_reddit_performance_report(self, performance_data: Dict):
        """Update Reddit performance report"""
        try:
            # This would integrate with Reddit bot
            # For now, just log
            self.logger.info("Updating Reddit performance report")
        except Exception as e:
            self.logger.error(f"Error updating Reddit performance report: {e}")
    
    async def share_educational_to_reddit(self, content: Dict):
        """Share educational content to Reddit"""
        try:
            # This would integrate with Reddit bot
            # For now, just log
            self.logger.info("Sharing educational content to Reddit")
        except Exception as e:
            self.logger.error(f"Error sharing educational content to Reddit: {e}")
    
    
    def get_community_metrics(self) -> Dict:
        """Get community metrics"""
        return self.community_metrics
    
    def update_community_metrics(self, metric: str, value: int = 1):
        """Update community metrics"""
        if metric in self.community_metrics:
            self.community_metrics[metric] += value
    
    async def generate_community_report(self) -> str:
        """Generate community report"""
        try:
            metrics = self.get_community_metrics()
            
            report = f"""
# AION Community Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Community Metrics
- **Total Alerts Sent**: {metrics['total_alerts_sent']}
- **Reddit Engagement**: {metrics['reddit_engagement']}
- **Telegram Engagement**: {metrics['telegram_engagement']}
- **Educational Content Shared**: {metrics['educational_content_shared']}
- **Performance Reports Posted**: {metrics['performance_reports_posted']}

## 🎯 Platform Status
- **Reddit**: Active
- **Telegram**: Active

## 📈 Recent Activity
- Last sync: {datetime.now().strftime('%H:%M:%S')}
- Alerts processed: {metrics['total_alerts_sent']}
- Educational content: {metrics['educational_content_shared']}
- Performance updates: {metrics['performance_reports_posted']}

---
*Report generated by AION Community Manager*
            """
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating community report: {e}")
            return "Error generating community report."

# Main execution
async def main():
    """Main execution function"""
    try:
        # Create community manager
        community_manager = AIONCommunityManager()
        
        # Start all bots
        await community_manager.start_community_bots()
        
    except Exception as e:
        print(f"Error starting community manager: {e}")

if __name__ == "__main__":
    asyncio.run(main())

