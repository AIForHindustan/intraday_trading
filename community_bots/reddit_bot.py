"""
AION Reddit Bot
Posts validation results and reports to Reddit community
"""

import logging
import json
from typing import Optional, Dict
from pathlib import Path
import base64
import requests

# Try to import praw, but handle gracefully if not available
PRAW_AVAILABLE = False
praw = None
try:
    import praw
    # Test if praw actually works (sometimes it's installed but broken)
    try:
        # Quick test to see if praw module is functional
        _ = praw.Reddit.__name__
        PRAW_AVAILABLE = True
    except (AttributeError, ModuleNotFoundError):
        PRAW_AVAILABLE = False
        praw = None
except (ImportError, ModuleNotFoundError):
    PRAW_AVAILABLE = False
    praw = None

logger = logging.getLogger(__name__)

class AIONRedditBot:
    """Reddit bot for posting to AION community"""
    
    def __init__(self):
        # Load Reddit config - try JSON file first, then environment variables
        import os
        config_path = Path(__file__).parent.parent / "alerts" / "config" / "reddit_config.json"
        self.config = {}
        
        if not PRAW_AVAILABLE:
            logger.warning("praw library not installed. Reddit bot will use Redis channel fallback.")
            logger.warning("To enable direct Reddit posting, install: pip install praw")
            self.reddit = None
            self.subreddit = None
            self.subreddit_name = 'NSEAlgoTrading'
            return
        
        # Try loading from JSON file first
        try:
            if config_path.exists():
                with open(config_path, 'r') as f:
                    self.config = json.load(f)
                    logger.info(f"✅ Loaded Reddit config from {config_path}")
        except Exception as e:
            logger.debug(f"Could not load Reddit config from file: {e}")
        
        # Fallback to environment variables if JSON file not found or incomplete
        if not self.config.get('client_id'):
            self.config['client_id'] = os.getenv('REDDIT_CLIENT_ID', '')
        if not self.config.get('client_secret'):
            self.config['client_secret'] = os.getenv('REDDIT_CLIENT_SECRET', '')
        if not self.config.get('username'):
            self.config['username'] = os.getenv('REDDIT_USERNAME', '')
        if not self.config.get('password'):
            self.config['password'] = os.getenv('REDDIT_PASSWORD', '')
        if not self.config.get('subreddit'):
            self.config['subreddit'] = os.getenv('REDDIT_SUBREDDIT', 'NSEAlgoTrading')
        
        # Check if we have minimum required credentials
        if not self.config.get('client_id') or not self.config.get('client_secret'):
            logger.warning("Reddit credentials not found. Check:")
            logger.warning(f"  1. JSON file: {config_path}")
            logger.warning("  2. Environment variables: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD")
            self.reddit = None
            self.subreddit = None
            self.subreddit_name = self.config.get('subreddit', 'NSEAlgoTrading')
            return
        
        try:
            # Initialize Reddit client
            # Note: redirect_uri is required even for personal use scripts
            # Using check_for_async=False to avoid async warnings
            self.reddit = praw.Reddit(
                client_id=self.config.get('client_id'),
                client_secret=self.config.get('client_secret'),
                username=self.config.get('username'),
                password=self.config.get('password'),
                redirect_uri='http://localhost',  # Required for personal use script
                user_agent='AION Algo Trading Bot v1.0 (by u/TheOldSoul15)',
                check_for_async=False
            )
            
            self.subreddit_name = self.config.get('subreddit', 'NSEAlgoTrading')
            self.subreddit = self.reddit.subreddit(self.subreddit_name)
            
            # Verify credentials work
            try:
                self.reddit.user.me()
                logger.info(f"✅ Reddit bot initialized for r/{self.subreddit_name} (authenticated as {self.reddit.user.me().name})")
            except Exception as auth_error:
                logger.warning(f"PRAW authentication failed: {auth_error}")
                logger.info("Will use direct Reddit API as fallback")
                # Keep credentials for direct API fallback
                # Don't set reddit/subreddit to None - we'll use direct API
                self.use_direct_api = True
                try:
                    # Test direct API authentication
                    self._get_access_token()
                    logger.info("✅ Direct Reddit API authentication working - will use this method")
                except Exception as api_error:
                    logger.error(f"Direct API authentication also failed: {api_error}")
                    self.reddit = None
                    self.subreddit = None
                    self.use_direct_api = False
            
        except FileNotFoundError:
            logger.error(f"Reddit config file not found: {config_path}")
            logger.info(f"Expected location: {config_path}")
            logger.info("Create the file with: client_id, client_secret, username, password, subreddit")
            self.reddit = None
            self.subreddit = None
            self.subreddit_name = 'NSEAlgoTrading'
        except Exception as e:
            logger.error(f"Error initializing Reddit bot: {e}")
            self.reddit = None
            self.subreddit = None
            self.subreddit_name = self.config.get('subreddit', 'NSEAlgoTrading') if hasattr(self, 'config') else 'NSEAlgoTrading'
        
        # Initialize direct API fallback flag (default False)
        self.use_direct_api = getattr(self, 'use_direct_api', False)
    
    def _get_access_token(self) -> str:
        """Get Reddit access token using direct OAuth API"""
        url = "https://www.reddit.com/api/v1/access_token"
        auth = base64.b64encode(
            f"{self.config.get('client_id')}:{self.config.get('client_secret')}".encode()
        ).decode()
        
        headers = {
            'Authorization': f'Basic {auth}',
            'User-Agent': 'AION Algo Trading Bot v1.0 (by u/TheOldSoul15)'
        }
        
        data = {
            'grant_type': 'password',
            'username': self.config.get('username'),
            'password': self.config.get('password')
        }
        
        response = requests.post(url, headers=headers, data=data, timeout=10)
        token_data = response.json()
        
        # Check for error in response (Reddit returns 200 even with errors)
        if 'error' in token_data:
            error_msg = token_data.get('error_description', token_data.get('error', 'Unknown error'))
            raise Exception(f"OAuth error: {error_msg}")
        
        if response.status_code == 200 and 'access_token' in token_data:
            return token_data.get('access_token', '')
        else:
            raise Exception(f"OAuth failed: {response.status_code} - {response.text}")
    
    async def post_update(self, title: str, text: str, flair: Optional[str] = None) -> bool:
        """Post update to Reddit subreddit - tries praw first, falls back to direct API"""
        # Try direct API if praw failed or not available
        if self.use_direct_api or (not self.reddit and self.config and self.config.get('client_id')):
            logger.info("Using direct Reddit API for posting")
            return await self._post_via_direct_api(title, text, flair)
        
        if not PRAW_AVAILABLE:
            logger.warning("praw library not available, trying direct API fallback")
            if self.config and self.config.get('client_id'):
                return await self._post_via_direct_api(title, text, flair)
            return False
            
        if not self.reddit or not self.subreddit:
            logger.warning("PRAW not initialized, trying direct API fallback")
            if self.config and self.config.get('client_id'):
                return await self._post_via_direct_api(title, text, flair)
            logger.error("Reddit bot not initialized. Check credentials in alerts/config/reddit_config.json")
            return False
        
        try:
            import asyncio
            # Run praw's sync method in executor to make it async-friendly
            loop = asyncio.get_event_loop()
            submission = await loop.run_in_executor(
                None, 
                lambda: self.subreddit.submit(title=title, selftext=text)
            )
            
            # Set flair if provided
            if flair:
                try:
                    available_flairs = list(self.subreddit.flair.link_templates)
                    flair_ids = {f['text']: f['id'] for f in available_flairs}
                    if flair in flair_ids:
                        await loop.run_in_executor(
                            None,
                            lambda: submission.flair.select(flair_ids[flair])
                        )
                except Exception as e:
                    logger.debug(f"Could not set flair: {e}")
            
            logger.info(f"✅ Posted to r/{self.subreddit_name}: {title[:50]}...")
            logger.info(f"   Post ID: {submission.id}, URL: https://reddit.com{submission.permalink}")
            
            return True
            
        except Exception as e:
            logger.warning(f"PRAW posting failed: {e}, trying direct API fallback")
            return await self._post_via_direct_api(title, text, flair)
    
    async def _post_via_direct_api(self, title: str, text: str, flair: Optional[str] = None) -> bool:
        """Post to Reddit using direct API calls (fallback when praw fails)"""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            
            # Get access token
            access_token = await loop.run_in_executor(None, self._get_access_token)
            
            if not access_token:
                logger.error("Failed to get Reddit access token")
                return False
            
            # Post to Reddit
            url = f"https://oauth.reddit.com/api/submit"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'User-Agent': 'AION Algo Trading Bot v1.0 (by u/TheOldSoul15)'
            }
            
            data = {
                'sr': self.subreddit_name,
                'title': title,
                'text': text,
                'kind': 'self'  # Self post (text post)
            }
            
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(url, headers=headers, data=data, timeout=30)
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'json' in result and 'data' in result['json']:
                    post_data = result['json']['data']
                    post_id = post_data.get('id', 'unknown')
                    post_url = f"https://reddit.com{post_data.get('url', '')}"
                    
                    logger.info(f"✅ Posted to r/{self.subreddit_name} via direct API: {title[:50]}...")
                    logger.info(f"   Post ID: {post_id}, URL: {post_url}")
                    
                    # Set flair if provided (would need additional API call)
                    # For now, skip flair as it requires more complex API calls
                    
                    return True
                else:
                    logger.error(f"Unexpected response format: {result}")
                    return False
            else:
                logger.error(f"Failed to post: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error posting via direct API: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def post_validation_result(self, reddit_post: str) -> bool:
        """Post validation result (sync version for compatibility)"""
        import asyncio
        try:
            # Extract title and body from reddit_post if it's formatted
            if isinstance(reddit_post, dict):
                title = reddit_post.get('title', 'Alert Validation Result')
                text = reddit_post.get('body', reddit_post.get('text', str(reddit_post)))
            else:
                # Assume it's just the body text
                title = "Alert Validation Result"
                text = str(reddit_post)
            
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.post_update(title, text))
        except Exception as e:
            logger.error(f"Error in post_validation_result: {e}")
            return False

