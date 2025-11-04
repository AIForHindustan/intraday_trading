#!/usr/bin/env python3
"""
Post Kow Signal Educational Content
Uses existing community_manager to post to Reddit, Telegram, and Discord
Also supports fetching Reddit posts by URL and reposting
"""

import sys
import asyncio
import re
import aiohttp
from pathlib import Path
from typing import Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from community_bots.community_manager import AIONCommunityManager

async def fetch_reddit_post(url: str) -> Optional[dict]:
    """Fetch Reddit post content from URL"""
    try:
        # Clean URL - remove query parameters and trailing slash
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        # Reconstruct base URL
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if base_url.endswith('/'):
            base_url = base_url.rstrip('/')
        
        # Convert Reddit URL to JSON API endpoint
        if not base_url.endswith('.json'):
            json_url = base_url + '.json'
        else:
            json_url = base_url
        
        async with aiohttp.ClientSession() as session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            async with session.get(json_url, headers=headers) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' in content_type:
                        data = await response.json()
                        # Reddit API returns a list: [post_data, comments]
                        if isinstance(data, list) and len(data) > 0:
                            post_data = data[0]['data']['children'][0]['data']
                            return {
                                'title': post_data.get('title', ''),
                                'content': post_data.get('selftext', ''),
                                'url': post_data.get('url', url),
                                'permalink': f"https://reddit.com{post_data.get('permalink', '')}"
                            }
                    else:
                        # Fallback: parse HTML (basic extraction)
                        html = await response.text()
                        print("⚠️ Received HTML instead of JSON, attempting HTML parsing...")
                        # Try to extract basic info from HTML
                        import re
                        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE)
                        title = title_match.group(1).strip() if title_match else "Reddit Post"
                        # Extract post ID from URL
                        post_id_match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
                        post_id = post_id_match.group(1) if post_id_match else ''
                        return {
                            'title': title.replace(' : r/', ' - ').replace(' : Reddit', ''),
                            'content': f"🔗 Content from Reddit post\n\n{url}",
                            'url': url,
                            'permalink': url
                        }
                else:
                    print(f"Error fetching Reddit post: {response.status}")
                    return None
    except Exception as e:
        print(f"Error fetching Reddit post: {e}")
        import traceback
        traceback.print_exc()
        return None

async def main():
    """Post Kow Signal educational content or Reddit post"""
    print("=" * 60)
    print("Posting Educational Content to Community Platforms")
    print("=" * 60)
    
    import argparse
    parser = argparse.ArgumentParser(description='Post educational content to community platforms')
    parser.add_argument('--reddit-url', type=str, help='Reddit post URL to fetch and repost')
    parser.add_argument('--kow-signal', action='store_true', help='Post Kow Signal document')
    args = parser.parse_args()
    
    # Initialize community manager
    manager = AIONCommunityManager()
    
    success = False
    platforms = []
    
    if args.reddit_url:
        # Fetch Reddit post and repost
        print(f"\n📥 Fetching Reddit post from: {args.reddit_url}")
        reddit_post = await fetch_reddit_post(args.reddit_url)
        
        if reddit_post:
            print(f"✅ Fetched: {reddit_post['title']}")
            
            # Format as educational content
            educational_content = {
                'title': reddit_post['title'],
                'content': f"{reddit_post['content']}\n\n🔗 Original Post: {reddit_post['permalink']}",
                'description': reddit_post['content'][:500] + '...' if len(reddit_post['content']) > 500 else reddit_post['content']
            }
            
            print("\n📤 Posting to Telegram, Discord, and Reddit...")
            
            # Post to Telegram
            telegram_success = await manager.telegram_bot.send_educational_content(educational_content)
            if telegram_success:
                platforms.append(f"Telegram: {manager.telegram_bot.main_channel}")
            
            # Post to Discord
            if manager.discord_bot:
                discord_success = await manager.discord_bot.send_educational_content(educational_content)
                if discord_success:
                    platforms.append("Discord")
            
            # Post to Reddit (will create new post with reference)
            reddit_success = await manager.share_educational_to_reddit(educational_content)
            if reddit_success:
                platforms.append(f"Reddit: r/{manager.reddit_bot.subreddit_name}")
            
            success = telegram_success or (manager.discord_bot and discord_success) or reddit_success
        else:
            print("❌ Failed to fetch Reddit post")
            return
    
    elif args.kow_signal or (not args.reddit_url and not args.kow_signal):
        # Default: Post Kow Signal document
        print("\n📄 Loading Kow Signal Strategy document...")
        
        # Path to educational post - look in educational_content_shared directory
        educational_dir = Path(__file__).parent.parent / "educational_content_shared"
        kow_signal_files = sorted(educational_dir.glob("kow_signal_*.md"), reverse=True)
        
        if not kow_signal_files:
            print(f"❌ Error: No Kow Signal educational post found in {educational_dir}")
            return
        
        post_file = kow_signal_files[0]  # Use most recent file
        print(f"📂 Loading content from: {post_file.name}")
        
        # Post to all platforms via community manager
        print("\n📤 Posting to Telegram, Discord, and Reddit...")
        success = await manager.share_educational_content(file_path=str(post_file))
        
        if success:
            platforms.append(f"Telegram: {manager.telegram_bot.main_channel}")
            if manager.discord_bot:
                platforms.append("Discord")
            platforms.append(f"Reddit: r/{manager.reddit_bot.subreddit_name}")
    
    if success:
        print("\n✅ Successfully posted educational content!")
        for platform in platforms:
            print(f"   ✓ {platform}")
    else:
        print("\n❌ Failed to post educational content to any platform")

if __name__ == "__main__":
    asyncio.run(main())

