# AION Community Bots Deployment Guide

## 🎯 Overview
This guide covers the deployment of AION algo trading community bots across Discord, Reddit, and Telegram platforms.

## 📋 Prerequisites

### System Requirements
- Python 3.8+
- Redis server running
- Internet connection
- Bot tokens and API credentials

### Required Python Packages
```bash
pip install discord.py
pip install praw
pip install aiohttp
pip install redis
pip install schedule
pip install matplotlib
pip install pandas
pip install numpy
```

## 🔧 Environment Setup

### 1. Environment Variables
Create a `.env` file with the following variables:

```bash
# Discord Bot
DISCORD_TOKEN=your_discord_bot_token

# Reddit Bot
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_MAIN_CHANNEL=@your_main_channel
TELEGRAM_NEWS_CHANNEL=@your_news_channel

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
```

### 2. Redis Setup
```bash
# Install Redis
sudo apt-get install redis-server

# Start Redis
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Test Redis connection
redis-cli ping
```

## 🤖 Bot Setup

### 1. Discord Bot Setup

#### Create Discord Application
1. Go to https://discord.com/developers/applications
2. Click "New Application"
3. Name it "AION Algo Trading Bot"
4. Go to "Bot" section
5. Click "Add Bot"
6. Copy the bot token
7. Set bot permissions:
   - Send Messages
   - Embed Links
   - Read Message History
   - Use Slash Commands

#### Invite Bot to Server
1. Go to "OAuth2" > "URL Generator"
2. Select "bot" scope
3. Select required permissions
4. Copy generated URL
5. Open URL in browser to invite bot

#### Server Setup
Create the following channels:
- `#live-alerts` - For real-time alerts
- `#market-updates` - For market updates
- `#performance-tracker` - For performance metrics
- `#pattern-education` - For educational content
- `#news-analysis` - For news analysis
- `#general-chat` - For general discussion
- `#market-discussion` - For market discussions
- `#questions-help` - For Q&A

### 2. Reddit Bot Setup

#### Create Reddit Application
1. Go to https://www.reddit.com/prefs/apps
2. Click "Create App" or "Create Another App"
3. Choose "script" type
4. Name it "AION Algo Trading Bot"
5. Note down client ID and secret

#### Subreddit Setup
1. Create subreddit: r/AIONAlgoTrading
2. Set up rules and guidelines
3. Configure automod
4. Set up flairs for different content types

#### Bot Permissions
- Post submissions
- Comment on posts
- Moderate content
- Access user data (if needed)

### 3. Telegram Bot Setup

#### Create Telegram Bot
1. Message @BotFather on Telegram
2. Use `/newbot` command
3. Follow instructions to create bot
4. Copy bot token
5. Set up bot commands:
   - `/start` - Start bot
   - `/help` - Show help
   - `/alerts` - Get latest alerts
   - `/performance` - Get performance stats

#### Channel Setup
1. Create public channels:
   - `@Nifty50Signals_Pro` - Main signals
   - `@Nifty50News_Alerts` - News alerts
2. Add bot as admin to channels
3. Configure channel settings

## 🚀 Deployment

### 1. Local Deployment

#### Install Dependencies
```bash
cd /path/to/intraday_trading/community_bots
pip install -r requirements.txt
```

#### Start Individual Bots
```bash
# Discord Bot
python discord_bot.py

# Reddit Bot
python reddit_bot.py

# Telegram Bot
python telegram_bot.py

# Community Manager
python community_manager.py
```

### 2. Production Deployment

#### Using systemd (Linux)
Create service files:

**Discord Bot Service**
```ini
[Unit]
Description=AION Discord Bot
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/community_bots
ExecStart=/usr/bin/python3 discord_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Reddit Bot Service**
```ini
[Unit]
Description=AION Reddit Bot
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/community_bots
ExecStart=/usr/bin/python3 reddit_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Telegram Bot Service**
```ini
[Unit]
Description=AION Telegram Bot
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/community_bots
ExecStart=/usr/bin/python3 telegram_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

#### Enable Services
```bash
sudo systemctl enable aion-discord-bot
sudo systemctl enable aion-reddit-bot
sudo systemctl enable aion-telegram-bot

sudo systemctl start aion-discord-bot
sudo systemctl start aion-reddit-bot
sudo systemctl start aion-telegram-bot
```

### 3. Docker Deployment

#### Create Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "community_manager.py"]
```

#### Create docker-compose.yml
```yaml
version: '3.8'

services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  discord-bot:
    build: .
    command: python discord_bot.py
    environment:
      - DISCORD_TOKEN=${DISCORD_TOKEN}
      - REDIS_HOST=redis
    depends_on:
      - redis

  reddit-bot:
    build: .
    command: python reddit_bot.py
    environment:
      - REDDIT_CLIENT_ID=${REDDIT_CLIENT_ID}
      - REDDIT_CLIENT_SECRET=${REDDIT_CLIENT_SECRET}
      - REDIS_HOST=redis
    depends_on:
      - redis

  telegram-bot:
    build: .
    command: python telegram_bot.py
    environment:
      - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
      - REDIS_HOST=redis
    depends_on:
      - redis

  community-manager:
    build: .
    command: python community_manager.py
    environment:
      - REDIS_HOST=redis
    depends_on:
      - redis
      - discord-bot
      - reddit-bot
      - telegram-bot

volumes:
  redis_data:
```

#### Deploy with Docker
```bash
docker-compose up -d
```

## 📊 Monitoring

### 1. Log Monitoring
```bash
# View logs
sudo journalctl -u aion-discord-bot -f
sudo journalctl -u aion-reddit-bot -f
sudo journalctl -u aion-telegram-bot -f

# Docker logs
docker-compose logs -f discord-bot
docker-compose logs -f reddit-bot
docker-compose logs -f telegram-bot
```

### 2. Health Checks
```bash
# Check Redis connection
redis-cli ping

# Check bot status
curl -X GET "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/getMe"

# Check Discord bot (if using webhooks)
curl -X POST "https://discord.com/api/webhooks/your_webhook_url"
```

### 3. Performance Monitoring
- Monitor Redis memory usage
- Track bot response times
- Monitor API rate limits
- Check error rates

## 🔧 Configuration

### 1. Rate Limiting
```python
# Discord rate limits
DISCORD_RATE_LIMITS = {
    'messages_per_second': 5,
    'messages_per_minute': 50
}

# Reddit rate limits
REDDIT_RATE_LIMITS = {
    'posts_per_hour': 10,
    'comments_per_hour': 50
}

# Telegram rate limits
TELEGRAM_RATE_LIMITS = {
    'messages_per_second': 30,
    'messages_per_minute': 20
}
```

### 2. Content Filtering
```python
# Confidence thresholds
CONFIDENCE_THRESHOLDS = {
    'discord': 0.70,
    'reddit': 0.60,
    'telegram': 0.70
}

# Content filters
CONTENT_FILTERS = {
    'min_confidence': 0.60,
    'max_alerts_per_hour': 10,
    'educational_content_required': True
}
```

### 3. Engagement Tracking
```python
# Engagement metrics
ENGAGEMENT_METRICS = {
    'discord': ['messages', 'reactions', 'voice_activity'],
    'reddit': ['upvotes', 'comments', 'shares'],
    'telegram': ['views', 'forwards', 'reactions']
}
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Bot Not Responding
- Check bot token validity
- Verify network connectivity
- Check Redis connection
- Review error logs

#### 2. Rate Limit Exceeded
- Implement rate limiting
- Add delays between requests
- Use exponential backoff
- Monitor API usage

#### 3. Redis Connection Issues
- Check Redis server status
- Verify connection parameters
- Check firewall settings
- Monitor Redis memory usage

#### 4. Content Not Posting
- Check bot permissions
- Verify channel IDs
- Review content filters
- Check API quotas

### Debug Commands
```bash
# Test Redis connection
redis-cli ping

# Test Discord bot
python -c "import discord; print('Discord.py installed')"

# Test Reddit bot
python -c "import praw; print('PRAW installed')"

# Test Telegram bot
python -c "import aiohttp; print('aiohttp installed')"
```

## 📈 Scaling

### 1. Horizontal Scaling
- Deploy multiple bot instances
- Use load balancers
- Implement message queues
- Use Redis clustering

### 2. Vertical Scaling
- Increase server resources
- Optimize database queries
- Implement caching
- Use CDN for static content

### 3. Performance Optimization
- Implement connection pooling
- Use async/await patterns
- Optimize Redis operations
- Implement message batching

## 🔒 Security

### 1. Bot Security
- Use environment variables for tokens
- Implement rate limiting
- Validate all inputs
- Use HTTPS for API calls

### 2. Data Security
- Encrypt sensitive data
- Use secure Redis configuration
- Implement access controls
- Regular security audits

### 3. Monitoring
- Log all bot activities
- Monitor for suspicious behavior
- Implement alerting
- Regular security updates

## 📚 Maintenance

### 1. Regular Updates
- Update bot dependencies
- Monitor API changes
- Update content templates
- Review performance metrics

### 2. Backup Strategy
- Backup Redis data
- Backup configuration files
- Backup logs
- Test restore procedures

### 3. Monitoring
- Set up health checks
- Monitor error rates
- Track performance metrics
- Implement alerting

This deployment guide provides comprehensive instructions for setting up and maintaining the AION community bots across all platforms.

