"""
INDEX DATA UPDATER - AION Market Context Provider
Updates NIFTY 50, BANK NIFTY, and SGX GIFT NIFTY at 30-second intervals
Provides market context for U-shaped volume formula calculations

Author: AION Integration Team
Date: September 16, 2025
"""

import sys
import os
import json
import logging
import time as time_module
import requests
import re
from datetime import datetime, time
from pathlib import Path
from bs4 import BeautifulSoup

# Add project root to path
project_root = Path(__file__).parent.parent  # crawlers -> project root
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
from redis_files.redis_key_standards import RedisKeyStandards

# Setup logging
# Ensure logs directory exists
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(logs_dir / 'index_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('IndexUpdater')

class IndexDataUpdater:
    """Updates market indices data for AION volume calculations"""

    def __init__(self):
        try:
            # Try to get credentials from environment first
            if os.environ.get("ZERODHA_ACCESS_TOKEN"):
                self.kite = ZerodhaConfig.get_kite_instance()
            else:
                # Fallback to loading from zerodha_token.json
                token_data = ZerodhaConfig.get_token_data()
                if token_data:
                    from kiteconnect import KiteConnect
                    self.kite = KiteConnect(api_key=token_data['api_key'])
                    self.kite.set_access_token(token_data['access_token'])
                    # Test the connection
                    self.kite.profile()
                else:
                    raise Exception("No token data found")
            
            self.initialized = True
            logger.info("✅ Initialized Zerodha connection")
        except Exception as e:
            logger.warning(f"⚠️ Zerodha credentials not configured: {e}")
            self.kite = None
            self.initialized = False

        # Initialize Redis connection - ALL data goes to DB 1 (unified structure)
        try:
            # ✅ FIX: Use only DB 1 for all publishing (unified structure)
            from redis_files.redis_client import redis_manager
            self.redis_client = redis_manager.get_client()  # DB 1 singleton
            
            if self.redis_client is None:
                raise Exception("Redis DB 1 client is None")
                
            self.redis_initialized = True
            logger.info("✅ Redis connected for index data publishing to DB 1 (unified structure)")
        except Exception as e:
            logger.error(f"⚠️ Redis connection failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.redis_initialized = False
            self.redis_client = None

        # Index definitions with tokens
        self.indices = {
            'NSE:NIFTY 50': 256265,
            'NSE:NIFTY BANK': 260105,
            'NSEIX:GIFT NIFTY': 291849,
            'NSE:INDIA VIX': 264969  # India VIX token
        }
        
        # Load company name to symbol mapping for news extraction
        self.company_symbol_map = {}
        self._load_company_symbol_mapping()

        # News collection setup
        self.pulse_url = "https://pulse.zerodha.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.news_data = []

        # Data storage
        self.index_data = {}
        self.last_updates = {}

        # Update interval (30 seconds)
        self.update_interval = 30

        # JSONL file for backtesting data
        self.jsonl_file = None
        self._setup_jsonl_file()

        # Running flag
        self.running = False
    
    def _load_company_symbol_mapping(self):
        """Load company name to symbol mapping from token lookup for news symbol extraction"""
        try:
            token_lookup_file = Path(__file__).parent.parent / "core" / "data" / "token_lookup_enriched.json"
            
            if not token_lookup_file.exists():
                logger.warning(f"⚠️ Token lookup file not found: {token_lookup_file}")
                return
            
            logger.info(f"📚 Loading company name mappings from {token_lookup_file}")
            
            with open(token_lookup_file, 'r') as fd:
                token_lookup = json.load(fd)
            
            # Load intraday crawler tokens to know which symbols we care about
            crawler_config_file = Path(__file__).parent.parent / 'crawlers' / 'binary_crawler1' / 'binary_crawler1.json'
            intraday_tokens = set()
            
            if crawler_config_file.exists():
                with open(crawler_config_file, 'r') as f:
                    config_data = json.load(f)
                intraday_tokens = {int(t) if isinstance(t, str) else t for t in config_data.get('tokens', [])}
                logger.info(f"📋 Loaded {len(intraday_tokens)} intraday crawler tokens")
            
            # Build company name to symbol mapping
            company_mappings = {}
            
            for token_str, inst_data in token_lookup.items():
                try:
                    token = int(token_str) if token_str.isdigit() else None
                    
                    # Only process instruments from intraday crawler
                    if intraday_tokens and token and token not in intraday_tokens:
                        continue
                    
                    # Get symbol and company name
                    key_field = inst_data.get('key', '')
                    company_name = inst_data.get('name', '').upper()
                    exchange = inst_data.get('exchange', '')
                    
                    if not key_field or not company_name:
                        continue
                    
                    # Extract symbol (remove exchange prefix)
                    symbol = key_field.split(':')[-1] if ':' in key_field else key_field
                    full_symbol = key_field  # Keep full exchange:symbol format
                    
                    # Create mappings for company name variations
                    company_variations = self._get_company_variations(company_name)
                    
                    for variation in company_variations:
                        if variation not in company_mappings:
                            company_mappings[variation] = []
                        # Store both base symbol and full symbol
                        if full_symbol not in company_mappings[variation]:
                            company_mappings[variation].append({
                                'symbol': symbol,
                                'full_symbol': full_symbol,
                                'exchange': exchange,
                                'instrument_type': inst_data.get('instrument_type', '')
                            })
                
                except Exception as e:
                    continue
            
            self.company_symbol_map = company_mappings
            logger.info(f"✅ Loaded {len(company_mappings)} company name mappings")
            
        except Exception as e:
            logger.error(f"❌ Error loading company symbol mapping: {e}")
            self.company_symbol_map = {}
    
    def _get_company_variations(self, company_name):
        """Generate company name variations for matching"""
        variations = [company_name]
        
        # Common variations
        if 'BANK' in company_name:
            variations.append(company_name.replace(' BANK', ''))
            variations.append(company_name.replace('BANK', ''))
        if 'LIMITED' in company_name:
            variations.append(company_name.replace(' LIMITED', ''))
            variations.append(company_name.replace('LIMITED', ''))
        if 'LTD' in company_name:
            variations.append(company_name.replace(' LTD', ''))
            variations.append(company_name.replace('LTD', ''))
        
        # Remove common suffixes
        suffixes = [' LIMITED', ' LTD', ' INC', ' CORPORATION', ' CORP']
        for suffix in suffixes:
            if company_name.endswith(suffix):
                variations.append(company_name[:-len(suffix)])
        
        return list(set(variations))

    def _setup_jsonl_file(self):
        """Setup JSONL file for backtesting data storage"""
        try:
            # Create data directory within intraday_trading project
            data_dir = os.path.join(os.path.dirname(__file__), '../config/data/indices')
            os.makedirs(data_dir, exist_ok=True)
            
            # Create daily JSONL file
            date_str = datetime.now().strftime("%Y%m%d")
            jsonl_filename = os.path.join(data_dir, f'indices_data_{date_str}.jsonl')
            
            self.jsonl_file = jsonl_filename
            logger.info(f"📁 JSONL file: {self.jsonl_file}")
            
        except Exception as e:
            logger.error(f"❌ Error setting up JSONL file: {e}")
            self.jsonl_file = None

    def _write_to_jsonl(self, data):
        """Write index data to JSONL file for backtesting"""
        if not self.jsonl_file:
            return
            
        try:
            with open(self.jsonl_file, 'a') as f:
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            logger.error(f"❌ Error writing to JSONL: {e}")

    def _collect_zerodha_news(self):
        """Collect news from Zerodha Pulse"""
        news_items = []
        
        try:
            logger.info("📰 Collecting news from Zerodha Pulse...")
            
            # Make request to Pulse
            response = self.session.get(self.pulse_url, timeout=10)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find news items
            news_elements = soup.find_all('li', class_='box item')
            
            for item in news_elements[:10]:  # Limit to 10 most recent
                try:
                    # Extract title
                    title_elem = item.find('h2', class_='title')
                    title = title_elem.get_text(strip=True) if title_elem else "No title"
                    
                    # Extract link
                    link_elem = item.find('a', href=True)
                    link = link_elem['href'] if link_elem else ""
                    
                    # Extract date
                    date_elem = item.find('span', class_='date')
                    date_text = date_elem.get_text(strip=True) if date_elem else "No date"
                    
                    # Extract source
                    source_elem = item.find('span', class_='feed')
                    source = source_elem.get_text(strip=True).replace('—', '').strip() if source_elem else "No source"
                    
                    # Create news item
                    news_item = {
                        'source': 'zerodha_pulse',
                        'title': title,
                        'link': link,
                        'date': date_text,
                        'publisher': source,
                        'collected_at': datetime.now().isoformat(),
                        'sentiment': self._analyze_news_sentiment(title)
                    }
                    
                    news_items.append(news_item)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing news item: {e}")
                    continue
            
            logger.info(f"✅ Collected {len(news_items)} news items from Pulse")
            
        except Exception as e:
            logger.error(f"❌ Error collecting Pulse news: {e}")
        
        return news_items

    def _analyze_news_sentiment(self, text):
        """Simple sentiment analysis for news titles"""
        positive_words = ['bullish', 'rise', 'gain', 'up', 'positive', 'growth', 'profit', 'earnings', 'beat', 'exceed', 'surge', 'rally']
        negative_words = ['bearish', 'fall', 'drop', 'down', 'negative', 'loss', 'miss', 'decline', 'crash', 'plunge', 'slump', 'dip']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'

    def _calculate_news_sentiment_score(self):
        """Calculate overall news sentiment score (-1 to 1)"""
        if not self.news_data:
            return 0.0
        
        positive_count = len([item for item in self.news_data if item.get('sentiment') == 'positive'])
        negative_count = len([item for item in self.news_data if item.get('sentiment') == 'negative'])
        total_news = len(self.news_data)
        
        if total_news > 0:
            positive_ratio = positive_count / total_news
            negative_ratio = negative_count / total_news
            return positive_ratio - negative_ratio
        else:
            return 0.0

    def start_continuous_updates(self):
        """Start continuous index updates"""
        logger.info("🚀 Starting Index Data Updater...")
        logger.info("📊 Tracking: NIFTY 50, BANK NIFTY, SGX GIFT NIFTY, INDIA VIX")
        logger.info("⏰ Update Interval: 30 seconds")
        if self.jsonl_file:
            logger.info(f"📁 Backtesting data: {self.jsonl_file}")
        self.running = True

        # Initial update
        self.update_indices()

        # Start periodic updates
        while self.running:
            try:
                time_module.sleep(self.update_interval)
                self.update_indices()
            except KeyboardInterrupt:
                logger.info("🛑 Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"❌ Error in update loop: {e}")
                time_module.sleep(5)  # Brief pause before retry

        logger.info("🛑 Index Data Updater stopped")

    def stop(self):
        """Stop the index updater"""
        logger.info("🛑 Stopping Index Data Updater...")
        self.running = False

    def update_indices(self):
        """Update all indices data and collect news"""
        logger.info("📊 Updating index data...")

        if not self.initialized or self.kite is None:
            logger.error("❌ Zerodha API not available")
            return

        # Collect news from Zerodha Pulse
        try:
            news_items = self._collect_zerodha_news()
            if news_items:
                self.news_data = news_items
                logger.info(f"📰 News updated: {len(news_items)} items")
                
                # ALWAYS write news to disk immediately after collection
                self._write_news_to_disk()
                logger.info(f"💾 News written to disk: {len(news_items)} items")
            else:
                logger.warning("⚠️ No news items collected from Zerodha Pulse")
        except Exception as e:
            logger.warning(f"⚠️ News collection failed: {e}")

        updated_count = 0
        for symbol, token in self.indices.items():
            try:
                # Get latest quote using instrument key (symbol) - more reliable than token
                # Zerodha API quote() expects a list of instrument keys or tokens
                quote = self.kite.quote([symbol])

                if quote and symbol in quote:
                    data = quote[symbol]
                elif quote and str(token) in quote:
                    # Fallback: try token format if symbol format doesn't work
                    data = quote[str(token)]
                else:
                    # Try alternative: use token as list
                    quote_alt = self.kite.quote([token])
                    if quote_alt and str(token) in quote_alt:
                        data = quote_alt[str(token)]
                    else:
                        logger.warning(f"⚠️ No data received for {symbol} (token: {token})")
                        if symbol == 'NSE:INDIA VIX':
                            logger.error(f"❌ CRITICAL: VIX data not available. Quote response: {quote}")
                        continue

                # Validate data before processing (especially important for VIX)
                last_price = data.get('last_price', 0)
                if symbol == 'NSE:INDIA VIX' and (not last_price or last_price == 0):
                    logger.warning(f"⚠️ VIX last_price is 0 or None. Raw data: {data}")
                    # Try to get from OHLC if last_price is missing
                    ohlc = data.get('ohlc', {})
                    last_price = ohlc.get('close', 0) or ohlc.get('last_price', 0)
                    if last_price and last_price > 0:
                        logger.info(f"✅ Using VIX close price from OHLC: {last_price}")
                    else:
                        logger.error(f"❌ VIX data invalid - last_price: {last_price}, data keys: {list(data.keys())}")
                        continue  # Skip this update if VIX data is invalid

                # Extract ONLY the fields that Zerodha actually provides
                ohlc = data.get('ohlc', {})
                close_price = ohlc.get('close', 0) or last_price
                
                # Calculate percent_change from close price (more accurate than net_change)
                net_change_from_close = last_price - close_price if close_price > 0 else data.get('net_change', 0)
                percent_change = (net_change_from_close / close_price * 100) if close_price > 0 else 0
                
                index_info = {
                    'symbol': symbol,
                    'instrument_token': data.get('instrument_token', token),
                    'tradingsymbol': data.get('tradingsymbol', symbol.split(':')[-1]),
                    'timestamp': data.get('timestamp', datetime.now()).isoformat() if data.get('timestamp') else datetime.now().isoformat(),
                    'last_price': last_price,
                    'net_change': net_change_from_close,  # Use calculated net_change from close
                    'ohlc': ohlc,
                    # Add calculated fields for backtesting
                    'percent_change': percent_change,
                    'timestamp_ns': int(data.get('timestamp', datetime.now()).timestamp() * 1_000_000_000) if data.get('timestamp') else int(datetime.now().timestamp() * 1_000_000_000),
                    'exchange': symbol.split(':')[0] if ':' in symbol else 'NSE',
                    'segment': 'INDEX' if 'NIFTY' in symbol or 'VIX' in symbol else 'EQUITY',
                    'mode': 'quote',  # Indicates this is from REST API quote
                    # Add news/sentiment data for backtesting
                    'news_sentiment_positive': len([item for item in self.news_data if item.get('sentiment') == 'positive']),
                    'news_sentiment_negative': len([item for item in self.news_data if item.get('sentiment') == 'negative']),
                    'news_sentiment_neutral': len([item for item in self.news_data if item.get('sentiment') == 'neutral']),
                    'news_total_count': len(self.news_data),
                    'news_sentiment_score': self._calculate_news_sentiment_score(),
                    'market_news_available': len(self.news_data) > 0,
                    'news_sources': list(set(item.get('publisher', '') for item in self.news_data if item.get('publisher'))),
                    'news_last_updated': datetime.now().isoformat() if self.news_data else ''
                }

                # Store locally
                self.index_data[symbol] = index_info
                self.last_updates[symbol] = datetime.now()

                # Write to JSONL for backtesting
                self._write_to_jsonl(index_info)

                # Publish to Redis
                self._publish_to_redis(symbol, index_info)

                updated_count += 1
                logger.info(f"✅ Updated {symbol}: {index_info['last_price']:.2f} pts ({index_info['percent_change']:+.2f}%)")

            except Exception as e:
                logger.error(f"❌ Error updating {symbol}: {e}")

        if updated_count > 0:
            logger.info(f"📊 Successfully updated {updated_count}/{len(self.indices)} indices")

            # Publish market context for AION
            self._publish_market_context()

            # Publish news data
            self._publish_news_data()

            # Calculate and publish gap analysis
            self._publish_gap_analysis()

    def _publish_to_redis(self, symbol, data):
        """Publish index data to Redis in DB 1 (unified structure)"""
        if not self.redis_initialized:
            return

        try:
            # ✅ FIX: Use only DB 1 client (unified structure)
            realtime_client = self.redis_client
            
            if not realtime_client:
                logger.warning(f"⚠️ No Redis client available for publishing {symbol}")
                return

            alias_symbol = RedisKeyStandards.canonical_symbol(symbol)
            symbol_variants = [symbol]
            if alias_symbol and alias_symbol not in symbol_variants:
                symbol_variants.append(alias_symbol)
            
            payload = json.dumps(data)
            
            def _market_suffix(sym: str, replace_spaces: bool) -> str:
                suffix = sym.replace(':', '_')
                suffix = suffix.replace(' ', '_' if replace_spaces else ' ')
                return suffix.lower()
            
            for sym_variant in symbol_variants:
                # Publish realtime channel
                channel = f"index:{sym_variant}"
                realtime_client.publish(channel, payload)
                
                # Persist JSON payload under multiple key shapes
                json_key = f"index:{sym_variant}"
                realtime_client.set(json_key, payload)
                
                legacy_suffix = _market_suffix(sym_variant, replace_spaces=False)
                realtime_client.set(f"market_data:indices:{legacy_suffix}", payload)
                
                underscore_suffix = _market_suffix(sym_variant, replace_spaces=True)
                if underscore_suffix != legacy_suffix:
                    realtime_client.set(f"market_data:indices:{underscore_suffix}", payload)
                
                # Hash storage for fast field access
                hash_key = f"index_hash:{sym_variant}"
                hash_data = {
                    'last_price': str(data.get('last_price', 0)),
                    'change': str(data.get('change', 0)),
                    'percent_change': str(data.get('percent_change', 0)),
                    'net_change': str(data.get('net_change', 0)),
                    'timestamp': data.get('timestamp', ''),
                    'symbol': data.get('symbol', sym_variant),
                }
                realtime_client.hset(hash_key, mapping=hash_data)
                realtime_client.expire(hash_key, 60)  # 60 second TTL (updated every 30s)
            
            # ✅ NEW: Publish to indices:stream for ClickHouse ingestion (port 6379, DB 1)
            # Also publish to port 6380 for clickhouse_pipeline.py
            try:
                # Format indices data for ClickHouse schema
                ch_indices_data = {
                    'instrument_token': str(data.get('instrument_token', 0)),
                    'symbol': data.get('symbol', symbol),
                    'tradingsymbol': data.get('tradingsymbol', symbol),
                    'exchange': data.get('exchange', 'NSE'),
                    'segment': 'INDEX',
                    'mode': 'quote',
                    'timestamp': data.get('timestamp', datetime.now().isoformat()),
                    'timestamp_ns': int(data.get('timestamp_ns', 0)) if data.get('timestamp_ns') else 0,
                    'last_price': float(data.get('last_price', 0)),
                    'net_change': float(data.get('net_change', 0)),
                    'percent_change': float(data.get('percent_change', 0)),
                    'open': float(data.get('open', 0)) if data.get('open') else 0.0,
                    'high': float(data.get('high', 0)) if data.get('high') else 0.0,
                    'low': float(data.get('low', 0)) if data.get('low') else 0.0,
                    'close': float(data.get('close', 0)) if data.get('close') else 0.0,
                    'volume': int(data.get('volume', 0)) if data.get('volume') else 0,
                    'news_sentiment_positive': int(data.get('news_sentiment_positive', 0)),
                    'news_sentiment_negative': int(data.get('news_sentiment_negative', 0)),
                    'news_sentiment_neutral': int(data.get('news_sentiment_neutral', 0)),
                    'news_total_count': int(data.get('news_total_count', 0)),
                    'news_sentiment_score': float(data.get('news_sentiment_score', 0.0)),
                    'market_news_available': 1 if data.get('market_news_available', False) else 0,
                    'news_sources': data.get('news_sources', []) if isinstance(data.get('news_sources'), list) else [],
                    'news_last_updated': data.get('news_last_updated', '') if data.get('news_last_updated') else ''
                }
                
                # Publish to stream on port 6379 (for redis_clickhouse_bridge)
                realtime_client.xadd('indices:stream', {k: str(v) if not isinstance(v, (list, dict)) else json.dumps(v) for k, v in ch_indices_data.items()}, maxlen=10000, approximate=True)
                
                # Also publish to port 6380 for clickhouse_pipeline.py
                try:
                    import redis
                    redis_6380 = redis.Redis(host='localhost', port=6380, db=0, decode_responses=False)
                    redis_6380.xadd('indices:stream', {k: str(v) if not isinstance(v, (list, dict)) else json.dumps(v) for k, v in ch_indices_data.items()}, maxlen=10000, approximate=True)
                except Exception as e:
                    logger.debug(f"Could not publish to port 6380 (optional): {e}")
            except Exception as e:
                logger.warning(f"⚠️ Could not publish to indices:stream: {e}")

        except Exception as e:
            logger.error(f"❌ Redis publish error for {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _publish_market_context(self):
        """Publish market context for AION U-shaped volume formula"""
        try:
            context = {
                'timestamp': datetime.now().isoformat(),
                'indices': {},
                'market_regime': self._calculate_market_regime(),
                'volatility_context': self._calculate_volatility_context(),
                'news': self.news_data  # Include news data
            }

            # Add index data
            for symbol, data in self.index_data.items():
                context['indices'][symbol] = {
                    'price': data['last_price'],
                    'change_pct': data['percent_change'],
                    'display_value': f"{data['last_price']:.2f} pts"  # Add display format
                }

            # ✅ FIX: Use only DB 1 for realtime data (unified structure)
            if self.redis_initialized:
                redis_client = self.redis_client
                if redis_client:
                    redis_client.publish('market:context', json.dumps(context))
                    redis_client.set('market:context:latest', json.dumps(context))

            logger.info("📡 Published market context for AION calculations")

        except Exception as e:
            logger.error(f"❌ Error publishing market context: {e}")

    def _publish_news_data(self):
        """Publish news data to Redis and write to disk"""
        if not self.redis_initialized or not self.news_data or not self.redis_client:
            if not self.redis_client:
                logger.warning("⚠️ Redis DB 1 client not available for news publishing")
            return
        
        try:
            # Write news to disk first
            self._write_news_to_disk()
            
            # ✅ FIX: Use only DB 1 client for all news operations (unified structure)
            redis_client = self.redis_client
            
            # Publish individual news items AND store them persistently
            for i, news_item in enumerate(self.news_data):
                # Publish to channel
                redis_client.publish('market_data.news', json.dumps(news_item))
                
                # Store individual news item persistently in Redis DB 1
                news_key = f"news:item:{datetime.now().strftime('%Y%m%d')}:{i}"
                redis_client.setex(news_key, 86400, json.dumps(news_item))  # Store for 24 hours
                
                # Also store using store_news_sentiment format (news:MARKET_NEWS:timestamp) for dashboard compatibility
                import time
                timestamp = int(time.time())
                market_news_key = f"news:MARKET_NEWS:{timestamp}"
                redis_client.setex(market_news_key, 10800, json.dumps(news_item))  # Store for 3 hours (180 mins + buffer)
                
                # NEW: Publish to symbol-specific keys for DataPipeline consumption
                self._publish_news_to_symbols(news_item, redis_client)
            
            # Publish news summary
            news_summary = {
                'timestamp': datetime.now().isoformat(),
                'total_news': len(self.news_data),
                'sources': list(set(item['publisher'] for item in self.news_data)),
                'sentiments': {
                    'positive': len([item for item in self.news_data if item['sentiment'] == 'positive']),
                    'negative': len([item for item in self.news_data if item['sentiment'] == 'negative']),
                    'neutral': len([item for item in self.news_data if item['sentiment'] == 'neutral'])
                }
            }
            
            redis_client.publish('market_data.news_summary', json.dumps(news_summary))
            redis_client.set('market_data.news:latest', json.dumps(news_summary))
            
            # ✅ NEW: Publish to news:stream for ClickHouse ingestion (port 6379, DB 1)
            # Also publish to port 6380 for clickhouse_pipeline.py
            try:
                # Publish each news item to stream on port 6379 (for redis_clickhouse_bridge)
                for news_item in self.news_data:
                    # Format news item for ClickHouse schema
                    ch_news_data = {
                        'source': news_item.get('publisher', 'zerodha_pulse'),
                        'title': news_item.get('title', ''),
                        'link': news_item.get('link', ''),
                        'published_at': news_item.get('written_at', datetime.now().isoformat()),
                        'sentiment': 1.0 if news_item.get('sentiment') == 'positive' else (-1.0 if news_item.get('sentiment') == 'negative' else 0.0),
                        'sentiment_confidence': news_item.get('sentiment_confidence', 0.5),
                        'categories': news_item.get('sector_relevance', []) if isinstance(news_item.get('sector_relevance'), list) else []
                    }
                    # Convert lists/dicts to JSON strings for Redis stream compatibility
                    stream_data = {k: str(v) if not isinstance(v, (list, dict)) else json.dumps(v) for k, v in ch_news_data.items()}
                    redis_client.xadd('news:stream', stream_data, maxlen=10000, approximate=True)
                
                # Also publish to port 6380 for clickhouse_pipeline.py
                try:
                    import redis
                    redis_6380 = redis.Redis(host='localhost', port=6380, db=0, decode_responses=False)
                    for news_item in self.news_data:
                        ch_news_data = {
                            'source': news_item.get('publisher', 'zerodha_pulse'),
                            'title': news_item.get('title', ''),
                            'link': news_item.get('link', ''),
                            'published_at': news_item.get('written_at', datetime.now().isoformat()),
                            'sentiment': 1.0 if news_item.get('sentiment') == 'positive' else (-1.0 if news_item.get('sentiment') == 'negative' else 0.0),
                            'sentiment_confidence': news_item.get('sentiment_confidence', 0.5),
                            'categories': news_item.get('sector_relevance', []) if isinstance(news_item.get('sector_relevance'), list) else []
                        }
                        redis_6380.xadd('news:stream', {k: str(v) if not isinstance(v, (list, dict)) else json.dumps(v) for k, v in ch_news_data.items()}, maxlen=10000, approximate=True)
                except Exception as e:
                    logger.debug(f"Could not publish to port 6380 (optional): {e}")
            except Exception as e:
                logger.warning(f"⚠️ Could not publish to news:stream: {e}")
            
            logger.info(f"📰 Published {len(self.news_data)} news items to Redis DB 1 and disk")

        except Exception as e:
            logger.error(f"❌ Error publishing news data: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _publish_news_to_symbols(self, news_item, redis_client=None):
        """Publish news to symbol-specific keys for DataPipeline consumption"""
        if redis_client is None:
            redis_client = self.redis_client
        
        if not redis_client:
            return
            
        try:
            # Extract relevant symbols from news content
            relevant_symbols = self._extract_symbols_from_news(news_item)
            
            for symbol in relevant_symbols:
                symbol_key = f"news:symbol:{symbol}"
                
                # Store in sorted set with timestamp as score (DataPipeline expects this)
                timestamp = time_module.time()
                
                # Convert sentiment to numeric value for DataPipeline
                sentiment_str = news_item.get('sentiment', 'neutral')
                sentiment_numeric = 0.0
                if sentiment_str == 'positive':
                    sentiment_numeric = 0.8
                elif sentiment_str == 'negative':
                    sentiment_numeric = -0.8
                elif sentiment_str == 'neutral':
                    sentiment_numeric = 0.0
                
                news_data = {
                    "data": {
                        **news_item,
                        "sentiment": sentiment_numeric,  # Numeric for DataPipeline
                        "volume_trigger": self._is_volume_trigger_news(news_item)
                    },
                    "source": "zerodha_pulse",
                    "timestamp": datetime.now().isoformat(),
                    "sentiment": sentiment_numeric,  # Also at top level
                    "volume_trigger": self._is_volume_trigger_news(news_item)
                }
                
                # Add to sorted set (DataPipeline uses zrevrangebyscore) - use DB 1
                redis_client.zadd(symbol_key, {json.dumps(news_data): timestamp})
                # Set expiry on the sorted set
                redis_client.expire(symbol_key, 300)  # 5 minute TTL
                
                # Also publish to symbol-specific channel
                redis_client.publish(f"news:symbol:{symbol}", json.dumps(news_data))
                
            if relevant_symbols:
                logger.info(f"📰 Published news to {len(relevant_symbols)} symbols: {relevant_symbols}")
                
        except Exception as e:
            logger.error(f"❌ Error publishing news to symbols: {e}")

    def _is_volume_trigger_news(self, news_item):
        """Check if news is likely to trigger volume"""
        title = news_item.get('title', '').upper()
        volume_keywords = ['BREAKING', 'URGENT', 'ALERT', 'SURGE', 'SPIKE', 'RALLY', 'CRASH', 'PLUNGE']
        return any(keyword in title for keyword in volume_keywords)

    def _extract_underlying_symbol(self, symbol: str) -> str:
        """Extract underlying symbol from option/future symbols"""
        if not symbol:
            return symbol
        
        symbol_upper = symbol.upper()
        if ':' in symbol_upper:
            symbol_upper = symbol_upper.split(':')[-1]
        
        index_match = re.search(r'(BANKNIFTY|NIFTY|FINNIFTY|MIDCPNIFTY)', symbol_upper)
        if index_match:
            return index_match.group(1)
        
        if 'CE' in symbol_upper or 'PE' in symbol_upper:
            match = re.search(r'^([A-Z]+)\d{2}[A-Z]{3}', symbol_upper)
            if match:
                return match.group(1)
            match = re.search(r'^([A-Z]+)\d', symbol_upper)
            if match:
                return match.group(1)
        
        if symbol_upper.endswith('FUT'):
            match = re.search(r'^([A-Z]+)\d', symbol_upper)
            if match:
                return match.group(1)
        
        return symbol_upper

    def _get_sector_from_symbol(self, symbol: str) -> str:
        """Map symbol to sector based on naming patterns"""
        if not symbol:
            return ""
        
        symbol_upper = symbol.upper()
        bank_keywords = ['BANK', 'HDFC', 'ICICI', 'SBI', 'AXIS', 'KOTAK', 'INDUSIND', 
                         'YESBANK', 'FEDERAL', 'IDFC', 'BANDHAN', 'RBL', 'UNION',
                         'PNB', 'CANARA', 'BANKOFBARODA', 'BANKOFINDIA']
        it_keywords = ['INFOSYS', 'TCS', 'WIPRO', 'HCL', 'TECHMAHINDRA', 'LTIM', 
                       'LTTS', 'PERSISTENT', 'MINDTREE', 'COFORGE', 'MPHASIS']
        auto_keywords = ['MARUTI', 'M&M', 'TATA', 'MOTORS', 'BAJAJ', 'HERO', 
                         'EICHER', 'ASHOK', 'LEYLAND', 'TVS', 'MOTHERSUM']
        pharma_keywords = ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'TORRENT', 
                           'AUROBINDO', 'DIVIS', 'GLENMARK', 'CADILA']
        fmcg_keywords = ['HUL', 'ITC', 'NESTLE', 'DABUR', 'MARICO', 'BRITANNIA', 
                         'GODREJ', 'TATA', 'CONSUMER']
        energy_keywords = ['RELIANCE', 'ONGC', 'GAIL', 'IOC', 'BPCL', 'HPCL', 
                           'OIL', 'PETRONET']
        metal_keywords = ['TATASTEEL', 'JSWSTEEL', 'SAIL', 'JINDAL', 'VEDANTA', 
                          'HINDALCO', 'NMDC']
        
        for keyword in bank_keywords:
            if keyword in symbol_upper:
                return "BANK"
        for keyword in it_keywords:
            if keyword in symbol_upper:
                return "IT"
        for keyword in auto_keywords:
            if keyword in symbol_upper:
                return "AUTO"
        for keyword in pharma_keywords:
            if keyword in symbol_upper:
                return "PHARMA"
        for keyword in fmcg_keywords:
            if keyword in symbol_upper:
                return "FMCG"
        for keyword in energy_keywords:
            if keyword in symbol_upper:
                return "ENERGY"
        for keyword in metal_keywords:
            if keyword in symbol_upper:
                return "METAL"
        return ""

    def _extract_symbols_from_news(self, news_item):
        """Extract relevant symbols from news using company name matching and sectoral mapping"""
        title = news_item.get('title', '')
        content = news_item.get('content', '')
        full_content = f"{title} {content}".strip()
        
        if not full_content:
            return ['NSE:NIFTY BANK', 'NSE:NIFTY 50']
        
        text_upper = full_content.upper()
        result_symbols = []
        symbol_set = set()
        sectors = set()
        matched_symbols = set()
        
        # Load token lookup for sector information
        token_lookup = {}
        try:
            token_lookup_file = Path(__file__).parent.parent / "core" / "data" / "token_lookup_enriched.json"
            if token_lookup_file.exists():
                with open(token_lookup_file, 'r') as f:
                    token_lookup = json.load(f)
        except Exception as e:
        
        # Step 1: Match company names from news text to company_symbol_map
        # Only match equity stocks, exclude options/futures
            if self.company_symbol_map:
                for company_name, symbol_list in self.company_symbol_map.items():
                    # Skip very short company names (likely false matches)
                    if len(company_name) < 4:
                        continue
                    
                # Check if company name appears in news text (exact match or significant words)
                company_words = [w for w in company_name.split() if len(w) > 3]
                if company_name in text_upper or (company_words and any(word in text_upper for word in company_words)):
                    for symbol_info in symbol_list:
                        symbol = symbol_info.get('symbol', '')
                        full_symbol = symbol_info.get('full_symbol', '')
                        instrument_type = symbol_info.get('instrument_type', '')
                        
                        # Skip options, futures, and indices - only match equity stocks
                        if instrument_type in ['CE', 'PE', 'FUT', 'OPT']:
                            continue
                        if 'CE' in symbol or 'PE' in symbol or 'FUT' in symbol:
                            continue
                        if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX']:
                            continue
                        
                        if full_symbol:
                            # Get sector from token lookup
                            for token_str, inst_data in token_lookup.items():
                                if inst_data.get('key') == full_symbol:
                                    sector = inst_data.get('sector', '').upper()
                                    if sector and sector not in ['UNKNOWN', 'OTHER', 'MISSING', '']:
                                        sectors.add(sector)
                                    
                                    # Add the symbol (only equity stocks)
                                    inst_type = inst_data.get('instrument_type', '')
                                    if inst_type not in ['CE', 'PE', 'FUT', 'OPT'] and full_symbol not in symbol_set:
                                        matched_symbols.add(full_symbol)
                                        symbol_set.add(full_symbol)
                                    break
                        elif symbol and symbol not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY']:
                            # Fallback to symbol matching (only equity)
                            nse_symbol = f"NSE:{symbol}" if ':' not in symbol else symbol
                            if 'CE' not in nse_symbol and 'PE' not in nse_symbol and 'FUT' not in nse_symbol:
                                if nse_symbol not in symbol_set:
                                    matched_symbols.add(nse_symbol)
                                    symbol_set.add(nse_symbol)
        
        # Step 2: Add matched symbols to result
        for symbol in matched_symbols:
            if ':' not in symbol:
                nse_symbol = f"NSE:{symbol}"
                result_symbols.append(nse_symbol)
            else:
                result_symbols.append(symbol)
        
        # Step 3: Route to indices based on sectoral mapping
        # Banking sector companies → NIFTY BANK
        if 'BANK' in sectors or any('BANK' in str(s).upper() for s in sectors):
            if 'NSE:NIFTY BANK' not in symbol_set:
                result_symbols.append('NSE:NIFTY BANK')
                symbol_set.add('NSE:NIFTY BANK')
        
        # Check for explicit index mentions in text
        if 'BANKNIFTY' in text_upper or 'BANK NIFTY' in text_upper:
            if 'NSE:NIFTY BANK' not in symbol_set:
                result_symbols.append('NSE:NIFTY BANK')
                symbol_set.add('NSE:NIFTY BANK')
        
        if 'FINNIFTY' in text_upper:
            if 'NSE:FINNIFTY' not in symbol_set:
                result_symbols.append('NSE:FINNIFTY')
                symbol_set.add('NSE:FINNIFTY')
        
        # Generic NIFTY mention (only if not already covered)
        if 'NIFTY' in text_upper and 'NSE:NIFTY BANK' not in symbol_set and 'NSE:FINNIFTY' not in symbol_set:
            if 'NSE:NIFTY 50' not in symbol_set:
                result_symbols.append('NSE:NIFTY 50')
                symbol_set.add('NSE:NIFTY 50')
        
        # If we found any company symbols but no indices, default to NIFTY 50
        if matched_symbols and not any('NIFTY' in s for s in result_symbols):
            if 'NSE:NIFTY 50' not in symbol_set:
                result_symbols.append('NSE:NIFTY 50')
                symbol_set.add('NSE:NIFTY 50')
        
        # Default fallback
        if not result_symbols:
            result_symbols = ['NSE:NIFTY BANK', 'NSE:NIFTY 50']
        
        return result_symbols

    def _write_news_to_disk(self):
        """Write news data to disk with sentiment analysis - ALWAYS WRITES"""
        if not self.news_data:
            logger.warning("⚠️ No news data to write to disk")
            return
            
        try:
            # Create news directory under canonical tick data path
            news_dir = Path("config/data/indices/news")
            news_dir.mkdir(parents=True, exist_ok=True)
            
            # Create daily news file
            date_str = datetime.now().strftime("%Y%m%d")
            news_file = news_dir / f"news_{date_str}.jsonl"
            
            # Enhanced news items with detailed sentiment analysis
            enhanced_news = []
            for item in self.news_data:
                enhanced_item = {
                    **item,
                    'sentiment_score': self._calculate_sentiment_score(item['title']),
                    'sentiment_confidence': self._calculate_sentiment_confidence(item['title']),
                    'market_impact': self._assess_market_impact(item['title']),
                    'sector_relevance': self._extract_sector_relevance(item['title']),
                    'written_at': datetime.now().isoformat()
                }
                enhanced_news.append(enhanced_item)
            
            # Write to JSONL file with immediate flush
            with open(news_file, 'a', encoding='utf-8') as f:
                for item in enhanced_news:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
                f.flush()  # Force write to disk
                os.fsync(f.fileno())  # Ensure OS writes to disk
            
            logger.info(f"📝 Written {len(enhanced_news)} news items to {news_file}")
            
            # Verify file was written
            if news_file.exists():
                file_size = news_file.stat().st_size
                logger.info(f"✅ News file verified: {news_file} ({file_size} bytes)")
            else:
                logger.error(f"❌ News file not found after write: {news_file}")
            
        except Exception as e:
            logger.error(f"❌ CRITICAL: Error writing news to disk: {e}")
            # Try to write a minimal backup
            try:
                backup_file = Path("config/data/indices/news") / f"news_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                with open(backup_file, 'w', encoding='utf-8') as f:
                    for item in self.news_data:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
                logger.info(f"🆘 Backup news written to: {backup_file}")
            except Exception as backup_error:
                logger.error(f"❌ Backup write also failed: {backup_error}")

    def _calculate_sentiment_score(self, text):
        """Calculate detailed sentiment score (-1.0 to 1.0)"""
        positive_words = {
            'bullish': 0.8, 'rise': 0.6, 'gain': 0.7, 'up': 0.5, 'positive': 0.8,
            'growth': 0.7, 'profit': 0.8, 'earnings': 0.6, 'beat': 0.9, 'exceed': 0.8,
            'surge': 0.9, 'rally': 0.8, 'strong': 0.6, 'robust': 0.7, 'outperform': 0.8,
            'breakthrough': 0.9, 'milestone': 0.7, 'record': 0.8, 'high': 0.6
        }
        
        negative_words = {
            'bearish': -0.8, 'fall': -0.6, 'drop': -0.7, 'down': -0.5, 'negative': -0.8,
            'loss': -0.8, 'miss': -0.9, 'decline': -0.7, 'crash': -0.9, 'plunge': -0.9,
            'slump': -0.8, 'dip': -0.6, 'weak': -0.6, 'poor': -0.7, 'underperform': -0.8,
            'crisis': -0.9, 'concern': -0.5, 'worry': -0.6, 'risk': -0.4, 'low': -0.6
        }
        
        text_lower = text.lower()
        positive_score = sum(score for word, score in positive_words.items() if word in text_lower)
        negative_score = sum(score for word, score in negative_words.items() if word in text_lower)
        
        total_score = positive_score + negative_score
        return max(-1.0, min(1.0, total_score))

    def _calculate_sentiment_confidence(self, text):
        """Calculate confidence in sentiment analysis (0.0 to 1.0)"""
        positive_words = ['bullish', 'rise', 'gain', 'up', 'positive', 'growth', 'profit', 'earnings', 'beat', 'exceed', 'surge', 'rally']
        negative_words = ['bearish', 'fall', 'drop', 'down', 'negative', 'loss', 'miss', 'decline', 'crash', 'plunge', 'slump', 'dip']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        total_sentiment_words = positive_count + negative_count
        if total_sentiment_words == 0:
            return 0.3  # Low confidence for neutral text
        
        # Higher confidence with more sentiment words
        confidence = min(1.0, 0.3 + (total_sentiment_words * 0.1))
        return confidence

    def _assess_market_impact(self, text):
        """Assess potential market impact (LOW, MEDIUM, HIGH)"""
        high_impact_words = ['earnings', 'results', 'guidance', 'forecast', 'outlook', 'crisis', 'breakthrough', 'merger', 'acquisition', 'fda', 'approval', 'rejection']
        medium_impact_words = ['analyst', 'upgrade', 'downgrade', 'target', 'price', 'rating', 'recommendation', 'sector', 'industry']
        
        text_lower = text.lower()
        high_count = sum(1 for word in high_impact_words if word in text_lower)
        medium_count = sum(1 for word in medium_impact_words if word in text_lower)
        
        if high_count > 0:
            return 'HIGH'
        elif medium_count > 0:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _extract_sector_relevance(self, text):
        """Extract relevant sectors from news text"""
        sectors = {
            'banking': ['bank', 'financial', 'credit', 'lending', 'nbfc', 'fintech'],
            'technology': ['tech', 'software', 'digital', 'ai', 'cloud', 'cyber', 'it'],
            'pharma': ['pharma', 'drug', 'medicine', 'fda', 'clinical', 'biotech'],
            'auto': ['auto', 'vehicle', 'car', 'truck', 'motor', 'electric'],
            'energy': ['oil', 'gas', 'energy', 'power', 'renewable', 'solar', 'wind'],
            'metals': ['steel', 'metal', 'mining', 'aluminum', 'copper', 'iron'],
            'fmcg': ['fmcg', 'consumer', 'retail', 'brand', 'product', 'goods'],
            'infrastructure': ['infra', 'construction', 'cement', 'road', 'bridge', 'project']
        }
        
        text_lower = text.lower()
        relevant_sectors = []
        
        for sector, keywords in sectors.items():
            if any(keyword in text_lower for keyword in keywords):
                relevant_sectors.append(sector)
        
        return relevant_sectors if relevant_sectors else ['general']

    def _publish_gap_analysis(self):
        """Publish gap analysis data (legacy compatibility)"""
        try:
            if 'NSEIX:GIFT NIFTY' not in self.index_data or 'NSE:NIFTY 50' not in self.index_data:
                return

            gift_data = self.index_data['NSEIX:GIFT NIFTY']
            nifty_data = self.index_data['NSE:NIFTY 50']

            gift_price = gift_data['last_price']
            nifty_price = nifty_data['last_price']

            gap_points = gift_price - nifty_price
            gap_percent = (gap_points / nifty_price) * 100 if nifty_price > 0 else 0

            gap_analysis = {
                'timestamp': datetime.now().isoformat(),
                'gift_price': gift_price,
                'nifty_price': nifty_price,  # Current NIFTY price instead of yesterday's close
                'gap_points': gap_points,
                'gap_percent': gap_percent,
                'signal': self._get_gap_signal(gap_percent)
            }

            # ✅ FIX: Use only DB 1 for realtime data (unified structure)
            if self.redis_initialized:
                redis_client = self.redis_client
                if redis_client:
                    redis_client.publish('market.gift_nifty.gap', json.dumps(gap_analysis))
                    redis_client.set('latest_gift_nifty_gap', json.dumps(gap_analysis))

            # Save to JSON file for scanner compatibility
            self._save_gap_to_file(gap_analysis)

            logger.info(f"📊 Gap Analysis: {gap_percent:+.2f}% ({gap_points:+.0f} pts)")

        except Exception as e:
            logger.error(f"❌ Error in gap analysis: {e}")

    def _save_gap_to_file(self, gap_data):
        """Save gap data to JSON file for scanner compatibility"""
        try:
            # Save to market microstructure path within repository
            # Store gap analysis under canonical tick data path
            microstructure_dir = os.path.join(os.path.dirname(__file__), '../config/data/indices/gap_analysis')
            os.makedirs(microstructure_dir, exist_ok=True)

            date_str = datetime.now().strftime("%Y%m%d")
            filename = os.path.join(microstructure_dir, f'gift_nifty_gap_{date_str}.json')

            with open(filename, 'w') as f:
                json.dump(gap_data, f, indent=2)

        except Exception as e:
            logger.error(f"❌ Error saving gap data to file: {e}")

    def _calculate_market_regime(self):
        """Calculate current market regime for AION using VIX and NIFTY"""
        try:
            regime_factors = []
            
            # VIX-based regime detection
            if 'NSE:INDIA VIX' in self.index_data:
                vix_data = self.index_data['NSE:INDIA VIX']
                vix_value = vix_data['last_price']
                
                if vix_value > 22:
                    regime_factors.append('PANIC')
                elif vix_value < 12:
                    regime_factors.append('COMPLACENT')
                else:
                    regime_factors.append('NORMAL')
            
            # NIFTY-based regime detection
            if 'NSE:NIFTY 50' in self.index_data:
                nifty_data = self.index_data['NSE:NIFTY 50']
                change_pct = nifty_data['percent_change']

                if change_pct > 1.0:
                    regime_factors.append('BULLISH')
                elif change_pct < -1.0:
                    regime_factors.append('BEARISH')
                else:
                    regime_factors.append('SIDEWAYS')
            
            # Combine regime factors
            if not regime_factors:
                return 'UNKNOWN'
            
            # Return primary regime (VIX takes precedence for volatility)
            if 'PANIC' in regime_factors:
                return 'PANIC'
            elif 'COMPLACENT' in regime_factors:
                return 'COMPLACENT'
            elif 'NORMAL' in regime_factors:
                return 'NORMAL'
            elif 'BULLISH' in regime_factors:
                return 'BULLISH'
            elif 'BEARISH' in regime_factors:
                return 'BEARISH'
            else:
                return 'SIDEWAYS'

        except Exception as e:
            logger.error(f"❌ Error calculating market regime: {e}")
            return 'UNKNOWN'

    def _calculate_volatility_context(self):
        """Calculate volatility context for AION including VIX"""
        try:
            context = {}
            
            # VIX-based volatility context
            if 'NSE:INDIA VIX' in self.index_data:
                vix_data = self.index_data['NSE:INDIA VIX']
                vix_value = vix_data['last_price']
                
                context.update({
                    'vix_value': round(vix_value, 2),
                    'vix_regime': 'PANIC' if vix_value > 22 else 'COMPLACENT' if vix_value < 12 else 'NORMAL',
                    'volatility_level': 'HIGH' if vix_value > 20 else 'LOW' if vix_value < 15 else 'MEDIUM'
                })
            
            # SGX GIFT NIFTY gap analysis
            if 'NSEIX:GIFT NIFTY' in self.index_data and 'NSE:NIFTY 50' in self.index_data:
                sgx_price = self.index_data['NSEIX:GIFT NIFTY']['last_price']
                nifty_price = self.index_data['NSE:NIFTY 50']['last_price']

                if nifty_price > 0:
                    gap_pct = ((sgx_price - nifty_price) / nifty_price) * 100
                    context.update({
                        'sgx_nifty_gap_pct': round(gap_pct, 2),
                        'premarket_bias': 'BULLISH' if gap_pct > 0.1 else 'BEARISH' if gap_pct < -0.1 else 'NEUTRAL'
                    })

            return context if context else {'sgx_nifty_gap_pct': 0, 'premarket_bias': 'UNKNOWN'}

        except Exception as e:
            logger.error(f"❌ Error calculating volatility context: {e}")
            return {'sgx_nifty_gap_pct': 0, 'premarket_bias': 'UNKNOWN'}

    def _get_gap_signal(self, gap_percent):
        """Generate trading signal based on gap"""
        if gap_percent > 0.75:
            return "STRONG_GAP_UP - Consider gap fade short"
        elif gap_percent > 0.3:
            return "GAP_UP - Watch for fade"
        elif gap_percent < -0.75:
            return "STRONG_GAP_DOWN - Consider gap fade long"
        elif gap_percent < -0.3:
            return "GAP_DOWN - Watch for bounce"
        else:
            return "FLAT_OPEN - No gap trade"

    def get_index_data(self, symbol=None):
        """Get current index data"""
        if symbol:
            return self.index_data.get(symbol)
        return self.index_data

    def get_market_context(self):
        """Get current market context for AION"""
        return {
            'indices': self.index_data,
            'regime': self._calculate_market_regime(),
            'volatility': self._calculate_volatility_context(),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_current_vix(self):
        """Get current VIX value for retail validation logic"""
        try:
            if 'NSE:INDIA VIX' in self.index_data:
                vix_data = self.index_data['NSE:INDIA VIX']
                return {
                    'value': vix_data['last_price'],
                    'regime': 'PANIC' if vix_data['last_price'] > 22 else 'COMPLACENT' if vix_data['last_price'] < 12 else 'NORMAL',
                    'timestamp': vix_data['timestamp']
                }
            return None
        except Exception as e:
            logger.error(f"❌ Error getting current VIX: {e}")
            return None


# Legacy GIFTNiftyGapAnalyzer class for backward compatibility
class GIFTNiftyGapAnalyzer(IndexDataUpdater):
    """Legacy compatibility wrapper"""

    def __init__(self):
        super().__init__()

    def get_gap_data(self):
        """Legacy method for gap data"""
        self.update_indices()
        return self._get_legacy_gap_format()

    def _get_legacy_gap_format(self):
        """Convert current data to legacy gap format"""
        try:
            if 'NSEIX:GIFT NIFTY' not in self.index_data or 'NSE:NIFTY 50' not in self.index_data:
                return None

            gift_data = self.index_data['NSEIX:GIFT NIFTY']
            nifty_data = self.index_data['NSE:NIFTY 50']

            gift_price = gift_data['last_price']
            nifty_price = nifty_data['last_price']

            gap_points = gift_price - nifty_price
            gap_percent = (gap_points / nifty_price) * 100 if nifty_price > 0 else 0

            return {
                'timestamp': datetime.fromisoformat(gift_data['timestamp']),
                'gift_price': gift_price,
                'nifty_close': nifty_price,  # Use current price for compatibility
                'gap_points': gap_points,
                'gap_percent': gap_percent,
                'signal': self._get_gap_signal(gap_percent),
                'targets': None  # Simplified for legacy compatibility
            }
        except Exception as e:
            logger.error(f"❌ Error in legacy gap format: {e}")
            return None


if __name__ == "__main__":
    import sys

    # Check for continuous mode
    continuous = '--continuous' in sys.argv

    if continuous:
        # Use new IndexDataUpdater for continuous updates
        updater = IndexDataUpdater()
        updater.start_continuous_updates()
    else:
        # Legacy single-run mode
        analyzer = GIFTNiftyGapAnalyzer()

        print("INDEX DATA UPDATER - AION Market Context")
        print("="*60)

        gap_data = analyzer.get_gap_data()
        if gap_data:
            print(f"Time: {gap_data['timestamp'].strftime('%H:%M:%S')}")
            print(f"GIFT Nifty: {gap_data['gift_price']:.2f}")
            print(f"NSE Nifty: {gap_data['nifty_close']:.2f}")
            print(f"Gap: {gap_data['gap_points']:.2f} pts ({gap_data['gap_percent']:+.2f}%)")
            print(f"\n📊 {gap_data['signal']}")
        else:
            print("Unable to fetch index data")
