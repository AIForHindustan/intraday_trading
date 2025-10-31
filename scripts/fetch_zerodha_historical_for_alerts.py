#!/usr/bin/env python3
"""
Fetch Historical Candle Data from Zerodha for Alert Analysis

Fetches minute-level historical data from Zerodha API for instruments
mentioned in high-confidence alerts to enable success/failure tracking.

Usage:
    python scripts/fetch_zerodha_historical_for_alerts.py
"""

import sys
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.zerodha_config import ZerodhaConfig
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ZerodhaHistoricalFetcher:
    """Fetch historical candle data from Zerodha API"""
    
    def __init__(self):
        """Initialize Zerodha Kite API client"""
        try:
            self.kite = ZerodhaConfig.get_kite_instance()
            logger.info("✅ Zerodha Kite API initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Zerodha API: {e}")
            raise
    
    def get_instrument_token(self, symbol: str) -> Optional[int]:
        """Get instrument token for a symbol"""
        try:
            # Query Zerodha instruments
            # Normalize symbol (remove NSE:/NFO: prefix if present)
            clean_symbol = symbol.replace('NSE:', '').replace('NFO:', '').strip()
            
            # Try NSE first
            instruments = self.kite.instruments("NSE")
            for inst in instruments:
                if inst['tradingsymbol'] == clean_symbol or inst['tradingsymbol'] == symbol:
                    return inst['instrument_token']
            
            # Try NFO for F&O
            instruments = self.kite.instruments("NFO")
            for inst in instruments:
                if inst['tradingsymbol'] == clean_symbol or inst['tradingsymbol'] == symbol:
                    return inst['instrument_token']
            
            logger.warning(f"⚠️ Instrument token not found for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting token for {symbol}: {e}")
            return None
    
    def fetch_minute_candles(self, instrument_token: int, from_date: datetime, to_date: datetime) -> List[Dict]:
        """Fetch minute-level historical candles from Zerodha"""
        try:
            # Zerodha API limits: max 2000 candles per request
            # For minute candles, that's about 1.4 days
            # Split into chunks if needed
            
            candles = []
            current_date = from_date
            
            while current_date < to_date:
                chunk_end = min(current_date + timedelta(hours=35), to_date)  # ~2000 minutes
                
                try:
                    chunk_candles = self.kite.historical_data(
                        instrument_token=instrument_token,
                        from_date=current_date,
                        to_date=chunk_end,
                        interval="minute"
                    )
                    
                    if chunk_candles:
                        candles.extend(chunk_candles)
                        logger.debug(f"   Fetched {len(chunk_candles)} candles for period {current_date} to {chunk_end}")
                    
                    # Rate limiting: Zerodha has 3 requests per second limit
                    time.sleep(0.35)
                    
                except Exception as e:
                    logger.error(f"❌ Error fetching candles for {instrument_token}: {e}")
                    break
                
                current_date = chunk_end + timedelta(minutes=1)
            
            logger.info(f"✅ Fetched {len(candles)} total candles for token {instrument_token}")
            return candles
            
        except Exception as e:
            logger.error(f"❌ Error fetching historical data: {e}")
            return []
    
    def fetch_for_alerts(self, alerts: List[Dict]) -> Dict[str, List[Dict]]:
        """Fetch historical data for all alert instruments"""
        historical_data = {}
        unique_symbols = set()
        
        # Extract unique symbols from alerts
        for alert in alerts:
            symbol = alert.get('symbol', '')
            if symbol and symbol not in unique_symbols:
                unique_symbols.add(symbol)
        
        logger.info(f"📊 Fetching historical data for {len(unique_symbols)} unique instruments...")
        
        for symbol in unique_symbols:
            try:
                # Get instrument token
                token = self.get_instrument_token(symbol)
                if not token:
                    logger.warning(f"⚠️ Skipping {symbol} - token not found")
                    continue
                
                # Calculate date range: from alert timestamp to 1 day after
                # Get earliest alert time for this symbol
                symbol_alerts = [a for a in alerts if a.get('symbol') == symbol]
                if not symbol_alerts:
                    continue
                
                # Parse alert timestamp
                earliest_alert = min(symbol_alerts, key=lambda x: x.get('timestamp', x.get('published_at', datetime.now().isoformat())))
                alert_time_str = earliest_alert.get('timestamp', earliest_alert.get('published_at', datetime.now().isoformat()))
                
                try:
                    if isinstance(alert_time_str, str):
                        alert_time = datetime.fromisoformat(alert_time_str.replace('Z', '+00:00'))
                    else:
                        alert_time = alert_time_str
                    
                    # Convert to IST if needed
                    if alert_time.tzinfo:
                        import pytz
                        ist_tz = pytz.timezone("Asia/Kolkata")
                        alert_time = alert_time.astimezone(ist_tz)
                    else:
                        import pytz
                        ist_tz = pytz.timezone("Asia/Kolkata")
                        alert_time = ist_tz.localize(alert_time)
                    
                    # Fetch data from 1 hour before alert to 1 day after
                    from_date = alert_time - timedelta(hours=1)
                    to_date = alert_time + timedelta(days=1)
                    
                    # Fetch candles
                    candles = self.fetch_minute_candles(token, from_date, to_date)
                    
                    if candles:
                        historical_data[symbol] = candles
                        logger.info(f"✅ {symbol}: {len(candles)} candles fetched")
                    else:
                        logger.warning(f"⚠️ {symbol}: No candles fetched")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {symbol}: {e}")
                    continue
                
                # Rate limiting between symbols
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error fetching data for {symbol}: {e}")
                continue
        
        logger.info(f"✅ Fetched historical data for {len(historical_data)} instruments")
        return historical_data
    
    def save_historical_data(self, historical_data: Dict[str, List[Dict]], output_file: str = None):
        """Save historical data to JSON file"""
        if output_file is None:
            timestamp = int(time.time())
            output_file = f"zerodha_historical_alerts_{timestamp}.json"
        
        # Convert datetime objects to ISO strings for JSON serialization
        serializable_data = {}
        for symbol, candles in historical_data.items():
            serializable_data[symbol] = []
            for candle in candles:
                serializable_candle = {}
                for key, value in candle.items():
                    if isinstance(value, datetime):
                        serializable_candle[key] = value.isoformat()
                    else:
                        serializable_candle[key] = value
                serializable_data[symbol].append(serializable_candle)
        
        with open(output_file, 'w') as f:
            json.dump(serializable_data, f, indent=2, default=str)
        
        logger.info(f"✅ Historical data saved to: {output_file}")
        return output_file


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch Zerodha historical data for alerts")
    parser.add_argument('--alerts-file', type=str, help='JSON file with alerts (from analyze_high_confidence_alerts.py)')
    parser.add_argument('--output', type=str, help='Output file for historical data')
    args = parser.parse_args()
    
    try:
        fetcher = ZerodhaHistoricalFetcher()
        
        # Load alerts if file provided, otherwise get from analysis script
        if args.alerts_file and os.path.exists(args.alerts_file):
            with open(args.alerts_file, 'r') as f:
                report = json.load(f)
                alerts = report.get('alerts', [])
        else:
            # Run analysis to get alerts
            from scripts.analyze_high_confidence_alerts import HighConfidenceAlertAnalyzer
            analyzer = HighConfidenceAlertAnalyzer()
            all_alerts = analyzer.get_all_alerts()
            top_alerts = analyzer.filter_high_confidence_alerts(all_alerts, min_confidence=0.90)
            alerts = top_alerts[:20]
        
        if not alerts:
            logger.error("❌ No alerts found")
            return
        
        logger.info(f"📊 Fetching historical data for {len(alerts)} alerts...")
        
        # Fetch historical data
        historical_data = fetcher.fetch_for_alerts(alerts)
        
        if historical_data:
            # Save to file
            output_file = fetcher.save_historical_data(historical_data, args.output)
            logger.info(f"✅ Historical data saved to {output_file}")
        else:
            logger.warning("⚠️ No historical data fetched")
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Fetch interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fetch failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
