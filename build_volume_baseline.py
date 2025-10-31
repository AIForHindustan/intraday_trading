"""
Volume Baseline Recovery Script
Run this to rebuild your 10-day volume baseline
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from your_data_source import HistoricalDataAPI  # Replace with your data source

class VolumeBaselineRecovery:
    def __init__(self, redis_storage: RedisStorage):
        self.redis = redis_storage
        self.historical_api = HistoricalDataAPI()
    
    async def rebuild_volume_baseline(self, symbol: str, days_back: int = 10):
        """Rebuild volume baseline from historical data"""
        print(f"Rebuilding volume baseline for {symbol} for {days_back} days...")
        
        end_date = datetime.now()
        volume_data = {}
        
        for i in range(days_back):
            current_date = end_date - timedelta(days=i)
            if await self.is_trading_day(current_date):
                # Fetch historical data for this day
                day_data = await self.historical_api.get_daily_data(
                    symbol, 
                    current_date
                )
                
                if day_data:
                    # Process volume by market hours
                    hourly_volumes = self.analyze_volume_by_hour(day_data)
                    volume_data[current_date.strftime('%Y-%m-%d')] = hourly_volumes
                    print(f"Processed {current_date.strftime('%Y-%m-%d')}")
        
        # Store the rebuilt baseline
        await self.store_volume_baseline(symbol, volume_data)
        print(f"Volume baseline rebuilt for {symbol}")
    
    def analyze_volume_by_hour(self, day_data: pd.DataFrame) -> dict:
        """Analyze volume patterns by market hour"""
        hourly_volumes = {}
        
        for hour in range(9, 16):  # Market hours 9 AM to 3 PM
            hour_data = day_data.between_time(f"{hour:02d}:00", f"{hour:02d}:59")
            if not hour_data.empty:
                hourly_volumes[hour] = {
                    'total_volume': hour_data['volume'].sum(),
                    'avg_volume': hour_data['volume'].mean(),
                    'max_volume': hour_data['volume'].max(),
                    'min_volume': hour_data['volume'].min()
                }
        
        return hourly_volumes
    
    async def store_volume_baseline(self, symbol: str, volume_data: dict):
        """Store the rebuilt volume baseline"""
        baseline_key = f"volume_baseline:{symbol}"
        
        baseline_summary = {}
        for date, hourly_data in volume_data.items():
            baseline_summary[date] = hourly_data
        
        await self.redis.redis.hset(
            baseline_key, 
            mapping={'data': json.dumps(baseline_summary)}
        )
    
    async def is_trading_day(self, date: datetime) -> bool:
        """Check if the given date was a trading day"""
        # Implement your trading day logic here
        # Skip weekends and holidays
        if date.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Add holiday checking logic
        holidays = await self.get_market_holidays()
        if date.strftime('%Y-%m-%d') in holidays:
            return False
        
        return True
    
    async def get_market_holidays(self):
        """Get list of market holidays"""
        # Implement holiday checking
        return []  # Return actual holidays

# Usage
async def main():
    redis_storage = RedisStorage(your_redis_client)
    recovery = VolumeBaselineRecovery(redis_storage)
    
    symbols = ["RELIANCE", "TCS", "INFY"]  # Your symbols
    for symbol in symbols:
        await recovery.rebuild_volume_baseline(symbol, days_back=10)

# Run the recovery
# asyncio.run(main())