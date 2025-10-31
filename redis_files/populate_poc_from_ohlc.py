#!/usr/bin/env python3
"""
Populate POC (Point of Control) data for all instruments from OHLC data
Calculates volume profiles from historical OHLC data stored in Redis
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from patterns.volume_profile_manager import VolumeProfileManager
from redis_files.redis_client import RobustRedisClient

def main():
    """Populate POC data for all instruments from OHLC data"""
    
    print("🚀 POPULATING POC DATA FROM OHLC DATA")
    print("=" * 60)
    
    # Initialize Redis client and VolumeProfileManager
    redis_client = RobustRedisClient()
    volume_manager = VolumeProfileManager(redis_client)
    
    # Populate POC data from OHLC data
    populated_count, failed_count = volume_manager.populate_poc_from_ohlc_data(redis_client)
    
    print(f"\n🎉 POC POPULATION COMPLETED!")
    print(f"📊 Successfully populated POC for {populated_count} instruments")
    print(f"❌ Failed: {failed_count} instruments")
    
    # Verify the results
    print(f"\n🔍 VERIFICATION:")
    underlying_client = redis_client.get_client()
    poc_keys = underlying_client.keys('volume_profile:poc:*')
    print(f"📈 Total POC keys in Redis: {len(poc_keys)}")
    
    if poc_keys:
        print(f"📋 Sample POC keys:")
        for key in poc_keys[:5]:
            print(f"   {key}")
        
        # Show sample POC data
        sample_key = poc_keys[0]
        sample_data = underlying_client.hgetall(sample_key)
        print(f"\n📊 Sample POC Data ({sample_key}):")
        for field, value in sample_data.items():
            print(f"   {field}: {value}")
    
    print(f"\n✅ POC data is now available for pattern detection and trading signals!")

if __name__ == "__main__":
    main()
