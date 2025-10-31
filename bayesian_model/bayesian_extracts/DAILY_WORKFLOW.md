# Daily Bayesian Research Workflow

## Overview

The Bayesian data extractor runs **completely separately** from the main trading system. It extracts data from Redis for statistical analysis and model development without any interference with live trading operations.

## Morning Routine (Trading Days)

### 1. Start Main Trading System (8:00 AM)

```bash
./launch_crawlers.py
# Wait for system to stabilize (2-3 minutes)
```

### 2. Start Bayesian Extractor (8:15 AM)

```bash
./launch_bayesian_extractor.sh
```

The extractor will automatically:
- ✅ Check if it's a trading day (using NSE calendar)
- ✅ Verify main system is running (warning only)
- ✅ Extract data every 2 minutes during market hours
- ✅ Auto-stop at 4:00 PM
- ✅ Compress files older than 1 day
- ✅ Delete compressed files older than 30 days

### 3. Monitor Status

```bash
# Check if running
ps aux | grep bayesian_data_extractor

# View live logs
tail -f logs/bayesian_research/extractor_$(date +%Y%m%d).log

# Check extracted data
ls -lh research_data/bayesian_extracts/$(date +%Y-%m-%d)/
```

## Operating Hours

- **Pre-market**: 8:00 AM - 9:15 AM
- **Market hours**: 9:15 AM - 3:30 PM
- **Post-market**: 3:30 PM - 4:00 PM

The extractor runs continuously during these hours on trading days only.

## End of Day (4:00 PM+)

The extractor **auto-stops** at market close (4:00 PM). To manually stop earlier:

```bash
./stop_bayesian_extractor.sh
```

Or emergency stop:

```bash
pkill -9 -f bayesian_data_extractor.py
```

## Data Management

### Retention Policy

- **Raw JSON files**: Kept for 7 days
- **Compressed files** (.json.gz): Kept for 30 days
- **Monthly summaries**: Kept for 1 year
- **Auto-cleanup**: Runs during each extraction cycle

### Storage Structure

```
research_data/bayesian_extracts/
├── 2025-10-10/
│   ├── bayesian_extract_20251010_091523.json
│   ├── bayesian_extract_20251010_093523.json.gz (compressed after 1 day)
│   └── daily_summary_20251010.json
├── 2025-10-11/
│   └── ...
└── DAILY_WORKFLOW.md (this file)
```

### Manual Compression

If you need to manually compress old data:

```bash
# Compress specific day
gzip research_data/bayesian_extracts/2025-10-09/*.json

# Check disk usage
du -sh research_data/bayesian_extracts/
```

## Safety Features

### Automatic Checks

1. **Trading Day Detection**: Uses NSE holiday calendar
2. **Operating Hours**: Only runs 8:00 AM - 4:00 PM
3. **Memory Limit**: Max 1024 MB (configurable)
4. **Disk Limit**: Max 50 GB usage (configurable)
5. **Auto-shutdown**: Stops at market close

### Configuration

Edit `config/bayesian_research_config.json` to modify:

- Operating hours
- Extraction interval (default: 120 seconds)
- Memory and disk limits
- Data retention periods

## Troubleshooting

### Issue: Extractor won't start

**Possible causes:**
- Main system not running → Warning only, will start anyway if confirmed
- Redis not running → Check: `pgrep -f redis-server`
- Already running → Check: `ps aux | grep bayesian_data_extractor`
- Not a trading day → Extractor will wait automatically

### Issue: No data being extracted

**Check:**
1. Is it a trading day? → `date`
2. Are you within operating hours (8 AM - 4 PM)?
3. Is Redis populated with data?
4. Check logs: `tail -f logs/bayesian_research/extractor_*.log`

### Issue: High disk usage

**Solutions:**
1. Check current usage: `du -sh research_data/bayesian_extracts/`
2. Manually compress old files
3. Reduce retention period in config
4. Delete very old data: `rm -rf research_data/bayesian_extracts/2025-09-*`

### Issue: Memory warnings

**Solutions:**
1. Increase limit in `config/bayesian_research_config.json`
2. Restart extractor to clear memory
3. Check for memory leaks: `ps aux | grep bayesian`

## Integration with Analysis Pipeline

### Using Extracted Data

```python
import json
from pathlib import Path

# Load latest extraction
data_dir = Path("research_data/bayesian_extracts")
latest_date = sorted(data_dir.glob("20*"))[-1]
latest_file = sorted(latest_date.glob("bayesian_extract_*.json"))[-1]

with open(latest_file, 'r') as f:
    data = json.load(f)

print(f"Loaded: {latest_file}")
print(f"Volume buckets: {len(data.get('volume_buckets', {}))}")
print(f"Session data: {len(data.get('session_data', {}))}")
```

### Next Steps in Research Pipeline

1. **Feature Engineering**: `research/bayesian_features.py`
2. **Model Training**: `research/bayesian_models/`
3. **Validation**: `research/production_gateway.py`

## Best Practices

1. **Always start main system first** before Bayesian extractor
2. **Monitor logs regularly** during development
3. **Check data quality** using validator: `research/bayesian_data_validator.py`
4. **Keep extracted data organized** by date
5. **Don't modify production system** based on research until validated
6. **Back up important extracts** before cleanup
7. **Document findings** in research notebooks

## Configuration Reference

### Operating Hours (`config/bayesian_research_config.json`)

```json
{
  "operating_hours": {
    "pre_market_start": "08:00",
    "market_open": "09:15",
    "market_close": "15:30",
    "post_market_end": "16:00"
  }
}
```

### Safety Limits

```json
{
  "safety_limits": {
    "max_memory_mb": 1024,
    "max_disk_usage_gb": 50,
    "auto_stop_at_market_close": true,
    "check_main_system_running": true
  }
}
```

### Data Retention

```json
{
  "data_retention": {
    "keep_raw_days": 7,
    "compress_after_days": 1,
    "delete_compressed_after_days": 30,
    "keep_monthly_summaries_days": 365
  }
}
```

## Support

For issues or questions:
1. Check logs: `logs/bayesian_research/`
2. Verify configuration: `config/bayesian_research_config.json`
3. Test market calendar: `python core/utils/market_calendar.py`
4. Validate data: `python research/bayesian_data_validator.py`

---

**Remember**: This is a research system. All Bayesian models must be statistically validated before any integration with the production trading system.

