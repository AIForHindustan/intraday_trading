
# file: diagnose_validator.py
import os, json, asyncio
from datetime import datetime, timezone
from pathlib import Path

import importlib.util

# Import original validator
spec = importlib.util.spec_from_file_location("alert_validator", "/mnt/data/alert_validator.py")
av = importlib.util.module_from_spec(spec)
spec.loader.exec_module(av)

# Patches have been integrated into alert_validator.py - no longer needed

# Instantiate
validator = av.AlertValidator(config_path=None)

# Force bucket metrics path via config flag (in-memory tweak)
validator.config.setdefault("validation", {})["force_bucket_metrics"] = True

# Construct a fake alert (symbol must exist in your Redis for full run; here we just call methods to trigger code paths)
alert = {
    "alert_id": "TEST-1",
    "symbol": "NIFTY",
    "pattern": "Breakout",
    "alert_type": "Breakout",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "confidence": 0.9,
    "last_price": 100.0,
    "side": "LONG",
}

# Run core validation (should log bucket metrics attempts)
res = validator.process_alert(alert)
print(json.dumps(res, indent=2, default=str)[:1000])
