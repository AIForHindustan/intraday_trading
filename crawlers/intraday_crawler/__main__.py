#!/usr/bin/env python3
"""
Entry point for intraday_crawler module.
Allows running: python crawlers/intraday_crawler
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and run the main function from the actual crawler
from crawlers.zerodha_websocket.intraday_crawler import main

if __name__ == "__main__":
    main()

