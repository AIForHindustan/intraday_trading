┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Zerodha API    │───▶│ crawlers/gift_nifty_gap.py│───▶│     Redis       │
│                 │    │  (30s updates)  │    │                 │
│ • GIFT Nifty    │    │                 │    │ • Publish to     │
│ • NSE Nifty     │    │ • Calculate gap │    │   'market.gift_ │
└─────────────────┘    │ • Generate      │    │   nifty.gap'    │
                       │   signals       │    │ • Set key       │
                       └─────────────────┘    │   'latest_gift_ │
                                               │   nifty_gap'   │
┌─────────────────┐    ┌─────────────────┐    └─────────────────┘
│  JSON Files     │◀──▶│  File System    │           │
│                 │    │                 │           │
│ • gift_nifty_   │    │ • scanner/data/ │           │
│   gap_YYYYMMDD  │    │   market_micro- │           │
│   .json         │    │   structure/    │           │
│                 │    │   gap_analysis  │           │
└─────────────────┘    └─────────────────┘           ▼
                                                     │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Main Scanner    │◀──▶│ Redis Consumer  │◀──▶│ Alert Manager   │
│ (main.py)       │    │                 │    │                 │
│                 │    │ • Subscribes to │    │ • Could use for │
│ • Logs updates  │    │   channel       │    │   thesis        │
│ • Real-time     │    │ • Real-time     │    │   patterns      │
│   monitoring    │    │   processing    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘

┌─────────────────┐    ┌─────────────────┐
│ Monitor Tool    │◀──▶│ Redis Consumer  │
│ (monitor_gap_   │    │                 │
│  redis.py)      │    │ • Dedicated     │
│                 │    │   subscriber    │
│ • Console       │    │ • Real-time     │
│   display       │    │   display       │
└─────────────────┘    └─────────────────┘