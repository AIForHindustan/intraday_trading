Zerodha Market Quotes — Full Mode Attributes

This document lists Zerodha Kite Connect Market Quote response attributes for FULL mode and cross‑checks them against our crawler implementation and unified schema.

Full Mode Attributes
- instrument_token (uint32) — Exchange-issued instrument ID
- timestamp (string) — Exchange timestamp of the quote packet
- last_trade_time (null | string) — Last trade timestamp
- last_price (float64) — Last traded market price
- last_quantity (int64) — Last traded quantity
- volume (int64) — Volume traded today
- average_price (float64) — VWAP for the day
- buy_quantity (int64) — Total pending buy quantity
- sell_quantity (int64) — Total pending sell quantity
- net_change (float64) — Absolute change from previous close to LTP
- lower_circuit_limit (float64) — Current lower circuit
- upper_circuit_limit (float64) — Current upper circuit
- oi (float64) — Open interest (F&O)
- oi_day_high (float64) — Highest OI today
- oi_day_low (float64) — Lowest OI today
- ohlc.open (float64) — Opening price
- ohlc.high (float64) — Day high
- ohlc.low (float64) — Day low
- ohlc.close (float64) — Previous close
- depth.buy[].price (float64) — Bid level price
- depth.buy[].orders (int64) — Bid level order count
- depth.buy[].quantity (int64) — Bid level quantity
- depth.sell[].price (float64) — Ask level price
- depth.sell[].orders (int64) — Ask level order count
- depth.sell[].quantity (int64) — Ask level quantity

Notes
- Some docs also reference open_interest for OI; in Kite WS, oi is standard. Keep an alias for safety.
- OHLC and depth are nested objects; we store OHLC as a dict and pass through depth levels as provided.

Cross‑Check Against Our Crawler
- Source files referenced: config/unified_schema.py, config/total_market_data_crawler.py, config/field_map.yaml, ENHANCED_TOKEN_MAPPING_IMPLEMENTATION.md

Coverage Summary (Present = captured/mapped; Alias = available via alternate field)
- instrument_token — Present (unified_schema: instrument_token)
- timestamp — Present as exchange_timestamp (our timestamp is local receive time)
- last_trade_time — Present
- last_price — Present
- last_quantity — Present (also supports alias last_traded_quantity)
- volume — Present (also supports alias volume_traded; falls back from last_quantity when absent)
- average_price — Present (also supports alias average_traded_price)
- buy_quantity — Present (also supports alias total_buy_quantity)
- sell_quantity — Present (also supports alias total_sell_quantity)
- net_change — Present
- lower_circuit_limit — Present
- upper_circuit_limit — Present
- oi — Present
- oi_day_high — Present
- oi_day_low — Present
- ohlc.open/high/low/close — Present (stored as ohlc dict)
- depth.buy/sell[].price/orders/quantity — Present (pass‑through). We also compute totals and spreads.

Observed Differences / Potential Gaps
- Addressed: Added alias mapping open_interest → oi in config/unified_schema.py and config/field_map.yaml.
- timestamp semantics: API’s timestamp is the exchange timestamp. Our schema keeps local receive time in timestamp and stores the API’s exchange time in exchange_timestamp. This is intentional but worth noting.
- Types: Our Parquet schema stores OI as int64 (counts) even though docs list float64. This is acceptable and preferable for OI counts.

Actionable Follow‑ups (if needed)
- No additional missing fields detected vs. FULL mode docs.
