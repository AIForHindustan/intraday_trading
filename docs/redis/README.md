# Redis Operations Control Plane

Central reference for every Redis touchpoint in the intraday trading stack. Use
this guide when onboarding modules, auditing key health, or reconciling Redis
state with detector behaviour.

## Quick Start
- **Primary client**: `core.data.redis_client.RobustRedisClient` (singleton via
  `get_redis_client()`).
- **Configuration**: `config/redis_config.py` defines the canonical host, DB
  segmentation, TTLs, and feature flags (client tracking / RedisJSON).
- **Health check**: `python core/data/redis_health.py` pings Redis, reports key
  counts, and inspects TTLs for buckets, patterns, and alerts.
- **Key standards**: Detailed naming conventions live in
  `docs/REDIS_KEY_STANDARDS.md`; update both files together.

## Database Map

| DB | Name                | Primary payloads                                  | TTL (s) | Core writers                                           |
|----|--------------------|----------------------------------------------------|---------|--------------------------------------------------------|
| 0  | system             | Session metadata, health probes, config mirrors    | 57 600  | `core.data.redis_client`, `alert_validation.alert_validator` |
| 2  | prices             | Equity/F&O/commodity last-price hashes             | 57 600  | Market data ingesters, `core.data.redis_storage`       |
| 3  | premarket          | Opening prints, incremental volume pre-open        | 57 600  | Pre-market crawlers                                    |
| 4  | continuous_market  | Tick + indicator streams (`ticks:*`, `patterns:*`) | 57 600  | WebSocket parser, `core.data.redis_storage`            |
| 5  | cumulative_volume  | Baselines, `volume:baseline:*`, bucket cumulatives | 57 600  | `core.data.volume_state_manager`, volume baseline jobs |
| 6  | alerts             | Alert queue + metadata (`alerts:*`, `alert:*`)      | 57 600  | `alert_validation.alert_streams`, scanners             |
| 8  | microstructure     | Depth snapshots, flow analytics                    | 57 600  | Microstructure analyzers                               |
| 10 | patterns           | Detector cache, math dispatcher diagnostics        | 57 600  | `patterns.pattern_detector`, ICT detectors             |
| 11 | news               | News payloads, sentiment, boosts                   | 57 600  | News ingestion pipeline                                |
| 13 | metrics            | Performance metrics, analytics caches              | 57 600  | Reporting scripts (`quality_dashboard.py`, monitors)   |

> **Tip**: Use `config.redis_config.get_database_for_data_type("pattern_alerts")`
> to resolve DB numbers programmatically instead of hardcoding.

## Core Client APIs
- `RobustRedisClient`: Thread-safe wrapper with circuit breaker, retry queue,
  and JSON-safe encoder (`core/data/redis_client.py`). Exposes:
  - `get_client(db_num)` for raw redis-py access.
  - `store_by_data_type`, `retrieve_by_data_type` to honor database mappings.
  - Stream helpers such as `publish_ticks`, `_write_pattern_detection`.
- `VolumeStateManager` (`core/data/volume_state_manager.py`): Maintains
  time-bucketed volume state and mirrors canonical session fields into Redis
  hashes with 24h expiry.
- `AlertStream` (`alert_validation/alert_streams.py`): Publishes pending alerts
  (`alerts:pending:validation`) and validation results
  (`alerts:validation:results`), while caching metadata under
  `alert:metadata:{alert_id}`.
- `OptimizedRedisCalculations` (`utils/redis_calculations.py`): Loads and invokes
  `core/data/redis_calculations.lua` via `SCRIPT LOAD`/`EVALSHA` for ATR, VWAP,
  and other indicator math.

## Key Families & Usage
- **Ticks & Indicators** — `ticks:{symbol}` (XADD stream), `indicator:{name}:{symbol}`
  hashes. Writers: WebSocket ingesters, `redis_storage.store_tick`.
- **Volume Baselines** — `volume:baseline:{symbol}:{HH}:{MM}` and
  `volume:bucket:{symbol}:{session}:{HH}:{MM}`. Writers: volume baseline loader,
  `VolumeStateManager`.
- **Patterns & Math** — `patterns:{pattern_type}:{symbol}`, dispatcher caches
  under `pattern_cache:*`, and ICT flags in `ict:*`. Writers: `PatternDetector`,
  `patterns/ict/pattern_detector.py`.
- **Alerts & Validation** — `alerts:pending:*`, `alerts:validation:*`,
  `alert_performance:stats`, rolling window snapshots in `alert:rolling_windows`.
  Writers/consumers: scanners, `AlertValidator`, human dashboards.
- **News Boost** — `news:latest:{symbol}` and sentiment aggregates housed in DB
  11; consumers read via `math_dispatcher.calculate_news_boost`.
- **Performance Metrics** — `metrics:*` for quality dashboards, stored by
  reporting scripts to feed daily audit artefacts.

Consult `docs/REDIS_KEY_STANDARDS.md` for the exhaustive key grammar and parsing
helpers.

## Writers & Consumers At a Glance
- **Ingestion** — `core/data/redis_storage.py`, `crawlers/mini_option_poller.py`,
  and `scanner_main.py` push normalized ticks, indicators, and pattern events.
- **Mathematical Engine** — `core/math_dispatcher.py` orchestrates confidence,
  position size, stop-loss, and news boosts. Results are cached in DB 10 to keep
  detectors stateless between ticks.
- **Risk & Alerts** — `alerts/risk_manager.py` consumes dispatcher outputs and
  persists validation artefacts; `alert_validation/alert_validator.py` reads
  pending alerts, schedules forward validation, and writes reports after market
  close.
- **Analytics** — `quality_dashboard.py`, `quality_monitor.py`, and reporting
  notebooks read metrics DBs for dashboards and health signals.

## Validation & Monitoring
- `python core/data/redis_health.py --host localhost --port 6379` — connectivity,
  TTL, and high-level key counts.
- `redis-cli -n 4 XINFO STREAM ticks:SYMBOL` — confirm stream length and pending
  consumer groups.
- `redis-cli -n 6 XLEN alerts:pending:validation` — ensure alert backlog is
  draining during market hours.
- `redis-cli -n 5 HGET volume:baseline:BANKNIFTY:09:30 average` — inspect volume
  baseline values for calibration checks.

Log findings in `audit.md` and update docstrings (see README audit playbook)
whenever Redis touchpoints change.

## Operational Notes
- Respect TTLs defined in `config/redis_config.REDIS_DATABASES`; any module that
  introduces long-lived keys must register them there before deployment.
- When introducing new key prefixes, add them to both this README and
  `docs/REDIS_KEY_STANDARDS.md`, and update associated docstrings so automated
  audits detect drift.
- Lua scripts (`core/data/redis_calculations.lua`) should be versioned with
  semantic comments; reload via `OptimizedRedisCalculations` after changes.
