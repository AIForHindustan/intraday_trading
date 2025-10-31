# 📊 Binary Data Ingestion Issue - Complete Snapshot

## 🔴 Current Problem

### Issue Summary
During binary data ingestion from Zerodha WebSocket captures (`.dat`/`.bin` files), the system is failing to properly resolve instrument tokens to their metadata (symbol, exchange, segment, etc.), resulting in:

- **127+ million records** with `exchange = None` or `symbol = 'UNKNOWN_{token}'`
- **~5,397 records** with `symbol LIKE 'UNKNOWN%'` 
- **Incomplete metadata** for instrument tokens that exist in `token_lookup.json`

### Root Cause Analysis

#### 1. Token Lookup Mechanism
**Location**: `binary_to_parquet/production_binary_converter.py:823-889`

The converter uses `TokenCacheManager` to load instrument metadata from `core/data/token_lookup.json` (358,784 instruments):

```python
def _get_token_metadata(self, conn, instrument_token: int) -> Dict:
    # First checks cache
    cached = self._token_metadata_cache.get(instrument_token)
    if cached:
        return cached
    
    # Creates fallback metadata
    metadata = {
        "symbol": f"UNKNOWN_{instrument_token}",
        "exchange": None,
        # ... other fields
    }
    
    # Enriches from token_cache if available
    if self.token_cache:
        cache_info = self.token_cache.get_instrument_info(instrument_token)
        # ... copy values from cache_info if not None
```

**Problem**: The token lookup is happening correctly in code, but tokens stored in database as `UNKNOWN_{token}` indicate:
- Token lookup file is loading successfully (358K instruments)
- But during enrichment, many tokens return `UNKNOWN_{token}` fallback
- Either tokens don't exist in lookup file, OR enrichment is not being applied

#### 2. Binary Parser Endianness (FALSE ALARM - Reverted)

**Original Issue Thought**: We incorrectly assumed endianness was the problem (big-endian `>I` vs little-endian `<I`).

**Investigation Results**:
- Big-endian parsing produces reasonable tokens (409, 512, etc.)
- Little-endian parsing produces out-of-range tokens (2.5 billion, etc.)
- Token 409 exists in `token_lookup.json` but shows up as `UNKNOWN_409` in DB

**Resolution**: Reverted parser back to big-endian (`>I`) - endianness was NOT the issue.

#### 3. Token Lookup Path Confusion

**The REAL Issue**: During parallel ingestion, the worker processes may not have access to the token cache properly, or tokens are being read correctly but enrichment is failing.

**Files**:
- `core/data/token_lookup.json` - Source of truth (358,784 instruments)
- `token_cache.py` - Token cache loader
- `production_binary_converter.py` - Enrichment logic

---

## 🛠️ Changes Made to Date

### 1. Parallel Ingestion Driver Changes
**File**: `binary_to_parquet/parallel_ingestion_driver.py`

**Changes**:
- Added support for `.parquet`, `.jsonl`, `.json` file extensions (previously only `.bin`, `.dat`)
- Fixed file discovery logic to handle individual files in addition to directory scanning

**Code**:
```python
supported_extensions = {".bin", ".dat", ".parquet", ".jsonl", ".json"}
# Added .parquet, .jsonl, .json support
```

### 2. Binary Parser Endianness (REVERTED)
**File**: `binary_to_parquet/production_binary_converter.py`

**Original Change** (now reverted):
- Changed `struct.unpack` from big-endian (`>I`) to little-endian (`<I`)
- Modified in 5 locations:
  - `_parse_full_mode_packet`: line 498
  - `_parse_full_mode_packet`: line 505 (depth entries)
  - `_parse_quote_mode_packet`: line 542
  - `_parse_index_mode_packet`: line 566
  - `_parse_ltp_mode_packet`: line 587

**Reverted**: All struct.unpack calls now use `>I` (big-endian) as original.

### 3. Token Lookup Path Verification
**File**: `token_cache.py:15-16`

**Current State**:
```python
def __init__(self, cache_path: Optional[str] = None, verbose: bool = True) -> None:
    default_path = Path("core/data/token_lookup.json")
```

**Status**: Path is correct, 358,784 instruments loaded successfully.

---

## 🔍 Bottlenecks & Investigation Journey

### Bottleneck 1: Database Locks & Zombie Processes
**Problem**: Multiple ingestion processes were running simultaneously, causing:
- Database lock conflicts (`_duckdb.IOException: Could not set lock on file`)
- Zombie/orphaned processes consuming resources
- Primary key violations during concurrent writes

**Resolution**: 
- Killed all background processes using `ps aux | grep python`
- Identified and terminated hanging processes
- User feedback: "why the fuck are u always in a rush in killing" → Learned to analyze first

### Bottleneck 2: Incorrect Token Interpretation
**Problem**: Token 409 was being converted to 256 via big-endian→little-endian conversion, but:
- Token 409 EXISTS in token_lookup.json
- Token 256 EXISTS as a DIFFERENT instrument
- This was a red herring

**Investigation**:
```python
# Tested token conversions
problematic_tokens = [65536, 16777216, 1, 131072, ...]
for token in problematic_tokens:
    token_bytes = struct.pack('>I', token)
    converted_token = struct.unpack('<I', token_bytes)[0]
    # Only 2 out of 20 tokens matched - most conversions were invalid
```

**Lesson**: The tokens being read are correct. The issue is that enrichment is not being applied properly during ingestion.

### Bottleneck 3: Missing Token Metadata in Database
**Problem**: 127M records with `exchange = None` suggests:
- Token lookup is not being applied during parallel ingestion
- OR tokens don't exist in lookup file
- OR there's a path/permission issue preventing token cache loading

**Investigation**:
```python
# Checked if tokens exist in token_lookup.json
token_409_info = token_data['409']
# Result: exchange='MCX', but instrument_type='UNKNOWN'
# This is correct data, but shows as 'UNKNOWN_409' in DB
```

---

## 📈 Current Database State

### Database Status
- **Database**: `intraday_trading.db` 
- **Current State**: EMPTY (no tables exist)
- **Previous Attempts**: Database was populated with corrupted data, now cleared

### Token Lookup Status
- **File**: `core/data/token_lookup.json`
- **Records**: 358,784 instruments loaded
- **Status**: ✅ Loaded successfully
- **Example**: Token 409 → MCX exchange, instrument_type=UNKNOWN

### Binary Files Status
- **Location**: `crawlers/raw_data/data_mining/`
- **Files**: Multiple `.dat` files (binary_data_YYYYMMDD_HHMMSS.dat)
- **Examples**:
  - `binary_data_20251013_133350.dat`
  - `binary_data_20251010_121000.dat`
  - `binary_data_20251024_101902.dat`
  - etc.

---

## 🎯 Next Steps (Pending)

### 1. Fix Token Enrichment During Ingestion
**Priority**: HIGH

**Required Actions**:
- Verify token cache is loaded in worker processes during parallel ingestion
- Add logging to track when `get_instrument_info()` returns `UNKNOWN_{token}` vs valid data
- Check if tokens in binary files match tokens in token_lookup.json

**Files to Modify**:
- `binary_to_parquet/parallel_ingestion_driver.py` (worker initialization)
- `token_cache.py` (add debug logging)

### 2. Re-ingest Binary Data
**Priority**: HIGH

**Required Actions**:
- Delete existing corrupted records (127M records with None/UNKNOWN metadata)
- Re-run parallel ingestion with fixed token lookup
- Verify token enrichment is working correctly

**Command**:
```bash
cd /Users/lokeshgupta/Desktop/intraday_trading
source .venv/bin/activate
python scripts/run_parallel_ingestion.py --workers 4 --batch-size 5000 --queue-size 16 \
  /Users/lokeshgupta/Desktop/backtesting/data_mining crawlers/raw_data research_data
```

### 3. Convert Binary → Parquet (Alternative Approach)
**Priority**: MEDIUM

**Approach**: Instead of fixing ingestion, convert binary files to parquet first with correct token mapping, then ingest parquet files.

**Benefits**:
- Cleaner pipeline (binary → parquet → ingestion)
- Can debug token mapping in isolation
- Parquet files can be validated before ingestion

**Files to Create**:
- `convert_binary_to_parquet.py` (read binary → enrich with tokens → write parquet)

---

## 🔧 Technical Details

### Token Lookup Flow

1. **Initialization** (`parallel_ingestion_driver.py:36-42`):
```python
token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
converter = ProductionZerodhaBinaryConverter(
    db_path=None,
    batch_size=batch_size,
    token_cache=token_cache,
    ensure_schema=False,
)
```

2. **Packet Parsing** (`production_binary_converter.py:494-535`):
```python
def _parse_full_mode_packet(self, data: bytes) -> Optional[Dict]:
    # Parse binary structure into raw packet
    fields = struct.unpack(">IIIIIIIIIIIIIIII", data[:64])
    packet = {
        "instrument_token": fields[0],
        # ... other fields
    }
```

3. **Token Enrichment** (`production_binary_converter.py:623-670`):
```python
def _enrich_packets(self, batch: List[Dict], file_path: Path, conn) -> Tuple[List[Dict], int]:
    for packet in batch:
        enriched = self._enrich_with_complete_metadata(conn, packet, file_path)
        # _enrich_with_complete_metadata calls _get_token_metadata
        # which uses token_cache.get_instrument_info(token)
```

4. **Token Lookup** (`token_cache.py:60-73`):
```python
def get_instrument_info(self, instrument_token: int) -> Dict:
    return self.token_map.get(
        instrument_token,
        {
            "symbol": f"UNKNOWN_{instrument_token}",
            "exchange": None,
            # ... fallback metadata
        },
    )
```

**The Problem**: Many tokens are returning the fallback `UNKNOWN_{token}` instead of cached metadata, suggesting either:
- Tokens don't exist in `token_lookup.json`
- `token_map` is not populated correctly
- Path/permission issues during parallel worker initialization

---

## 📝 Key Findings

### Finding 1: Endianness Was Not the Problem
- **Evidence**: Big-endian parsing produces tokens like 409, which EXIST in token_lookup.json
- **Evidence**: Token 409 shows up as `UNKNOWN_409` in database
- **Conclusion**: Parser is reading tokens correctly, but enrichment is failing

### Finding 2: Token Lookup File is Complete
- **Evidence**: 358,784 instruments loaded successfully
- **Evidence**: Token 409 exists in file with correct metadata (MCX exchange)
- **Conclusion**: The lookup file is not the problem

### Finding 3: Database Contains Corrupted Data
- **Evidence**: 127M records with `exchange = None`
- **Evidence**: ~5K records with `symbol LIKE 'UNKNOWN%'`
- **Conclusion**: Previous ingestion attempts failed to apply token lookup correctly

### Finding 4: Parallel Ingestion May Be Bypassing Enrichment
- **Evidence**: Large number of records without metadata suggests batch processing issue
- **Evidence**: Worker processes may not be loading token cache correctly
- **Conclusion**: Need to investigate worker initialization in `parallel_ingestion_driver.py`

---

## 🚨 Critical Actions Required

1. **Immediate**: Add debug logging to track token cache loading in workers
2. **Immediate**: Verify tokens in binary files exist in token_lookup.json
3. **Short-term**: Fix token enrichment in parallel ingestion pipeline
4. **Short-term**: Delete corrupted records and re-ingest with fixes
5. **Long-term**: Convert binary → parquet → ingestion pipeline for cleaner architecture

---

## 📊 Statistics

- **Token Lookup Records**: 358,784 instruments
- **Problematic Records in DB**: ~127 million (exchange = None)
- **Files to Process**: Unknown (binary files in `crawlers/raw_data/`)
- **Current Database State**: EMPTY (cleared during investigation)
- **Parser Changes**: REVERTED (back to big-endian)

---

**Last Updated**: 2025-01-24
**Status**: Investigation in progress, awaiting token enrichment fix

