"""
Instrument Registry - FIXED VERSION
"""

import json
import os
from typing import Dict, List, Optional
from datetime import datetime

class InstrumentRegistry:
    def __init__(self):
        self._canonical_instruments: Dict[str, Dict] = {}
        self._displayable_instruments: List[Dict] = []
        self._load_canonical_instruments()

    def _load_canonical_instruments(self):
        """Load the canonical instrument catalog with FIXED file paths"""
        try:
            from pathlib import Path
            
            # Resolve project root relative to this file to avoid hardcoded paths
            backend_dir = Path(__file__).resolve().parent
            dashboard_root = backend_dir.parent
            project_root = dashboard_root.parent
            
            possible_paths = [
                dashboard_root / "zerodha_websocket" / "crawlers" / "binary_crawler1" / "binary_crawler1.json",
                project_root / "zerodha_websocket" / "crawlers" / "binary_crawler1" / "binary_crawler1.json",
                project_root / "zerodha" / "crawlers" / "binary_crawler1" / "binary_crawler1.json",
                project_root / "crawlers" / "binary_crawler1" / "binary_crawler1.json",
            ]
            
            crawler_config_path = None
            token_lookup_path = None
            
            # Find the correct crawler config path
            for path in possible_paths:
                if os.path.exists(path):
                    crawler_config_path = path
                    break
            
            if not crawler_config_path:
                print(f"❌ InstrumentRegistry: Could not find binary_crawler1.json in any path")
                # Fallback: Use environment variable or create empty list
                crawler_config_path = os.getenv('CRAWLER_CONFIG_PATH')
                if not crawler_config_path:
                    print("❌ No crawler config found, using empty instrument list")
                    return
            
            print(f"✅ Found crawler config at: {crawler_config_path}")
            
            # Load crawler config
            with open(crawler_config_path, 'r') as f:
                crawler_config = json.load(f)
            intraday_tokens = [str(t) for t in crawler_config.get('tokens', [])]
            
            # Try to use hot token mapper for metadata
            token_lookup_index: Dict[str, Dict] = {}
            mapper = None
            try:
                from crawlers.hot_token_mapper import get_hot_token_mapper
                mapper = get_hot_token_mapper()
                print("✅ Using HotTokenMapper for instrument metadata")
            except Exception as mapper_exc:
                print(f"⚠️ HotTokenMapper unavailable ({mapper_exc}), falling back to token_lookup_enriched.json")
                token_lookup_possible_paths = [
                    project_root / "intraday_trading" / "core" / "data" / "token_lookup_enriched.json",
                    dashboard_root / "intraday_trading" / "core" / "data" / "token_lookup_enriched.json",
                    Path.cwd() / "intraday_trading" / "core" / "data" / "token_lookup_enriched.json",
                ]
                for path in token_lookup_possible_paths:
                    if os.path.exists(path):
                        token_lookup_path = path
                        break
                if token_lookup_path:
                    print(f"✅ Found token lookup at: {token_lookup_path}")
                    with open(token_lookup_path, 'r') as f:
                        raw_lookup = json.load(f)
                    if isinstance(raw_lookup, dict) and 'instruments' in raw_lookup:
                        raw_instruments = raw_lookup.get('instruments', [])
                    elif isinstance(raw_lookup, list):
                        raw_instruments = raw_lookup
                    else:
                        raw_instruments = []
                    token_lookup_index = {
                        str(inst.get('instrument_token')): inst
                        for inst in raw_instruments
                        if inst.get('instrument_token') is not None
                    }
                else:
                    print(f"❌ InstrumentRegistry: Could not find token metadata files")
                    self._create_fallback_instruments()
                    return
            
            today = datetime.now().date()
            
            # Process instruments
            for token in intraday_tokens:
                metadata = None
                if mapper:
                    try:
                        metadata = mapper.get_token_metadata(int(token))
                    except Exception:
                        metadata = None
                else:
                    metadata = token_lookup_index.get(str(token))
                
                if not metadata:
                    continue

                symbol = metadata.get('tradingsymbol') or metadata.get('symbol') or metadata.get('name')
                if not symbol:
                    continue
                
                exchange = metadata.get('exchange', 'NSE')
                instrument_code = metadata.get('key') or f"{exchange}:{symbol}"

                expiry_str = metadata.get('expiry')
                if expiry_str:
                    try:
                        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
                        if expiry_date < today:
                            continue  # Skip expired instruments
                    except (ValueError, TypeError):
                        pass

                self._canonical_instruments[instrument_code] = {
                    "instrument_code": instrument_code,
                    "zerodha_token": token,
                    "display_symbol": symbol,
                    "name": metadata.get('name', symbol),
                    "exchange": exchange,
                    "instrument_type": metadata.get('instrument_type'),
                    "lot_size": metadata.get('lot_size', 1),
                    "expiry": expiry_str,
                }
            
            # Create display list
            self._displayable_instruments = [
                {"code": code, "label": data["display_symbol"]}
                for code, data in self._canonical_instruments.items()
            ]
            
            print(f"✅ InstrumentRegistry: Loaded {len(self._canonical_instruments)} instruments")
            
            # Debug: Print first few instruments
            for i, inst in enumerate(self._displayable_instruments[:5]):
                print(f"   {i+1}. {inst['label']} -> {inst['code']}")

        except Exception as e:
            print(f"❌ InstrumentRegistry: Failed to load instruments: {e}")
            import traceback
            traceback.print_exc()
            # Fallback to empty list
            self._create_fallback_instruments()

    def _create_fallback_instruments(self):
        """Create fallback instruments for testing when files aren't found"""
        print("🔄 Creating fallback instruments for testing...")
        
        fallback_instruments = [
            {"code": "NFO:NIFTY25DEC26400CE", "label": "NIFTY25DEC26400CE", "exchange": "NFO", "lot_size": 50},
            {"code": "NFO:BANKNIFTY25DEC55400PE", "label": "BANKNIFTY25DEC55400PE", "exchange": "NFO", "lot_size": 25},
            {"code": "NSE:RELIANCE", "label": "RELIANCE", "exchange": "NSE", "lot_size": 1},
            {"code": "NSE:TATAMOTORS", "label": "TATAMOTORS", "exchange": "NSE", "lot_size": 1},
        ]
        
        for inst in fallback_instruments:
            self._canonical_instruments[inst["code"]] = {
                "instrument_code": inst["code"],
                "zerodha_token": "FALLBACK",  # Placeholder
                "display_symbol": inst["label"],
                "exchange": inst["exchange"],
                "lot_size": inst["lot_size"],
            }
        
        self._displayable_instruments = fallback_instruments
        print(f"✅ Created {len(fallback_instruments)} fallback instruments")

    def list_displayable_instruments(self) -> List[Dict]:
        """Returns instruments for UI dropdown"""
        return self._displayable_instruments

    def resolve_for_broker(self, instrument_code: str, broker: str) -> Optional[Dict]:
        """Resolve instrument for specific broker"""
        canonical = self._canonical_instruments.get(instrument_code)
        if not canonical:
            return None

        if broker.upper() == "ZERODHA":
            return {
                "tradingsymbol": canonical["display_symbol"],
                "exchange": canonical["exchange"],
                "instrument_token": canonical.get("zerodha_token", "0"),
                "lot_size": canonical.get("lot_size", 1)
            }
        elif broker.upper() == "ANGEL_ONE":
            return {
                "tradingsymbol": canonical["display_symbol"],
                "exchange": canonical["exchange"],
                "symboltoken": canonical.get("zerodha_token", "0"),  # Placeholder
                "lot_size": canonical.get("lot_size", 1)
            }
        
        return None

# Singleton instance
_registry_instance: Optional[InstrumentRegistry] = None

def get_instrument_registry() -> InstrumentRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = InstrumentRegistry()
    return _registry_instance
