"""
Direct YAML Field Loader - Replaces field_mapping.py
====================================================

Direct access to optimized_field_mapping.yaml without the field_mapping.py layer.
Provides the same functionality but loads YAML directly for better performance.

Author: AION Integration Team
Date: October 25, 2025
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger(__name__)

# Global YAML config cache
_yaml_config_cache = None
_yaml_config_path = None

@lru_cache(maxsize=1)
def _load_yaml_config(config_path: str = None) -> Dict[str, Any]:
    """Load YAML configuration with caching"""
    global _yaml_config_cache, _yaml_config_path
    
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "optimized_field_mapping.yaml"
    
    config_path = Path(config_path)
    
    # Return cached config if path hasn't changed
    if _yaml_config_cache is not None and _yaml_config_path == str(config_path):
        return _yaml_config_cache
    
    try:
        if not config_path.exists():
            logger.error(f"YAML config not found: {config_path}")
            return {}
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        _yaml_config_cache = config or {}
        _yaml_config_path = str(config_path)
        
        logger.info(f"✅ Loaded YAML config: {config_path}")
        return _yaml_config_cache
        
    except Exception as e:
        logger.error(f"❌ Error loading YAML config: {e}")
        return {}

def get_yaml_config() -> Dict[str, Any]:
    """Get the YAML configuration"""
    return _load_yaml_config()

def resolve_session_field(field_name: str) -> str:
    """Resolve Redis session field name from YAML config"""
    if not field_name:
        return field_name
    
    config = get_yaml_config()
    session_fields = config.get('redis_session_fields', {})
    
    if not session_fields:
        return field_name
    
    # Direct lookup in session fields
    if field_name in session_fields:
        return session_fields[field_name]
    
    # Check if field_name is already canonical
    for alias, canonical in session_fields.items():
        if canonical == field_name:
            return canonical
    
    return field_name

def resolve_calculated_field(field_name: str) -> str:
    """Resolve calculated indicator field name from YAML config"""
    if not field_name:
        return field_name
    
    config = get_yaml_config()
    indicator_fields = config.get('calculated_indicator_fields', {})
    
    if not indicator_fields:
        return field_name
    
    # Direct lookup in indicator fields
    if field_name in indicator_fields:
        return indicator_fields[field_name]
    
    # Check if field_name is already canonical
    for alias, canonical in indicator_fields.items():
        if canonical == field_name:
            return canonical
    
    return field_name

def get_redis_session_fields() -> Dict[str, str]:
    """Get Redis session fields mapping from YAML"""
    config = get_yaml_config()
    return config.get('redis_session_fields', {})

def get_calculated_indicator_fields() -> Dict[str, str]:
    """Get calculated indicator fields mapping from YAML"""
    config = get_yaml_config()
    return config.get('calculated_indicator_fields', {})

def resolve_volume_profile_field(field_name: str) -> str:
    """Resolve volume profile field name from YAML config"""
    if not field_name:
        return field_name
    
    config = get_yaml_config()
    volume_profile_fields = config.get('volume_profile_fields', {})
    
    if not volume_profile_fields:
        return field_name
    
    # Direct lookup in volume profile fields
    if field_name in volume_profile_fields:
        return volume_profile_fields[field_name]
    
    # Check if field_name is already canonical
    for alias, canonical in volume_profile_fields.items():
        if canonical == field_name:
            return canonical
    
    return field_name

def get_volume_profile_fields() -> Dict[str, str]:
    """Get volume profile fields mapping from YAML"""
    config = get_yaml_config()
    return config.get('volume_profile_fields', {})

def get_volume_profile_patterns() -> Dict[str, str]:
    """Get volume profile patterns mapping from YAML"""
    config = get_yaml_config()
    return config.get('volume_profile_patterns', {})

def get_websocket_core_fields() -> Dict[str, str]:
    """Get WebSocket core fields mapping from YAML"""
    config = get_yaml_config()
    return config.get('websocket_core_fields', {})

def get_required_fields(asset_type: str) -> List[str]:
    """Get required fields for asset type from YAML"""
    config = get_yaml_config()
    validation_rules = config.get('field_validation', {})
    
    # Get universal required fields
    universal_required = validation_rules.get('universal_required', [])
    
    # Get asset-specific required fields
    asset_required_key = f"{asset_type}_required"
    asset_required = validation_rules.get(asset_required_key, [])
    
    return universal_required + asset_required

def get_asset_specific_fields(asset_type: str) -> List[str]:
    """Get asset-specific additional fields from YAML"""
    config = get_yaml_config()
    asset_fields = config.get('asset_websocket_fields', {}).get(asset_type, {})
    return asset_fields.get('additional_fields', [])

def get_preferred_timestamp(data: Dict[str, Any]) -> Optional[Union[str, float, int]]:
    """Get preferred timestamp from data based on YAML priority rules"""
    config = get_yaml_config()
    timestamp_priority = config.get('timestamp_priority', {})
    
    # Try primary timestamp first
    primary_field = timestamp_priority.get('primary', 'exchange_timestamp')
    if primary_field in data and data[primary_field]:
        return data[primary_field]
    
    # Try fallback timestamp
    fallback_field = timestamp_priority.get('fallback', 'timestamp_ns')
    if fallback_field in data and data[fallback_field]:
        return data[fallback_field]
    
    # Try legacy timestamp
    legacy_field = timestamp_priority.get('legacy', 'timestamp')
    if legacy_field in data and data[legacy_field]:
        return data[legacy_field]
    
    return None

def get_session_key_pattern(symbol: str, date: str = None) -> str:
    """Get standardized session key pattern from YAML"""
    config = get_yaml_config()
    
    if date is None:
        date = datetime.now().strftime("%Y%m%d")
    
    session_patterns = config.get('session_key_patterns', {})
    standard_pattern = session_patterns.get('standard', 'session:{symbol}:{date}')
    
    return standard_pattern.format(symbol=symbol, date=date)

def get_websocket_mode_fields(mode: str) -> List[str]:
    """Get fields available for specific WebSocket mode from YAML"""
    config = get_yaml_config()
    websocket_modes = config.get('websocket_modes', {})
    mode_config = websocket_modes.get(mode, {})
    return mode_config.get('fields', [])

def get_field_categories() -> Dict[str, List[str]]:
    """Get field categories for analysis from YAML"""
    config = get_yaml_config()
    return config.get('field_categories', {})

def validate_data(data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """Validate data against YAML requirements"""
    config = get_yaml_config()
    
    if not config:
        return {'valid': True, 'errors': [], 'warnings': []}
    
    errors = []
    warnings = []
    
    # Get required fields for asset type
    required_fields = get_required_fields(asset_type)
    
    # Check for missing required fields
    for field in required_fields:
        if field not in data or data[field] is None:
            errors.append(f"Missing required field: {field}")
    
    # Validate field types
    validation_rules = config.get('field_validation', {})
    field_types = validation_rules.get('field_types', {})
    
    for field, expected_type in field_types.items():
        if field in data:
            actual_type = type(data[field]).__name__
            if expected_type == 'int' and actual_type != 'int':
                try:
                    data[field] = int(data[field])
                except (ValueError, TypeError):
                    warnings.append(f"Field {field} expected int, got {actual_type}")
            elif expected_type == 'float' and actual_type not in ['float', 'int']:
                try:
                    data[field] = float(data[field])
                except (ValueError, TypeError):
                    warnings.append(f"Field {field} expected float, got {actual_type}")
            elif expected_type == 'string' and actual_type not in ['str', 'unicode']:
                data[field] = str(data[field])
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }

def apply_field_mapping(data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """Apply field mapping rules from YAML to data"""
    config = get_yaml_config()
    
    if not config:
        return data
    
    mapped_data = data.copy()
    
    # Apply field mapping rules
    field_mapping_rules = config.get('field_mapping_rules', {})
    
    # Handle backward compatibility mappings
    if 'high_price_to_high' in field_mapping_rules:
        if 'high_price' in mapped_data and 'high' not in mapped_data:
            mapped_data['high'] = mapped_data['high_price']
    
    if 'low_price_to_low' in field_mapping_rules:
        if 'low_price' in mapped_data and 'low' not in mapped_data:
            mapped_data['low'] = mapped_data['low_price']
    
    if 'last_update_to_timestamp' in field_mapping_rules:
        if 'last_update' in mapped_data and 'last_update_timestamp' not in mapped_data:
            mapped_data['last_update_timestamp'] = mapped_data['last_update']
    
    # Add display aliases
    if 'last_price_to_current_price' in field_mapping_rules:
        if 'last_price' in mapped_data and 'current_price' not in mapped_data:
            mapped_data['current_price'] = mapped_data['last_price']
    
    return mapped_data

# Convenience functions for backward compatibility
def normalize_session_record(data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
    """Normalize session record using YAML field mappings"""
    if not isinstance(data, dict):
        return data
    
    session_fields = get_redis_session_fields()
    
    # Map aliases to canonical fields
    for alias, canonical in session_fields.items():
        if alias in data and canonical not in data:
            data[canonical] = data[alias]
    
    # Add aliases for backward compatibility
    if include_aliases:
        for alias, canonical in session_fields.items():
            if canonical in data and alias not in data:
                data[alias] = data[canonical]
    
    return data

def normalize_indicator_record(data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
    """Normalize indicator record using YAML field mappings"""
    if not isinstance(data, dict):
        return data
    
    indicator_fields = get_calculated_indicator_fields()
    
    # Map aliases to canonical fields
    for alias, canonical in indicator_fields.items():
        if alias in data and canonical not in data:
            data[canonical] = data[alias]
    
    # Add aliases for backward compatibility
    if include_aliases:
        for alias, canonical in indicator_fields.items():
            if canonical in data and alias not in data:
                data[alias] = data[canonical]
    
    return data

# Legacy compatibility - these functions maintain the same interface
def get_field_mapping_manager():
    """Legacy compatibility - returns a mock object with the same interface"""
    class MockFieldMappingManager:
        def resolve_redis_session_field(self, field_name: str) -> str:
            return resolve_session_field(field_name)
        
        def resolve_calculated_field(self, field_name: str) -> str:
            return resolve_calculated_field(field_name)
        
        def get_redis_session_fields(self) -> Dict[str, str]:
            return get_redis_session_fields()
        
        def get_calculated_indicator_fields(self) -> Dict[str, str]:
            return get_calculated_indicator_fields()
        
        def resolve_volume_profile_field(self, field_name: str) -> str:
            return resolve_volume_profile_field(field_name)
        
        def get_volume_profile_fields(self) -> Dict[str, str]:
            return get_volume_profile_fields()
        
        def get_volume_profile_patterns(self) -> Dict[str, str]:
            return get_volume_profile_patterns()
        
        def get_websocket_core_fields(self) -> Dict[str, str]:
            return get_websocket_core_fields()
        
        def get_required_fields(self, asset_type: str) -> List[str]:
            return get_required_fields(asset_type)
        
        def get_asset_specific_fields(self, asset_type: str) -> List[str]:
            return get_asset_specific_fields(asset_type)
        
        def get_preferred_timestamp(self, data: Dict[str, Any]) -> Optional[Union[str, float, int]]:
            return get_preferred_timestamp(data)
        
        def get_session_key_pattern(self, symbol: str, date: str = None) -> str:
            return get_session_key_pattern(symbol, date)
        
        def get_websocket_mode_fields(self, mode: str) -> List[str]:
            return get_websocket_mode_fields(mode)
        
        def get_field_categories(self) -> Dict[str, List[str]]:
            return get_field_categories()
        
        def validate_data(self, data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
            return validate_data(data, asset_type)
        
        def apply_field_mapping(self, data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
            return apply_field_mapping(data, asset_type)
        
        def normalize_session_record(self, data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
            return normalize_session_record(data, include_aliases)
        
        def normalize_indicator_record(self, data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
            return normalize_indicator_record(data, include_aliases)
        
        def get_session_aliases_by_canonical(self) -> Dict[str, List[str]]:
            """Get session aliases grouped by canonical field names"""
            session_fields = get_redis_session_fields()
            aliases_by_canonical = {}
            
            for alias, canonical in session_fields.items():
                if canonical not in aliases_by_canonical:
                    aliases_by_canonical[canonical] = []
                aliases_by_canonical[canonical].append(alias)
            
            return aliases_by_canonical
    
    return MockFieldMappingManager()


# Additional convenience functions for backward compatibility
def get_standard_session_key(symbol: str, date: str = None) -> str:
    """Get standard session key (convenience function)"""
    return get_session_key_pattern(symbol, date)


def apply_standard_field_mapping(data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """Apply standard field mapping to data (convenience function)"""
    return apply_field_mapping(data, asset_type)


def validate_standard_data(data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """Validate data against standard requirements (convenience function)"""
    return validate_data(data, asset_type)


def resolve_indicator_field(field_name: str) -> str:
    """Convenience accessor for resolving calculated indicator field names."""
    return resolve_calculated_field(field_name)
