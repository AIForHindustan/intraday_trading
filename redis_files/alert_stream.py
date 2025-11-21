# redis/alert_stream.py
"""
Alert Stream Manager - Publishes alerts to streams for Telegram and stores validation results in DB 2.

Architecture:
- DB 1 (realtime): Alert streams for Telegram consumption (alerts:stream, alerts:telegram)
- DB 2 (pattern_validation): Pattern validation results storage

Does NOT detect patterns - receives detected patterns.
"""
import json
import time
import logging
import random
import string
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

class AlertStreamManager:
    """Manages alert publishing to streams (Telegram) and validation storage (DB 2)."""
    
    def __init__(self, redis_client=None):
        """
        Initialize Alert Stream Manager.
        
        Args:
            redis_client: Optional Redis client. If None, will initialize DB 1 and DB 2 clients.
        """
        self.logger = logging.getLogger(__name__)
        
        # Load pattern registry config
        self.pattern_registry = self._load_pattern_registry()
        
        # Initialize DB 1 client for alert streams (Telegram consumption)
        try:
            from redis_files.redis_client import RedisManager82
            self.redis_db1 = RedisManager82.get_client(process_name="alert_stream", db=1)
        except Exception as e:
            self.logger.error(f"Failed to initialize DB 1 client: {e}")
            self.redis_db1 = redis_client  # Fallback to provided client
        
        # Initialize DB 2 client for pattern validation storage
        try:
            from redis_files.redis_client import RedisManager82
            self.redis_db2 = RedisManager82.get_client(process_name="alert_stream_db2", db=2)
        except Exception as e:
            self.logger.error(f"Failed to initialize DB 2 client: {e}")
            self.redis_db2 = None
        
        # Backward compatibility
        self.redis = self.redis_db1
    
    def _load_pattern_registry(self) -> Dict[str, Any]:
        """
        Load pattern registry configuration from JSON file.
        
        Returns:
            Pattern registry dictionary, or empty dict if loading fails
        """
        try:
            # Try to find pattern_registry_config.json relative to this file
            current_file = Path(__file__)
            # Go up from redis_files/ to root, then to patterns/data/
            config_path = current_file.parent.parent / "patterns" / "data" / "pattern_registry_config.json"
            
            if config_path.exists():
                with open(config_path, 'r') as f:
                    registry = json.load(f)
                    self.logger.info(f"✅ Loaded pattern registry: {registry.get('metadata', {}).get('total_patterns', 0)} patterns")
                    return registry
            else:
                self.logger.warning(f"⚠️ Pattern registry config not found at {config_path}")
                return {}
        except Exception as e:
            self.logger.error(f"❌ Failed to load pattern registry config: {e}")
            return {}
    
    def get_pattern_config(self, pattern_type: str) -> Optional[Dict[str, Any]]:
        """
        Get pattern configuration from registry.
        
        Args:
            pattern_type: Pattern type name (e.g., 'volume_spike', 'ict_killzone')
            
        Returns:
            Pattern configuration dictionary or None if not found
        """
        if not self.pattern_registry:
            return None
        
        pattern_configs = self.pattern_registry.get('pattern_configs', {})
        return pattern_configs.get(pattern_type)
    
    def get_pattern_emergency_tier(self, pattern_type: str) -> Optional[str]:
        """
        Get emergency filter tier for pattern.
        
        Returns:
            Emergency filter tier ('TIER_1', 'TIER_2', 'TIER_3') or None
        """
        config = self.get_pattern_config(pattern_type)
        if config:
            return config.get('emergency_filter_tier')
        return None
    
    def is_pattern_enabled(self, pattern_type: str) -> bool:
        """
        Check if pattern is enabled in registry.
        
        Args:
            pattern_type: Pattern type name
            
        Returns:
            True if pattern is enabled, False otherwise
        """
        config = self.get_pattern_config(pattern_type)
        if config:
            return config.get('enabled', False)
        return True  # Default to enabled if not in registry
    
    def _generate_alert_id(self, symbol: str) -> str:
        """
        Generate unique alert ID using nanosecond timestamp + random suffix.
        Format: alert_{timestamp_ns}_{random_suffix}
        """
        timestamp_ns = int(time.time() * 1e9)
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"alert_{timestamp_ns}_{random_suffix}"
    
    def _prepare_alert_payload(self, symbol: str, pattern_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare complete alert payload for Telegram and validation.
        
        Includes all fields needed for Telegram formatting and validation tracking.
        Enriches payload with pattern registry metadata.
        """
        # ✅ SOURCE OF TRUTH: Ensure symbol is canonicalized
        from redis_files.redis_key_standards import RedisKeyStandards
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        timestamp_ms = int(time.time() * 1000)
        alert_id = self._generate_alert_id(symbol)
        pattern_type = pattern_data.get('pattern_type', 'unknown')
        
        # Get pattern config from registry
        pattern_config = self.get_pattern_config(pattern_type)
        
        # Base alert payload
        alert_payload = {
            'alert_id': alert_id,
            'symbol': symbol,
            'pattern_type': pattern_type,
            'pattern_name': pattern_data.get('pattern_name', 'unknown'),
            'confidence': float(pattern_data.get('confidence', 0.0)),
            'current_price': float(pattern_data.get('current_price', pattern_data.get('last_price', 0.0))),
            'timestamp': timestamp_ms,
            'timestamp_ms': timestamp_ms,
            'signal': pattern_data.get('signal', 'NEUTRAL'),
            'direction': pattern_data.get('direction', 'NEUTRAL'),
            'created_at': datetime.now().isoformat(),
        }
        
        # Add pattern registry metadata if available
        if pattern_config:
            alert_payload['pattern_category'] = pattern_config.get('category', 'UNKNOWN')
            alert_payload['pattern_enabled'] = pattern_config.get('enabled', True)
            alert_payload['mathematical_integrity'] = pattern_config.get('mathematical_integrity', False)
            alert_payload['risk_manager_integration'] = pattern_config.get('risk_manager_integration', False)
            alert_payload['emergency_filter_tier'] = pattern_config.get('emergency_filter_tier')
            alert_payload['emergency_filter_group_size'] = pattern_config.get('emergency_filter_group_size')
            alert_payload['pattern_description'] = pattern_config.get('description', '')
        
        # Add optional fields if present
        optional_fields = [
            'entry_price', 'stop_loss', 'take_profit', 'target_price',
            'volume_ratio', 'volume_profile', 'greeks', 'underlying_price',
            'strike_price', 'expiry', 'option_type', 'iv', 'dte',
            'expected_move', 'expected_move_min', 'expected_move_max',
            'expected_move_context', 'risk_reward_ratio', 'quality_score',
            'news_context', 'vix_regime', 'market_regime',
            'ideal_conditions', 'position_management',
            'entry_premium_points', 'entry_premium_rupees',
            'ce_premium', 'pe_premium',
            'stop_loss_rupees', 'target_rupees',
            'breakeven_upper', 'breakeven_lower', 'lot_size'
        ]
        
        for field in optional_fields:
            if field in pattern_data:
                alert_payload[field] = pattern_data[field]
        
        return alert_payload
    
    def publish_alert(self, symbol: str, pattern_data: Dict[str, Any]) -> bool:
        """
        Publish pattern alert to streams for Telegram and store validation in DB 2.
        
        Streams (DB 1):
        - alerts:stream: Main alert stream for all consumers
        - alerts:telegram: Dedicated stream for Telegram notifications
        
        Validation Storage (DB 2):
        - alert:{alert_id}: Alert data for validation tracking
        
        Args:
            symbol: Trading symbol
            pattern_data: Pattern detection data dictionary
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            if not self.redis_db1:
                self.logger.error("DB 1 client not available for alert streams")
                return False
            
            pattern_type = pattern_data.get('pattern_type', 'unknown')
            
            # ✅ Check if pattern is enabled in registry
            if not self.is_pattern_enabled(pattern_type):
                self.logger.debug(f"⏭️ Skipping disabled pattern: {pattern_type} for {symbol}")
                return False
            
            # ✅ SOURCE OF TRUTH: Canonicalize symbol
            from redis_files.redis_key_standards import RedisKeyStandards, get_key_builder
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            key_builder = get_key_builder()
            
            alert_payload = self._prepare_alert_payload(canonical_symbol, pattern_data)
            alert_id = alert_payload['alert_id']
            
            # ✅ DB 1: Publish to main alert stream (for all consumers)
            # ✅ SOURCE OF TRUTH: Use key builder for stream key
            try:
                alerts_stream_key = key_builder.live_alerts_stream()
                self.redis_db1.xadd(
                    alerts_stream_key,
                    {'data': json.dumps(alert_payload)},
                    maxlen=10000,
                    approximate=True
                )
                self.logger.debug(f"✅ Published alert {alert_id} to {alerts_stream_key}")
            except Exception as e:
                self.logger.error(f"Failed to publish to {alerts_stream_key}: {e}")
            
            # ✅ DB 1: Publish to Telegram-specific stream with formatted message
            # ✅ SOURCE OF TRUTH: Use key builder for stream key
            try:
                # Format message using HumanReadableAlertTemplates for consistent Telegram formatting
                try:
                    from alerts.notifiers import HumanReadableAlertTemplates
                    formatted_message = HumanReadableAlertTemplates.format_telegram_alert(alert_payload)
                    alert_payload['formatted_message'] = formatted_message
                except Exception as format_error:
                    self.logger.warning(f"Failed to format message for Telegram: {format_error}")
                    # Continue without formatted message if formatting fails
                
                alerts_telegram_key = key_builder.live_alerts_telegram()
                self.redis_db1.xadd(
                    alerts_telegram_key,
                    {'data': json.dumps(alert_payload)},
                    maxlen=5000,
                    approximate=True
                )
                self.logger.debug(f"✅ Published alert {alert_id} to {alerts_telegram_key}")
            except Exception as e:
                self.logger.error(f"Failed to publish to {alerts_telegram_key}: {e}")
            
            # ✅ DB 1: Publish to pattern-specific stream (optional, for filtering)
            # ✅ SOURCE OF TRUTH: Use key builder for pattern stream
            try:
                pattern_type = alert_payload.get('pattern_type', 'unknown')
                pattern_stream_key = key_builder.live_pattern_stream(pattern_type, canonical_symbol)
                self.redis_db1.xadd(
                    pattern_stream_key,
                    {'data': json.dumps(alert_payload)},
                    maxlen=1000,
                    approximate=True
                )
            except Exception as e:
                self.logger.debug(f"Pattern-specific stream publish failed (non-critical): {e}")
            
            # ✅ DB 2: Store alert for pattern validation tracking
            # ✅ SOURCE OF TRUTH: Use key builder for alert storage key
            if self.redis_db2:
                try:
                    alert_data = alert_payload.copy()
                    alert_data['storage_db'] = 2
                    alert_data['stored_at'] = datetime.now().isoformat()
                    
                    alert_storage_key = key_builder.analytics_alert_storage(alert_id)
                    self.redis_db2.set(
                        alert_storage_key,
                        json.dumps(alert_data),
                        ex=604800  # 7 days TTL
                    )
                    self.logger.debug(f"✅ Stored alert {alert_id} in DB 2 for validation")
                except Exception as e:
                    self.logger.error(f"Failed to store alert in DB 2: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Alert publish failed for {symbol}: {e}")
            return False

    def publish_batch_alerts(self, alerts: List[Dict[str, Any]]) -> bool:
        """
        Publish multiple alerts efficiently using pipelines.
        
        Args:
            alerts: List of alert dictionaries, each containing 'symbol' and pattern data
            
        Returns:
            True if batch published successfully, False otherwise
        """
        try:
            if not self.redis_db1:
                self.logger.error("DB 1 client not available for batch alert streams")
                return False
            
            # Prepare all alert payloads
            alert_payloads = []
            for alert_data in alerts:
                symbol = alert_data.get('symbol', 'UNKNOWN')
                # ✅ SOURCE OF TRUTH: Canonicalize symbol before preparing alert
                if symbol and symbol != 'UNKNOWN':
                    from redis_files.redis_key_standards import RedisKeyStandards, get_key_builder
                    symbol = RedisKeyStandards.canonical_symbol(symbol)
                alert_payload = self._prepare_alert_payload(symbol, alert_data)
                alert_payloads.append(alert_payload)
            
            # ✅ DB 1: Batch publish to streams using pipeline
            # ✅ SOURCE OF TRUTH: Use key builder for stream keys
            from redis_files.redis_key_standards import get_key_builder
            key_builder = get_key_builder()
            alerts_stream_key = key_builder.live_alerts_stream()
            alerts_telegram_key = key_builder.live_alerts_telegram()
            
            pipeline_db1 = self.redis_db1.pipeline()
            
            for alert_payload in alert_payloads:
                alert_json = json.dumps(alert_payload)
                
                # Main alert stream
                pipeline_db1.xadd(
                    alerts_stream_key,
                    {'data': alert_json},
                    maxlen=10000,
                    approximate=True
                )
                
                # Telegram stream
                pipeline_db1.xadd(
                    alerts_telegram_key,
                    {'data': alert_json},
                    maxlen=5000,
                    approximate=True
                )
            
            pipeline_db1.execute()
            self.logger.info(f"✅ Batch published {len(alert_payloads)} alerts to streams")
            
            # ✅ DB 2: Batch store alerts for validation
            if self.redis_db2:
                try:
                    pipeline_db2 = self.redis_db2.pipeline()
                    
                    # ✅ SOURCE OF TRUTH: Use key builder for alert storage keys
                    from redis_files.redis_key_standards import get_key_builder
                    key_builder = get_key_builder()
                    
                    for alert_payload in alert_payloads:
                        alert_id = alert_payload['alert_id']
                        alert_data = alert_payload.copy()
                        alert_data['storage_db'] = 2
                        alert_data['stored_at'] = datetime.now().isoformat()
                        
                        alert_storage_key = key_builder.analytics_alert_storage(alert_id)
                        pipeline_db2.set(
                            alert_storage_key,
                            json.dumps(alert_data),
                            ex=604800  # 7 days TTL
                        )
                    
                    pipeline_db2.execute()
                    self.logger.info(f"✅ Batch stored {len(alert_payloads)} alerts in DB 2")
                except Exception as e:
                    self.logger.error(f"Failed to batch store alerts in DB 2: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Batch alert publish failed: {e}")
            return False
    
    def store_validation_result(self, alert_id: str, validation_data: Dict[str, Any]) -> bool:
        """
        Store pattern validation result in DB 2.
        
        Storage Locations (DB 2):
        - Hash: validation:{alert_id} → {'alert_id': str, 'data': JSON, 'stored_at': ISO}
        - Stream: alerts:validation:results (maxlen=1000)
        - List: validation_results:recent (maxlen=100)
        
        Args:
            alert_id: Alert ID to associate validation with
            validation_data: Validation result dictionary
            
        Returns:
            True if stored successfully, False otherwise
        """
        try:
            if not self.redis_db2:
                self.logger.warning("DB 2 client not available, cannot store validation result")
                return False
            
            # Add metadata
            validation_data['stored_at'] = datetime.now().isoformat()
            validation_data['storage_db'] = 2
            validation_data['alert_id'] = alert_id
            
            # ✅ SOURCE OF TRUTH: Use key builder for all validation keys
            from redis_files.redis_key_standards import get_key_builder
            key_builder = get_key_builder()
            
            # 1. Store as hash for direct lookup
            hash_key = key_builder.analytics_validation_storage(alert_id)
            self.redis_db2.hset(hash_key, mapping={
                'alert_id': alert_id,
                'data': json.dumps(validation_data),
                'stored_at': validation_data['stored_at']
            })
            
            # 2. Store in stream for real-time consumption
            stream_key = key_builder.analytics_validation_stream()
            stream_data = {}
            for key, value in validation_data.items():
                if isinstance(value, (dict, list)):
                    stream_data[key] = json.dumps(value)
                else:
                    stream_data[key] = str(value)
            
            self.redis_db2.xadd(
                stream_key,
                stream_data,
                maxlen=1000,
                approximate=True
            )
            
            # 3. Store in recent list (FIFO, max 100)
            recent_key = key_builder.analytics_validation_recent()
            self.redis_db2.lpush(recent_key, json.dumps(validation_data))
            self.redis_db2.ltrim(recent_key, 0, 99)  # Keep last 100
            
            self.logger.debug(f"✅ Stored validation result for {alert_id} in DB 2")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error storing validation result for {alert_id} in DB 2: {e}")
            return False
