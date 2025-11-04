"""
Alert Manager - Core Alert Processing Pipeline
============================================

Core alert management functionality with conflict resolution and pattern-specific thresholds.
Consolidated from scanner/alert_manager.py.

Classes:
- AlertManager: Base alert management
- EnhancedAlertManager: Enhanced with conflict resolution
- ProductionAlertManager: Production-ready alert manager
- AlertConflictResolver: Resolves conflicts between pattern detectors

Created: October 9, 2025
"""

import sys
import os
import json
import time
import logging
import threading
import subprocess
import re
import glob
import calendar
import requests
import redis
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo
import pytz

from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_daily_zset
from config.utils.timestamp_normalizer import TimestampNormalizer
from community_bots.transparency_bot import TransparencyBot

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

# Import enhanced validation components
try:
    from patterns.volume_profile_manager import VolumeProfileManager
    VOLUME_PROFILE_AVAILABLE = True
except ImportError:
    VOLUME_PROFILE_AVAILABLE = False
    VolumeProfileManager = None

try:
    from patterns.ict.pattern_detector import ICTLiquidityDetector
    ICT_LIQUIDITY_AVAILABLE = True
except ImportError:
    ICT_LIQUIDITY_AVAILABLE = False
    ICTLiquidityDetector = None

# Import VIX utilities
try:
    from utils.vix_utils import (
        get_current_vix,
        get_vix_value,
        get_vix_regime,
        is_vix_panic,
    )
except ImportError:
    def get_current_vix():
        return None
    def get_vix_value():
        return None
    def get_vix_regime():
        return "NORMAL"
    def is_vix_panic():
        return False

# Import field mapping utilities
try:
    from utils.yaml_field_loader import (
        get_field_mapping_manager,
        apply_field_mapping as apply_standard_field_mapping,
        validate_data as validate_standard_data,
        get_session_key_pattern as get_standard_session_key
    )
    from redis_files.volume_state_manager import get_volume_manager as get_volume_state_manager
except ImportError:
    def get_field_mapping_manager():
        return None
    def apply_standard_field_mapping(data, asset_type='equity'):
        return data
    def validate_standard_data(data, asset_type='equity'):
        return {'valid': True, 'errors': [], 'warnings': []}
    def get_standard_session_key(symbol, date=None):
        return f"session:{symbol}"
    def get_volume_state_manager():
        return None

# Add project root to Python path for proper imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from redis_files.redis_calculations import RedisCalculations
except ImportError:
    RedisCalculations = None

# Load threshold configuration from consolidated config
try:
    from config.thresholds import get_volume_threshold, get_confidence_threshold, get_move_threshold, get_all_thresholds_for_regime
    THRESHOLD_CONFIG_AVAILABLE = True
except ImportError as e:
    logging.warning(f"⚠️  Could not import volume_thresholds: {e}")
    logging.warning("   Using hardcoded thresholds")
    THRESHOLD_CONFIG_AVAILABLE = False

# Import pattern registry for dynamic pattern management
try:
    import json
    from pathlib import Path
    _pattern_registry_path = Path(__file__).parent.parent / "patterns" / "data" / "pattern_registry_config.json"
    with open(_pattern_registry_path, 'r') as f:
        PATTERN_REGISTRY = json.load(f)
    PATTERN_REGISTRY_AVAILABLE = True
except ImportError:
    PATTERN_REGISTRY = {}
    PATTERN_REGISTRY_AVAILABLE = False
except FileNotFoundError:
    PATTERN_REGISTRY = {}
    PATTERN_REGISTRY_AVAILABLE = False

# Initialize logger
logger = logging.getLogger(__name__)

try:
    from .notifiers import HumanReadableAlertTemplates
except Exception:
    HumanReadableAlertTemplates = None


def get_patterns_from_registry(category=None):
    """Get patterns from pattern registry config."""
    if not PATTERN_REGISTRY_AVAILABLE:
        return []
    
    if category:
        categories = PATTERN_REGISTRY.get("categories", {})
        category_data = categories.get(category, {})
        return category_data.get("patterns", [])
    else:
        # Return all patterns from all categories
        all_patterns = []
        categories = PATTERN_REGISTRY.get("categories", {})
        for cat_data in categories.values():
            all_patterns.extend(cat_data.get("patterns", []))
        return all_patterns


class EmergencyAlertFilter:
    """Emergency filter to prioritize critical alerts that should be sent immediately."""
    
    def __init__(self):
        self.priority_patterns = {
            'TIER_1': ['kow_signal_straddle', 'reversal', 'NEWS_ALERT'],  # Always send
            'TIER_2': ['volume_breakout', 'ict_liquidity_pools'],         # Group after 2
            'TIER_3': ['volume_spike', 'hidden_accumulation']             # Group after 5
        }
        
    def should_send_immediately(self, alert):
        """
        Check if alert should be sent immediately based on priority tier.
        
        Args:
            alert: Alert data dictionary
            
        Returns:
            True if alert should be sent immediately (TIER_1 with high confidence),
            False otherwise (will go through normal filtering and grouping)
        """
        if not alert or not isinstance(alert, dict):
            return False
            
        pattern = alert.get('pattern') or alert.get('pattern_type', '')
        confidence = alert.get('confidence', 0)
        
        # Tier 1: Always send high-priority patterns with confidence >= 0.85
        if pattern in self.priority_patterns['TIER_1'] and confidence >= 0.85:
            logger.debug(f"🚨 Emergency filter: TIER_1 alert {pattern} (confidence: {confidence:.2f}) - sending immediately")
            return True
            
        # Tier 2 & 3: Apply grouping (return False to continue with normal flow)
        return False
    
    def get_alert_tier(self, alert):
        """Get the priority tier for an alert."""
        if not alert or not isinstance(alert, dict):
            return None
            
        pattern = alert.get('pattern') or alert.get('pattern_type', '')
        
        if pattern in self.priority_patterns['TIER_1']:
            return 'TIER_1'
        elif pattern in self.priority_patterns['TIER_2']:
            return 'TIER_2'
        elif pattern in self.priority_patterns['TIER_3']:
            return 'TIER_3'
        
        return None


class AlertConflictResolver:
    """Resolve conflicts between multiple pattern detectors."""
    
    # Define pattern priorities for 8 core patterns + scalper strategy (higher number = higher priority)
    PATTERN_PRIORITIES = {
        'reversal': 90,
        'volume_breakout': 80,
        'breakout': 80,
        'scalper_opportunity': 85,  # High priority for scalping opportunities
        'volume_spike': 70,
        'volume_price_divergence': 60,
        'hidden_accumulation': 60,
        'upside_momentum': 50,
        'downside_momentum': 50,
    }
    
    def __init__(self):
        self.resolved_conflicts = 0
        self.conflict_stats = {}
    
    def resolve_conflicts(self, alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Resolve conflicts between multiple alerts for the same symbol.
        
        Args:
            alerts: List of alert dictionaries
            
        Returns:
            List of resolved alerts with conflicts removed
        """
        if not alerts:
            return []
        
        # Group alerts by symbol
        symbol_groups = {}
        for alert in alerts:
            symbol = alert.get('symbol', 'UNKNOWN')
            if symbol not in symbol_groups:
                symbol_groups[symbol] = []
            symbol_groups[symbol].append(alert)
        
        resolved_alerts = []
        
        for symbol, symbol_alerts in symbol_groups.items():
            if len(symbol_alerts) == 1:
                # No conflicts for this symbol
                resolved_alerts.extend(symbol_alerts)
                continue
            
            # Resolve conflicts for this symbol
            resolved = self._resolve_symbol_conflicts(symbol, symbol_alerts)
            resolved_alerts.extend(resolved)
        
        return resolved_alerts
    
    def _resolve_symbol_conflicts(self, symbol: str, alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Resolve conflicts for a specific symbol."""
        if len(alerts) <= 1:
            return alerts
        
        # Sort by priority (highest first)
        sorted_alerts = sorted(alerts, key=lambda x: self._get_alert_priority(x), reverse=True)
        
        # Keep the highest priority alert
        highest_priority = sorted_alerts[0]
        conflict_count = len(sorted_alerts) - 1
        
        # Update stats
        self.resolved_conflicts += conflict_count
        pattern_type = highest_priority.get('pattern', 'unknown')
        if pattern_type not in self.conflict_stats:
            self.conflict_stats[pattern_type] = 0
        self.conflict_stats[pattern_type] += 1
        
        logger.debug(f"Resolved {conflict_count} conflicts for {symbol}, kept {pattern_type}")
        
        return [highest_priority]
    
    def _get_alert_priority(self, alert: Dict[str, Any]) -> int:
        """Get priority score for an alert."""
        pattern = alert.get('pattern', 'unknown')
        return self.PATTERN_PRIORITIES.get(pattern, 0)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get conflict resolution statistics."""
        return {
            'resolved_conflicts': self.resolved_conflicts,
            'conflict_stats': self.conflict_stats.copy()
        }

# AlertGrouper class removed - functionality integrated into EnhancedAlertValidator

class EnhancedAlertValidator:
    """COMPREHENSIVE SIGNAL VALIDATION AND ALERT GROUPING"""
    
    def __init__(self, redis_client, volume_profile_manager=None, ict_liquidity_detector=None):
        self.redis_client = redis_client
        self.volume_profile_manager = volume_profile_manager
        self.ict_liquidity_detector = ict_liquidity_detector
        
        # Initialize components if not provided
        if not self.volume_profile_manager and VOLUME_PROFILE_AVAILABLE:
            try:
                self.volume_profile_manager = VolumeProfileManager(redis_client)
            except Exception as e:
                logger.warning(f"Could not initialize VolumeProfileManager: {e}")
        
        if not self.ict_liquidity_detector and ICT_LIQUIDITY_AVAILABLE:
            try:
                self.ict_liquidity_detector = ICTLiquidityDetector(redis_client)
            except Exception as e:
                logger.warning(f"Could not initialize ICTLiquidityDetector: {e}")
        
        # Initialize alert grouping (integrated into validator)
        self._init_alert_grouping()
        logger.info("✅ Enhanced Alert Validator initialized (with integrated grouping)")
    
    def _init_alert_grouping(self):
        """Initialize alert grouping configuration"""
        self.grouping_config = {
            'time_window_seconds': 300,  # 5 minutes
            'min_group_size': 2,         # Don't group singles
            'max_group_size': 50,
            
            # NEVER GROUP THESE - CRITICAL (send immediately)
            'never_group_patterns': {
                'kow_signal_straddle', 'reversal', 'NEWS_ALERT', 
                'ict_liquidity_pools', 'ict_optimal_trade_entry'
            },
            
            # HIGH VOLUME PATTERNS - Group more aggressively
            'high_volume_patterns': {
                'volume_spike', 'volume_breakout', 'upside_momentum', 
                'downside_momentum', 'hidden_accumulation'
            },
            
            # Grouping dimensions
            'group_by_instrument': True,
            'group_by_pattern': True, 
            'group_by_direction': True,
            'group_by_timeframe': True,
            
            # Session boundaries (from filters.py MarketSession enum)
            'session_boundaries': ['MORNING', 'MID_MORNING', 'MIDDAY', 'AFTERNOON']
        }
        
        self.active_groups = {}
        self.alert_history = []
        self._cleanup_interval = 600  # Clean up old groups every 10 minutes
        self._last_cleanup = time.time()
        
        # Thread safety
        self._lock = threading.Lock()
    
    def should_apply_grouping(self, alert_data: Dict[str, Any]) -> bool:
        """Determine if grouping should be applied to this alert"""
        if not alert_data or not isinstance(alert_data, dict):
            return False
        
        # Don't group if alert type is already grouped
        if alert_data.get('type') == 'grouped_summary':
            return False
        
        # Check if alert passes grouping criteria
        return self.should_group_alert(alert_data)
    
    def should_group_alert(self, alert: Dict[str, Any]) -> bool:
        """Check if alert should be grouped"""
        if not alert or not isinstance(alert, dict):
            return False
            
        pattern = alert.get('pattern') or alert.get('pattern_type') or 'unknown'
        
        # NEVER group critical patterns
        if pattern in self.grouping_config['never_group_patterns']:
            logger.debug(f"Alert {pattern} excluded from grouping (critical pattern)")
            return False
            
        # Don't group very high confidence alerts (>=0.95)
        confidence = float(alert.get('confidence', 0.0))
        if confidence >= 0.95:
            logger.debug(f"Alert {pattern} excluded from grouping (high confidence: {confidence:.2f})")
            return False
            
        return True
    
    def add_alert_to_group(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add alert to appropriate group. Returns action dict.
        
        Returns:
            {'action': 'SEND_IMMEDIATE', 'alert': alert} - Send immediately
            {'action': 'SEND_GROUPED', 'alert': summary_alert} - Send grouped summary
            {'action': 'GROUPED', 'group_key': key} - Alert grouped, wait for flush
        """
        try:
            with self._lock:
                # Cleanup old groups periodically
                if time.time() - self._last_cleanup > self._cleanup_interval:
                    self._cleanup_stale_groups()
                    self._last_cleanup = time.time()
                
                if not self.should_group_alert(alert):
                    return {'action': 'SEND_IMMEDIATE', 'alert': alert}
                
                group_key = self.get_group_key(alert)
                
                if group_key not in self.active_groups:
                    self.active_groups[group_key] = {
                        'alerts': [],
                        'created_at': alert.get('timestamp_ms') or self.get_current_time(),
                        'first_alert': alert
                    }
                
                group = self.active_groups[group_key]
                group['alerts'].append(alert)
                
                # Check if group should be flushed
                if self.should_flush_group(group):
                    return self.flush_group(group_key)
                else:
                    logger.debug(f"Alert grouped: {group_key} (count: {len(group['alerts'])})")
                    return {'action': 'GROUPED', 'group_key': group_key}
                    
        except Exception as e:
            logger.error(f"Error in add_alert_to_group: {e}")
            # On error, send immediately to avoid losing alerts
            return {'action': 'SEND_IMMEDIATE', 'alert': alert}
    
    def get_group_key(self, alert: Dict[str, Any]) -> str:
        """Create grouping key using existing field names"""
        pattern = alert.get('pattern') or alert.get('pattern_type') or 'unknown'
        
        # Extract instrument symbol (handle both NSE:SYMBOL and SYMBOL formats)
        symbol = alert.get('symbol', 'UNKNOWN')
        if ':' in symbol:
            instrument = symbol.split(':')[-1]  # Extract just the symbol part
        else:
            instrument = symbol
        
        # Get direction from existing fields
        direction = alert.get('direction', 'neutral')
        if not direction or direction == 'neutral':
            # Fallback to signal if direction not available
            signal = alert.get('signal', '').upper()
            if signal in ['BUY', 'LONG', 'BULLISH']:
                direction = 'long'
            elif signal in ['SELL', 'SHORT', 'BEARISH']:
                direction = 'short'
            else:
                direction = 'neutral'
        
        # Get timeframe from existing fields
        timeframe = alert.get('timeframe', '5min')
        
        # Use session-based time window using existing market session logic
        alert_time_ms = alert.get('timestamp_ms') or alert.get('exchange_timestamp') or int(time.time() * 1000)
        time_window = alert_time_ms // (self.grouping_config['time_window_seconds'] * 1000)
        
        # Build group key based on configuration
        key_parts = []
        if self.grouping_config['group_by_instrument']:
            key_parts.append(instrument)
        if self.grouping_config['group_by_pattern']:
            key_parts.append(pattern)
        if self.grouping_config['group_by_direction']:
            key_parts.append(direction)
        if self.grouping_config['group_by_timeframe']:
            key_parts.append(timeframe)
        
        key_parts.append(str(time_window))
        
        return ':'.join(key_parts)
    
    def get_current_time(self) -> int:
        """Get current time in milliseconds"""
        return int(time.time() * 1000)
    
    def should_flush_group(self, group: Dict[str, Any]) -> bool:
        """Check if group should be sent based on size and time, respecting alert tiers"""
        try:
            alert_count = len(group['alerts'])
            created_at = group.get('created_at', self.get_current_time())
            time_elapsed = (self.get_current_time() - created_at) / 1000  # Convert to seconds
            
            # Determine minimum group size based on alert tier (if available)
            # Check first alert's tier to determine minimum flush size
            first_alert = group.get('first_alert') or (group['alerts'][0] if group['alerts'] else None)
            min_flush_size = self.grouping_config['min_group_size']
            
            if first_alert:
                # Try to get tier from emergency filter (if available)
                try:
                    emergency_filter = EmergencyAlertFilter()
                    tier = emergency_filter.get_alert_tier(first_alert)
                    
                    # Adjust minimum flush size based on tier
                    if tier == 'TIER_2':
                        min_flush_size = 2  # Group after 2
                    elif tier == 'TIER_3':
                        min_flush_size = 5  # Group after 5
                except Exception:
                    pass  # Fall back to default min_group_size
            
            # Flush conditions
            if alert_count >= self.grouping_config['max_group_size']:
                logger.debug(f"Group flush: max size reached ({alert_count})")
                return True
            if time_elapsed >= self.grouping_config['time_window_seconds']:
                logger.debug(f"Group flush: time window elapsed ({time_elapsed:.1f}s)")
                return True
            if alert_count >= min_flush_size and time_elapsed >= 60:  # 1 min minimum
                logger.debug(f"Group flush: tier-adjusted min size + time reached ({alert_count} alerts >= {min_flush_size}, {time_elapsed:.1f}s)")
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error in should_flush_group: {e}")
            return True  # Flush on error
    
    def flush_group(self, group_key: str) -> Optional[Dict[str, Any]]:
        """Convert group to summary alert and remove from active groups"""
        try:
            with self._lock:
                group = self.active_groups.pop(group_key, None)
                if not group or len(group['alerts']) < self.grouping_config['min_group_size']:
                    # If below min size, send individual alerts
                    if group:
                        alerts = group['alerts']
                        logger.debug(f"Group {group_key} below min size ({len(alerts)}), sending individually")
                        # Return first alert to send immediately
                        return {'action': 'SEND_IMMEDIATE', 'alert': alerts[0]} if alerts else None
                    return None
                    
                return self.create_summary_alert(group)
        except Exception as e:
            logger.error(f"Error in flush_group: {e}")
            return None
    
    def create_summary_alert(self, group: Dict[str, Any]) -> Dict[str, Any]:
        """Create grouped summary alert using existing field names - moved from AlertGrouper"""
        try:
            alerts = group['alerts']
            first_alert = group['first_alert']
            
            if not alerts or not first_alert:
                return None
            
            # Calculate aggregates (preserving original field names)
            confidences = [float(a.get('confidence', 0.0)) for a in alerts]
            prices = [float(a.get('last_price', 0.0)) for a in alerts if a.get('last_price')]
            volume_ratios = [float(a.get('volume_ratio', 0.0)) for a in alerts if a.get('volume_ratio')]
            
            # Get unique trigger reasons from pattern descriptions
            trigger_reasons = set()
            for alert in alerts:
                pattern_desc = alert.get('pattern_description') or alert.get('pattern_title') or ''
                if pattern_desc:
                    trigger_reasons.add(pattern_desc[:50])  # First 50 chars
            
            # Get timestamps (handle both formats)
            timestamps = []
            timestamps_ms = []
            for alert in alerts:
                ts = alert.get('timestamp') or alert.get('published_at')
                ts_ms = alert.get('timestamp_ms') or alert.get('exchange_timestamp') or int(time.time() * 1000)
                timestamps.append(ts)
                timestamps_ms.append(ts_ms)
            
            # Get representative alert (highest confidence) - PRESERVE ALL ITS FIELDS INCLUDING MATH CALCULATIONS
            representative_alert = max(alerts, key=lambda x: float(x.get('confidence', 0.0)))
            
            # Create summary alert - START WITH REPRESENTATIVE ALERT TO PRESERVE ALL MATH/LOGIC
            summary_alert = {
                **representative_alert,
                'type': 'grouped_summary',
                'grouped_count': len(alerts),
                'window_start': min(t for t in timestamps if t) if timestamps else first_alert.get('timestamp'),
                'window_end': max(t for t in timestamps if t) if timestamps else first_alert.get('timestamp'),
                'duration_minutes': round((max(timestamps_ms) - min(timestamps_ms)) / 60000.0, 1) if timestamps_ms else 0,
                'max_confidence': max(confidences) if confidences else representative_alert.get('confidence', 0.0),
                'avg_confidence': round(sum(confidences) / len(confidences), 3) if confidences else representative_alert.get('confidence', 0.0),
                'min_confidence': min(confidences) if confidences else representative_alert.get('confidence', 0.0),
                'price_range': (min(prices), max(prices)) if prices else (representative_alert.get('last_price', 0.0), representative_alert.get('last_price', 0.0)),
                'max_volume_ratio': max(volume_ratios) if volume_ratios else representative_alert.get('volume_ratio', 0.0),
                'avg_volume_ratio': round(sum(volume_ratios) / len(volume_ratios), 2) if volume_ratios else representative_alert.get('volume_ratio', 0.0),
                'trigger_reasons': list(trigger_reasons)[:5],
                'qualified_paths': list(set(a.get('qualified_path') for a in alerts if a.get('qualified_path'))),
                'representative_alert': representative_alert,
                'news_count': sum(1 for a in alerts if a.get('has_news')),
                'pattern_title_grouped': f"📊 {representative_alert.get('pattern', '').replace('_', ' ').title()} - Grouped ({len(alerts)} signals)",
                'pattern_description_grouped': f"Multiple {representative_alert.get('pattern', '')} signals detected with {max(confidences)*100:.1f}% max confidence" if confidences else f"Multiple {representative_alert.get('pattern', '')} signals detected",
                'trading_instruction_grouped': f"WATCH - {len(alerts)} similar signals in {round((max(timestamps_ms) - min(timestamps_ms)) / 60000.0, 1)}min" if timestamps_ms else f"WATCH - {len(alerts)} similar signals",
            }
            
            # Update fields conditionally
            if alerts and alerts[-1].get('last_price'):
                summary_alert['last_price'] = alerts[-1].get('last_price')
            if volume_ratios:
                max_vol_ratio = max(volume_ratios)
                if max_vol_ratio > summary_alert.get('volume_ratio', 0.0):
                    summary_alert['volume_ratio'] = max_vol_ratio
            if not summary_alert.get('price_change') and len(prices) >= 2 and prices[0] > 0:
                summary_alert['price_change'] = round(((prices[-1] - prices[0]) / prices[0]) * 100, 2)
            if any(a.get('has_news') for a in alerts):
                summary_alert['has_news'] = True
            if not summary_alert.get('market_session'):
                summary_alert['market_session'] = first_alert.get('market_session')
            if not summary_alert.get('vix_regime'):
                summary_alert['vix_regime'] = first_alert.get('vix_regime')
            if not summary_alert.get('pattern_title'):
                summary_alert['pattern_title'] = summary_alert.get('pattern_title_grouped', '')
            if not summary_alert.get('pattern_description'):
                summary_alert['pattern_description'] = summary_alert.get('pattern_description_grouped', '')
            if not summary_alert.get('trading_instruction'):
                summary_alert['trading_instruction'] = summary_alert.get('trading_instruction_grouped', '')
            if not summary_alert.get('urgency'):
                summary_alert['urgency'] = 'medium' if len(alerts) < 10 else 'high'
            if timestamps_ms:
                summary_alert['timestamp_ms'] = max(timestamps_ms)
            if not summary_alert.get('published_at'):
                summary_alert['published_at'] = datetime.now().isoformat()
            
            # Clean up temporary grouped fields
            summary_alert.pop('pattern_title_grouped', None)
            summary_alert.pop('pattern_description_grouped', None)
            summary_alert.pop('trading_instruction_grouped', None)
            
            logger.info(f"Created grouped summary: {len(alerts)} alerts for {summary_alert.get('symbol')} {summary_alert.get('pattern')}")
            
            return {'action': 'SEND_GROUPED', 'alert': summary_alert}
            
        except Exception as e:
            logger.error(f"Error creating summary alert: {e}")
            if alerts:
                return {'action': 'SEND_IMMEDIATE', 'alert': alerts[0]}
            return None
    
    def _check_and_flush_stale_groups(self):
        """Check all active groups and flush those that meet flush conditions, sending notifications"""
        try:
            with self._lock:
                groups_to_flush = []
                group_data = {}  # Store group data before flushing
                for group_key, group in list(self.active_groups.items()):
                    if self.should_flush_group(group):
                        groups_to_flush.append(group_key)
                        group_data[group_key] = len(group.get('alerts', []))
                
                # Flush groups and send notifications (return first flushed group)
                for group_key in groups_to_flush:
                    alert_count = group_data.get(group_key, 0)
                    flush_result = self.flush_group(group_key)
                    if flush_result and flush_result.get('action') == 'SEND_GROUPED':
                        summary_alert = flush_result.get('alert')
                        if summary_alert:
                            logger.info(f"📊 Auto-flushing group {group_key}: {alert_count} alerts")
                            # Return flush result to be handled by caller for notification
                            return flush_result
                    elif flush_result and flush_result.get('action') == 'SEND_IMMEDIATE':
                        # Group below min size - individual alert returned
                        logger.info(f"📊 Auto-flushing group {group_key}: sending individual alert (below min size)")
                        return flush_result
        except Exception as e:
            logger.error(f"Error in _check_and_flush_stale_groups: {e}")
        return None
    
    def _cleanup_stale_groups(self):
        """Clean up groups that are too old"""
        try:
            current_time = self.get_current_time()
            stale_threshold = current_time - (self.grouping_config['time_window_seconds'] * 2 * 1000)
            
            groups_to_remove = []
            for group_key, group in self.active_groups.items():
                created_at = group.get('created_at', 0)
                if created_at < stale_threshold:
                    groups_to_remove.append(group_key)
            
            for key in groups_to_remove:
                self.flush_group(key)
                if key in self.active_groups:
                    del self.active_groups[key]
                    logger.debug(f"Cleaned up stale group: {key}")
        except Exception as e:
            logger.error(f"Error in _cleanup_stale_groups: {e}")
        
    def validate_enhanced_alert(self, pattern: Dict) -> Optional[Dict]:
        """END-TO-END SIGNAL VALIDATION"""
        
        # STEP 1: BASIC SCHEMA VALIDATION (Existing)
        if not self._validate_schema(pattern):
            return None
        
        # STEP 2: MULTI-LAYER PRE-VALIDATION (NEW - CRITICAL)
        pre_validation = self._run_pre_validation(pattern)
        if not pre_validation['passed']:
            self._log_rejected_signal(pattern, 'PRE_VALIDATION_FAILED', pre_validation)
            return None
        
        # STEP 3: ENHANCED 6-PATH VALIDATION
        path_results = self._run_enhanced_path_validation(pattern, pre_validation)
        qualified_path = self._select_best_path(path_results)
        
        if not qualified_path:
            self._log_rejected_signal(pattern, 'NO_PATH_QUALIFIED', path_results)
            return None
        
        # STEP 4: RISK-ADJUSTED POSITION SIZING (NEW)
        risk_adjusted_pattern = self._apply_risk_adjustments(pattern, qualified_path, pre_validation)
        
        # STEP 5: REDIS DEDUPLICATION (Existing)
        if not self._check_redis_deduplication(risk_adjusted_pattern):
            return None
        
        # STEP 6: PROFITABILITY VALIDATION (Enhanced)
        if not self._validate_enhanced_profitability(risk_adjusted_pattern, pre_validation):
            return None
        
        return risk_adjusted_pattern
    
    def _validate_schema(self, pattern: Dict) -> bool:
        """Enhanced schema validation using unified schemas"""
        try:
            # Import pattern validation from unified schemas
            from patterns.utils.pattern_schema import validate_pattern
            
            # Use comprehensive pattern validation
            is_valid = validate_pattern(pattern)
            
            if not is_valid:
                logger.debug(f"Schema validation failed for pattern: {pattern.get('symbol', 'UNKNOWN')}")
                return False
            
            # Additional validation for enhanced fields
            enhanced_fields = ['action', 'position_size', 'stop_loss', 'expected_move']
            for field in enhanced_fields:
                if field in pattern and pattern[field] is None:
                    logger.debug(f"Enhanced field {field} is None for pattern: {pattern.get('symbol', 'UNKNOWN')}")
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Enhanced schema validation failed, using basic validation: {e}")
            # Fallback to basic validation
            required_fields = ['symbol', 'pattern', 'confidence', 'last_price']
            return all(pattern.get(field) is not None for field in required_fields)
    
    def _run_pre_validation(self, pattern: Dict) -> Dict:
        """CRITICAL: Multi-layer pre-validation"""
        symbol = pattern.get('symbol')
        
        # Get market context data
        volume_nodes = {}
        if self.volume_profile_manager:
            try:
                volume_nodes = self.volume_profile_manager.get_volume_nodes(symbol) or {}
            except Exception as e:
                logger.debug(f"Could not get volume nodes for {symbol}: {e}")
        
        liquidity_pools = {}
        if self.ict_liquidity_detector:
            try:
                liquidity_pools = self.ict_liquidity_detector.detect_liquidity_pools(symbol) or {}
            except Exception as e:
                logger.debug(f"Could not get liquidity pools for {symbol}: {e}")
        
        # Import validation methods from PatternDetector
        try:
            from patterns.pattern_detector import PatternDetector
            pattern_detector = PatternDetector(redis_client=self.redis_client)
            
            validations = {
                'volume_profile_alignment': pattern_detector.validate_volume_profile_alignment(pattern, volume_nodes),
                'ict_liquidity_alignment': pattern_detector.validate_ict_liquidity_alignment(pattern, liquidity_pools),
                'multi_timeframe_confirmation': pattern_detector.validate_multi_timeframe_confirmation(pattern, symbol),
                'pattern_specific_risk': pattern_detector.validate_pattern_specific_risk(pattern),
                'market_regime_appropriateness': self._validate_market_regime(pattern)
            }
        except Exception as e:
            logger.warning(f"Could not import PatternDetector validation methods: {e}")
            # Fallback to basic validation
            validations = {
                'volume_profile_alignment': True,  # Allow to proceed
                'ict_liquidity_alignment': True,   # Allow to proceed
                'multi_timeframe_confirmation': True,  # Allow to proceed
                'pattern_specific_risk': True,    # Allow to proceed
                'market_regime_appropriateness': True  # Allow to proceed
            }
        
        # Calculate overall pre-validation score
        passed_count = sum(1 for v in validations.values() if v)
        pre_validation_score = passed_count / len(validations)
        
        return {
            'passed': pre_validation_score >= 0.7,  # 70% of checks must pass
            'score': pre_validation_score,
            'details': validations,
            'volume_nodes': volume_nodes,
            'liquidity_pools': liquidity_pools
        }
    
    def _run_enhanced_path_validation(self, pattern: Dict, pre_validation: Dict) -> Dict:
        """Enhanced 6-path validation with pre-validation integration"""
        path_results = {}
        
        confidence = float(pattern.get('confidence', 0.0))
        expected_move = float(pattern.get('expected_move', 0.0))
        volume_ratio = float(pattern.get('volume_ratio', 1.0))
        
        # PATH 1: High Confidence (Enhanced)
        if (confidence >= 0.85 and 
            expected_move >= 0.003 and 
            volume_ratio >= 2.2 and
            pre_validation['details']['volume_profile_alignment'] and
            pre_validation['details']['ict_liquidity_alignment']):
            path_results['PATH_1'] = {
                'qualified': True,
                'score': self._calculate_path_score(pattern, 'PATH_1', pre_validation)
            }
        
        # PATH 2: Volume Spike (Enhanced)
        if (volume_ratio >= 2.2 and
            expected_move >= 0.0035 and
            confidence >= 0.85 and
            pre_validation['details']['volume_profile_alignment']):
            # Additional volume spike specific checks
            volume_nodes = pre_validation.get('volume_nodes', {})
            profile_strength = volume_nodes.get('profile_strength', 0)
            if profile_strength >= 0.3:
                path_results['PATH_2'] = {
                    'qualified': True,
                    'score': self._calculate_path_score(pattern, 'PATH_2', pre_validation)
                }
        
        # PATH 3: Sector-Specific (Enhanced)
        if self._check_sector_specific_path(pattern, pre_validation):
            path_results['PATH_3'] = {
                'qualified': True,
                'score': self._calculate_path_score(pattern, 'PATH_3', pre_validation)
            }
        
        # PATH 4: Balanced Play (Enhanced)
        if (confidence >= 0.80 and 
            volume_ratio >= 1.5 and 
            expected_move >= 0.0045 and
            self._check_risk_reward_ratio(pattern, 1.5)):
            path_results['PATH_4'] = {
                'qualified': True,
                'score': self._calculate_path_score(pattern, 'PATH_4', pre_validation)
            }
        
        # PATH 5: Proven Patterns (Enhanced)
        if self._check_proven_patterns_path(pattern, pre_validation):
            path_results['PATH_5'] = {
                'qualified': True,
                'score': self._calculate_path_score(pattern, 'PATH_5', pre_validation)
            }
        
        # PATH 6: Composite Score (Enhanced)
        composite_score = self._calculate_enhanced_composite_score(pattern, pre_validation)
        if composite_score >= 30:
            path_results['PATH_6'] = {
                'qualified': True,
                'score': composite_score
            }
        
        return path_results
    
    def _select_best_path(self, path_results: Dict) -> Optional[str]:
        """Select the best qualified path"""
        if not path_results:
            return None
        
        # Return the path with the highest score
        best_path = max(path_results.keys(), key=lambda p: path_results[p]['score'])
        return best_path
    
    def _apply_risk_adjustments(self, pattern: Dict, qualified_path: str, pre_validation: Dict) -> Dict:
        """Apply risk-adjusted position sizing and confidence"""
        risk_category = self._determine_risk_category(pattern)
        validation_score = pre_validation['score']
        
        # Risk-adjusted position sizing
        risk_multipliers = {
            'LOW_RISK': 1.0,
            'MEDIUM_RISK': 0.7,
            'HIGH_RISK': 0.4
        }
        
        position_multiplier = risk_multipliers.get(risk_category, 0.5)
        
        # Validation-adjusted confidence
        adjusted_confidence = pattern['confidence'] * validation_score
        
        return {
            **pattern,
            'risk_category': risk_category,
            'adjusted_confidence': adjusted_confidence,
            'position_size_multiplier': position_multiplier,
            'pre_validation_score': validation_score,
            'qualified_path': qualified_path
        }
    
    def _check_redis_deduplication(self, pattern: Dict) -> bool:
        """Check Redis deduplication"""
        try:
            if not self.redis_client:
                return True  # Allow if no Redis
            
            symbol = pattern.get('symbol')
            pattern_type = pattern.get('pattern')
            
            # Check if similar alert was sent recently
            dedup_key = f"alerts:dedup:{symbol}:{pattern_type}"
            if self.redis_client.exists(dedup_key):
                return False
            
            # Set deduplication key with TTL
            self.redis_client.setex(dedup_key, 300, "1")  # 5 minutes TTL
            return True
            
        except Exception as e:
            logger.debug(f"Deduplication check failed: {e}")
            return True  # Allow to proceed
    
    def _validate_enhanced_profitability(self, pattern: Dict, pre_validation: Dict) -> bool:
        """Enhanced profitability validation"""
        try:
            entry_price = float(pattern.get('entry_price', pattern.get('last_price', 0)))
            target_price = float(pattern.get('target', 0))
            stop_loss = float(pattern.get('stop_loss', 0))
            
            if entry_price <= 0 or target_price <= 0 or stop_loss <= 0:
                return False
            
            # Calculate minimum profit based on validation score
            min_profit_pct = 0.05 + (pre_validation['score'] * 0.05)  # 5-10% based on validation
            
            if target_price > entry_price:  # Long position
                profit_pct = (target_price - entry_price) / entry_price
            else:  # Short position
                profit_pct = (entry_price - target_price) / entry_price
            
            return profit_pct >= min_profit_pct
            
        except Exception:
            return False
    
    def _validate_market_regime(self, pattern: Dict) -> bool:
        """Validate market regime appropriateness"""
        try:
            vix_regime = get_vix_regime()
            pattern_type = pattern.get('pattern', '')
            
            # High-risk patterns should be avoided in panic regimes
            high_risk_patterns = ['hidden_accumulation', 'volume_price_divergence']
            if pattern_type in high_risk_patterns and vix_regime == 'PANIC':
                return False
            
            return True
            
        except Exception:
            return True  # Allow to proceed
    
    def _check_sector_specific_path(self, pattern: Dict, pre_validation: Dict) -> bool:
        """Check sector-specific path qualification"""
        symbol = pattern.get('symbol', '')
        confidence = float(pattern.get('confidence', 0.0))
        volume_ratio = float(pattern.get('volume_ratio', 1.0))
        
        # Sector-specific thresholds
        sector_thresholds = {
            'HDFCBANK': {'volume_spike': 2.8, 'confidence': 0.75},
            'ICICIBANK': {'volume_spike': 2.8, 'confidence': 0.75},
            'TCS': {'volume_spike': 2.2, 'confidence': 0.70},
            'INFY': {'volume_spike': 2.2, 'confidence': 0.70},
        }
        
        thresholds = sector_thresholds.get(symbol, {'volume_spike': 2.2, 'confidence': 0.70})
        
        return (volume_ratio >= thresholds['volume_spike'] and 
                confidence >= thresholds['confidence'])
    
    def _check_risk_reward_ratio(self, pattern: Dict, min_ratio: float) -> bool:
        """Check risk/reward ratio"""
        try:
            entry_price = float(pattern.get('entry_price', pattern.get('last_price', 0)))
            target_price = float(pattern.get('target', 0))
            stop_loss = float(pattern.get('stop_loss', 0))
            
            if entry_price <= 0 or target_price <= 0 or stop_loss <= 0:
                return False
            
            if target_price > entry_price:  # Long position
                reward = target_price - entry_price
                risk = entry_price - stop_loss
            else:  # Short position
                reward = entry_price - target_price
                risk = stop_loss - entry_price
            
            if risk <= 0:
                return False
            
            ratio = reward / risk
            return ratio >= min_ratio
            
        except Exception:
            return False
    
    def _check_proven_patterns_path(self, pattern: Dict, pre_validation: Dict) -> bool:
        """Check proven patterns path qualification"""
        pattern_type = pattern.get('pattern', '')
        confidence = float(pattern.get('confidence', 0.0))
        
        proven_patterns = [
            'spring_coil',
            'volume_price_divergence',
            'hidden_accumulation',
            'fake_bid_wall_basic',
            'fake_ask_wall_basic'
        ]
        
        return pattern_type in proven_patterns and confidence >= 0.70
    
    def _calculate_enhanced_composite_score(self, pattern: Dict, pre_validation: Dict) -> int:
        """Calculate enhanced composite score"""
        score = 0
        
        # Confidence score (0-40 points)
        confidence = float(pattern.get('confidence', 0.0))
        score += int(confidence * 40)
        
        # Volume ratio score (0-30 points)
        volume_ratio = float(pattern.get('volume_ratio', 1.0))
        if volume_ratio >= 3.0:
            score += 30
        elif volume_ratio >= 2.0:
            score += 20
        elif volume_ratio >= 1.5:
            score += 10
        
        # Expected move score (0-20 points)
        expected_move = float(pattern.get('expected_move', 0.0))
        if expected_move >= 1.0:
            score += 20
        elif expected_move >= 0.5:
            score += 15
        elif expected_move >= 0.25:
            score += 10
        
        # Pre-validation bonus (0-10 points)
        validation_score = pre_validation['score']
        score += int(validation_score * 10)
        
        return min(score, 100)
    
    def _calculate_path_score(self, pattern: Dict, path: str, pre_validation: Dict) -> int:
        """Calculate score for specific path"""
        base_score = self._calculate_enhanced_composite_score(pattern, pre_validation)
        
        # Path-specific bonuses
        path_bonuses = {
            'PATH_1': 10,  # High confidence bonus
            'PATH_2': 8,   # Volume spike bonus
            'PATH_3': 6,   # Sector-specific bonus
            'PATH_4': 7,   # Balanced play bonus
            'PATH_5': 5,   # Proven patterns bonus
            'PATH_6': 0,   # No bonus for composite
        }
        
        return base_score + path_bonuses.get(path, 0)
    
    def _determine_risk_category(self, pattern: Dict) -> str:
        """Determine risk category for pattern"""
        pattern_type = pattern.get('pattern', '')
        
        risk_categories = {
            'LOW_RISK': ['volume_breakout', 'breakout'],
            'MEDIUM_RISK': ['volume_spike', 'reversal'],
            'HIGH_RISK': ['hidden_accumulation', 'volume_price_divergence']
        }
        
        for category, patterns in risk_categories.items():
            if pattern_type in patterns:
                return category
        
        return 'HIGH_RISK'  # Default to high risk
    
    def _log_rejected_signal(self, pattern: Dict, reason: str, details: Dict):
        """Log rejected signal for analysis"""
        logger.debug(f"Signal rejected: {pattern.get('symbol', 'UNKNOWN')} - {reason}: {details}")


class AlertManager:
    """AlertManager wrapper class for compatibility with main.py"""

    def __init__(self, quant_calculator=None, telemetry_client=None, redis_client=None):
        """Initialize AlertManager with RetailAlertFilter and EnhancedAlertValidator"""
        self.redis_client = redis_client  # Store redis_client as instance variable
        self.transparency_bot = TransparencyBot()
        
        # Import filter from filters module
        try:
            from .filters import RetailAlertFilter
            self.filter = RetailAlertFilter(
                quant_calculator=quant_calculator,
                telemetry_client=telemetry_client,
                redis_client=redis_client,
            )
        except ImportError:
            logger.error("Could not import RetailAlertFilter from filters module")
            self.filter = None

        # Initialize Emergency Alert Filter
        self.emergency_filter = EmergencyAlertFilter()
        logger.info("✅ Emergency Alert Filter initialized")
        
        # Initialize Alert Grouper for throttling
        self.alert_grouper = AlertConflictResolver()
        logger.info("✅ Alert Grouper initialized for alert throttling")
        
        # Initialize Enhanced Alert Validator
        try:
            self.enhanced_validator = EnhancedAlertValidator(redis_client)
            logger.info("✅ Enhanced Alert Validator initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Enhanced Alert Validator: {e}")
            self.enhanced_validator = None

        self.historical_enabled = False  # DISABLED historical data collection to prevent confidence interference
        self.risk_manager = None  # Will be set by main scanner
        self.market_state = {
            "vpin": 0.0,
            "order_imbalance": 0.0,
            "breadth_ratio": 0.5,
            "sector_strength": {},
        }  # Track market state for evidence scoring
        self.high_confidence = 0.9


    def should_send_alert(self, alert_data):
        """Delegates to RetailAlertFilter.should_send_alert() from filters.py
        
        NOTE: This method should delegate to the filter. If filter is not available,
        use basic validation only.
        """
        # Delegate to RetailAlertFilter if available (proper filter helper)
        if self.filter and hasattr(self.filter, 'should_send_alert'):
            return self.filter.should_send_alert(alert_data)
        
        # Fallback: basic validation only
        if not alert_data or not isinstance(alert_data, dict):
            return False
        
        # Basic validation - let the proper filter handle the rest
        pattern = alert_data.get('pattern') or alert_data.get('pattern_type', '')
        confidence = alert_data.get('confidence', 0)
        
        try:
            confidence = float(confidence) if confidence else 0.0
        except (ValueError, TypeError):
            confidence = 0.0
        
        # Only allow critical patterns without proper filter
        critical_patterns = ['kow_signal_straddle', 'reversal', 'NEWS_ALERT', 'ict_killzone']
        if pattern in critical_patterns and confidence >= 0.70:
            return True
        
        logger.warning(f"⚠️ AlertManager.should_send_alert: Filter not available, rejecting {pattern}")
        return False

    def send_alert(self, pattern):
        """Send alert with enhanced validation and filtering."""
        try:
            logger.info(f"🔍 SEND_ALERT DEBUG: AlertManager.send_alert called for {pattern.get('symbol', 'Unknown') if pattern else 'None'}")
            
            # STEP 0: Alert Throttling Check (before all other checks)
            if hasattr(self, 'should_send_alert'):
                should_send = self.should_send_alert(pattern)
                logger.info(f"🔍 SEND_ALERT DEBUG: should_send_alert returned {should_send}")
                if not should_send:
                    logger.info(f"⏸️  THROTTLED: Alert {pattern.get('symbol', 'UNKNOWN')} {pattern.get('pattern', 'UNKNOWN')} rejected by throttling filter")
                    return False
            
            # STEP 1: Emergency Filter Check (before validation and filtering)
            if self.emergency_filter and self.emergency_filter.should_send_immediately(pattern):
                logger.info(f"🚨 EMERGENCY: Alert {pattern.get('symbol', 'UNKNOWN')} {pattern.get('pattern', 'UNKNOWN')} bypassing normal filters - sending immediately")
                # For emergency alerts, we still want to pass through validation, but skip normal filtering
                # Return True to indicate it should be sent (caller will handle actual sending)
                return True
            
            # STEP 2: Enhanced Validation (if available)
            if self.enhanced_validator:
                logger.info(f"🔍 SEND_ALERT DEBUG: Running enhanced validation for {pattern.get('symbol', 'UNKNOWN')}")
                validated_pattern = self.enhanced_validator.validate_enhanced_alert(pattern)
                if not validated_pattern:
                    logger.info(f"❌ Enhanced validation failed for {pattern.get('symbol', 'UNKNOWN')}")
                    return False
                
                # Use validated pattern for further processing
                pattern = validated_pattern
                logger.info(f"✅ Enhanced validation passed for {pattern.get('symbol', 'UNKNOWN')} - Path: {pattern.get('qualified_path', 'UNKNOWN')}")
            
            # STEP 3: Traditional Filter (fallback)
            if self.filter:
                logger.info(f"🔍 SEND_ALERT DEBUG: Running traditional filter for {pattern.get('symbol', 'UNKNOWN')}")
                filter_result = self.filter.should_send_alert(pattern)
                logger.info(f"🔍 SEND_ALERT DEBUG: Traditional filter returned {filter_result}")
                return filter_result
            else:
                logger.error("No filter available for alert processing")
                return False
                
        except Exception as e:
            logger.error(f"Error in send_alert: {e}")
            return False


class EnhancedAlertManager(AlertManager):
    """Alert manager with conflict resolution and pattern-specific thresholds."""
    
    def __init__(self, *args, **kwargs):
        """Initialize with indicator caching support."""
        super().__init__(*args, **kwargs)
        
        # ✅ SMART CACHING: In-memory indicator cache matching HybridCalculations
        self._indicator_cache = {}  # {symbol: {indicator_name: {timestamp, value}}}
        self._cache_ttl = 300  # 5 minutes TTL (matches HybridCalculations)
        self._cache_lock = threading.Lock()
        self._cache_max_size = 1000  # Max cache entries
        self._cache_hits = 0
        self._cache_misses = 0

    INDICATOR_FIELDS: List[str] = [
        "rsi",
        "macd",
        "ema_5",
        "ema_10", 
        "ema_20",
        "ema_50",
        "ema_100",
        "ema_200",
        "ema_crossover",
        "atr",
        "vwap",
        "bollinger_bands",
        "volume_profile",
        "volume_ratio",
        "price_change",
        "bucket_incremental_volume",
        # Canonical field names from optimized_field_mapping.yaml
        "zerodha_cumulative_volume",
        "bucket_cumulative_volume", 
        "zerodha_last_traded_quantity",
        "momentum",
        "volatility",
        "support",
        "resistance",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "delta",
        "gamma",
        "theta",
        "vega",
        "iv",
        "oi_change",
        "oi_spread",
        "dte",
        "days_to_expiry",
        "vix_value",
    ]

    NUMERIC_INDICATOR_FIELDS = {
        "rsi",
        "macd",
        "ema_5",
        "ema_10",
        "ema_20",
        "ema_50",
        "ema_100",
        "ema_200",
        "atr",
        "vwap",
        "bollinger_bands",
        "volume_profile",
        "volume_ratio",
        "price_change",
        "bucket_incremental_volume",
        # Canonical field names from optimized_field_mapping.yaml
        "zerodha_cumulative_volume",
        "bucket_cumulative_volume", 
        "bucket_incremental_volume",
        "zerodha_last_traded_quantity",
        # Legacy aliases for backward compatibility
        "bucket_incremental_volume",
        "bucket_cumulative_volume",
        "price_change",
        "momentum",
        "volatility",
        "support",
        "resistance",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "delta",
        "gamma",
        "theta",
        "vega",
        "iv",
        "oi_change",
        "oi_spread",
        "dte",
        "days_to_expiry",
        "vix_value",
    }

    @staticmethod
    def _has_meaningful_value(value: Any) -> bool:
        """Determine if a value is present and usable."""
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_alert_thresholds()
        self.redis_calculations = None
        if self.redis_client and RedisCalculations:
            try:
                self.redis_calculations = RedisCalculations(self.redis_client)
            except Exception as e:
                logger.debug(f"⚠️ Failed to initialize RedisCalculations: {e}")
    
    def _load_alert_thresholds(self):
        """Load alert thresholds from config/thresholds.py (single source of truth)."""
        try:
            from config.thresholds import get_confidence_threshold, BASE_THRESHOLDS
            
            # Load confidence thresholds from centralized config
            self.minimum_confidence = BASE_THRESHOLDS.get('min_confidence', 0.70)
            self.high_confidence = BASE_THRESHOLDS.get('high_confidence', 0.85)
            
            # Load pattern-specific thresholds using the actual patterns defined in thresholds.py
            # 8 Core Patterns from BASE_THRESHOLDS
            core_patterns = [
                'volume_spike', 'volume_breakout', 'volume_price_divergence',
                'upside_momentum', 'downside_momentum', 'breakout', 
                'reversal', 'hidden_accumulation'
            ]
            
            # ICT Patterns from BASE_THRESHOLDS
            ict_patterns = [
                'ict_liquidity_pools', 'ict_fair_value_gaps', 'ict_optimal_trade_entry',
                'ict_premium_discount', 'ict_killzone', 'ict_momentum'
            ]
            
            # Straddle Strategy Patterns from BASE_THRESHOLDS
            straddle_patterns = [
                'iv_crush_play_straddle', 'range_bound_strangle', 'market_maker_trap_detection',
                'premium_collection_strategy', 'kow_signal_straddle'
            ]
            
            self.PATTERN_CONFIDENCE_THRESHOLDS = {}
            
            # Set confidence thresholds for core patterns using get_confidence_threshold
            for pattern in core_patterns:
                self.PATTERN_CONFIDENCE_THRESHOLDS[pattern] = get_confidence_threshold(pattern)
            
            # Set confidence thresholds for ICT patterns using their specific confidence values
            ict_confidence_mapping = {
                'ict_liquidity_pools': 'ict_liquidity_confidence',
                'ict_fair_value_gaps': 'ict_fvg_confidence', 
                'ict_optimal_trade_entry': 'ict_ote_confidence',
                'ict_premium_discount': 'ict_premium_discount_confidence',
                'ict_killzone': 'ict_killzone_confidence',
                'ict_momentum': 'ict_momentum_confidence'
            }
            
            for pattern in ict_patterns:
                confidence_key = ict_confidence_mapping.get(pattern, 'ict_momentum_confidence')
                self.PATTERN_CONFIDENCE_THRESHOLDS[pattern] = BASE_THRESHOLDS.get(confidence_key, 0.70)
            
            # Set confidence thresholds for straddle patterns using straddle_entry_confidence
            for pattern in straddle_patterns:
                self.PATTERN_CONFIDENCE_THRESHOLDS[pattern] = BASE_THRESHOLDS.get('straddle_entry_confidence', 0.85)
            
            # Set unknown pattern confidence
            self.PATTERN_CONFIDENCE_THRESHOLDS['unknown'] = self.high_confidence
            
            logger.info(f"✅ Loaded alert thresholds from config/thresholds.py: min={self.minimum_confidence}, high={self.high_confidence}")
            logger.info(f"✅ Loaded {len(self.PATTERN_CONFIDENCE_THRESHOLDS)} pattern confidence thresholds")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to load alert thresholds from config/thresholds.py, using defaults: {e}")
            # Fallback to hardcoded values for 8 core patterns
            self.minimum_confidence = 0.70
            self.high_confidence = 0.85
            self.PATTERN_CONFIDENCE_THRESHOLDS = {
                'volume_spike': 0.7,
                'volume_breakout': 0.7,
                'volume_price_divergence': 0.7,
                'upside_momentum': 0.7,
                'downside_momentum': 0.7,
                'breakout': 0.7,
                'reversal': 0.7,
                'hidden_accumulation': 0.7,
                'unknown': 0.85,  # 85% confidence required for unknown patterns
            }
    
    def __init__(self, quant_calculator=None, telemetry_client=None, redis_client=None, config=None):
        # Initialize base AlertManager with proper parameters
        super().__init__(quant_calculator=quant_calculator, telemetry_client=telemetry_client, redis_client=redis_client)
        
        # Initialize enhanced features
        self.config = config or {}
        self.conflict_resolver = AlertConflictResolver()
        
        # Load alert thresholds
        self._load_alert_thresholds()
        
        # Enhanced validator is already initialized in base class
        logger.info("✅ Enhanced Alert Manager initialized with Enhanced Validator")
        
        # Cooldown removed
    
    # Cooldown methods removed
        
    def process_alerts(self, alerts):
        """Process alerts with comprehensive filtering and conflict resolution."""
        if not alerts:
            return []
        
        logging.info(f"🔍 [ALERT_PROCESSING] Processing {len(alerts)} raw alerts")
        
        # Step 1: Apply confidence thresholds
        confidence_filtered = self._apply_confidence_thresholds(alerts)
        
        # Step 2: Apply quality filter (with very permissive threshold)
        quality_filtered = self._apply_quality_filter(confidence_filtered)
        
        # Step 3: Resolve conflicts
        resolved_alerts = self.conflict_resolver.resolve_conflicts(quality_filtered)
        
        return resolved_alerts
    
    def _fetch_indicators_from_redis(self, symbol: str, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Compose consolidated indicator payload using alert data with Redis fallback."""
        try:
            indicators: Dict[str, Any] = {}

            # Start with indicators already attached to the pattern payload
            base_indicators = alert_data.get("indicators") or {}
            if isinstance(base_indicators, dict):
                for key, value in base_indicators.items():
                    if self._has_meaningful_value(value):
                        indicators[key] = value

            # Merge top-level indicator hints from the alert payload
            for key in self.INDICATOR_FIELDS:
                if key in alert_data and self._has_meaningful_value(alert_data[key]):
                    current_value = indicators.get(key)
                    if not self._has_meaningful_value(current_value):
                        indicators[key] = alert_data[key]

            # Preserve the latest known last_price for downstream consumers
            for price_key in ("last_price", "last_price", "last_price"):
                if self._has_meaningful_value(alert_data.get(price_key)):
                    indicators.setdefault("last_price", alert_data.get(price_key))
                    break

            used_fallback = False
            core_fields = ("rsi", "atr", "volume_ratio", "price_change")
            missing_core = [
                field for field in core_fields if not self._has_meaningful_value(indicators.get(field))
            ]

            if missing_core and self.redis_client:
                try:
                    fallback = self._fetch_basic_indicators_from_redis(symbol)
                except Exception as fallback_error:
                    logger.debug(f"Redis indicator fallback failed for {symbol}: {fallback_error}")
                    fallback = {}

                for key, value in fallback.items():
                    if not self._has_meaningful_value(indicators.get(key)) and self._has_meaningful_value(value):
                        indicators[key] = value
                        used_fallback = True

            # Normalise numeric indicator values where possible
            for key in list(indicators.keys()):
                if key in self.NUMERIC_INDICATOR_FIELDS:
                    try:
                        indicators[key] = float(indicators[key])
                    except (TypeError, ValueError):
                        continue

            if indicators:
                indicators["indicators_source"] = (
                    "pattern_with_redis_fallback" if used_fallback else "pattern_payload"
                )

            return indicators

        except Exception as e:
            logger.error(f"❌ [INDICATOR_COMPOSE] Error for {symbol}: {e}")
            existing = alert_data.get("indicators") or {}
            return existing if isinstance(existing, dict) else {}
    
    def _get_cached_indicator(self, symbol: str, indicator_name: str) -> Optional[Any]:
        """Get cached indicator if available and not expired."""
        with self._cache_lock:
            if symbol in self._indicator_cache:
                symbol_cache = self._indicator_cache[symbol]
                if indicator_name in symbol_cache:
                    cached = symbol_cache[indicator_name]
                    if time.time() - cached['timestamp'] < self._cache_ttl:
                        self._cache_hits += 1
                        return cached['value']
            self._cache_misses += 1
            return None
    
    def _cache_indicator(self, symbol: str, indicator_name: str, value: Any):
        """Cache indicator value with timestamp."""
        with self._cache_lock:
            # Enforce cache size bounds
            if len(self._indicator_cache) >= self._cache_max_size:
                # Remove oldest entry
                oldest_symbol = min(self._indicator_cache.keys(), 
                                  key=lambda k: min(
                                      [x.get('timestamp', 0) if isinstance(x, dict) else 0 
                                       for x in (self._indicator_cache.get(k).values() 
                                                if isinstance(self._indicator_cache.get(k), dict) else [])] or [0]
                                  ))
                del self._indicator_cache[oldest_symbol]
            
            if symbol not in self._indicator_cache:
                self._indicator_cache[symbol] = {}
            
            self._indicator_cache[symbol][indicator_name] = {
                'timestamp': time.time(),
                'value': value
            }
    
    def _fetch_basic_indicators_from_redis(self, symbol: str) -> Dict[str, Any]:
        """
        ✅ SMART CACHING: Fetch indicators from Redis with in-memory cache.
        Checks cache first, then Redis (DB 5 → DB 1 → DB 4), then caches result.
        
        Redis Storage Schema:
        - DB 5 (primary per user): indicators:{symbol}:{indicator_name} via analysis_cache data type
        - DB 1 (realtime): indicators:{symbol}:{indicator_name} via analysis_cache data type (fallback)
        - DB 4 (fallback): Additional indicator storage
        - Format: Simple indicators (RSI, EMA, ATR, VWAP) stored as string/number
                  Complex indicators (MACD, BB) stored as JSON with {'value': {...}, 'timestamp': ..., 'symbol': ...}
        """
        indicators = {}
        if not self.redis_client:
            return indicators
        
        try:
            # ✅ SMART CACHING: Check cache first for each indicator
            indicator_names = [
                'rsi', 'atr', 'vwap', 'volume_ratio', 'price_change',
                'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                'macd', 'bollinger_bands', 'volume_profile',
            ]
            
            # Try cache first
            cached_indicators = {}
            for indicator_name in indicator_names:
                cached_value = self._get_cached_indicator(symbol, indicator_name)
                if cached_value is not None:
                    cached_indicators[indicator_name] = cached_value
            
            # If all indicators found in cache, return immediately
            if len(cached_indicators) == len(indicator_names):
                return cached_indicators
            
            # Get Redis clients for DB 5 (primary), DB 1 (fallback), DB 4 (fallback)
            redis_db5 = self.redis_client.get_client(5) if hasattr(self.redis_client, 'get_client') else None
            redis_db1 = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') else self.redis_client
            redis_db4 = self.redis_client.get_client(4) if hasattr(self.redis_client, 'get_client') else None
            
            # ✅ PRIORITY: Check DB 5 first (user confirmed indicators are in DB 5)
            redis_clients = []
            if redis_db5:
                redis_clients.append(('DB5', redis_db5))
            redis_clients.append(('DB1', redis_db1))
            if redis_db4:
                redis_clients.append(('DB4', redis_db4))
            
            # Normalize symbol for Redis key lookup (same variants as dashboard)
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol and ('NIFTY' in symbol.upper() or 'BANKNIFTY' in symbol.upper()) else symbol,
                f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # Merge cached indicators into result
            indicators.update(cached_indicators)
            
            # Fetch missing indicators from Redis (check DB 5 → DB 1 → DB 4)
            missing_indicator_names = [name for name in indicator_names if name not in cached_indicators]
            
            for variant in symbol_variants:
                found_any = False
                for indicator_name in missing_indicator_names:
                    redis_key = f"indicators:{variant}:{indicator_name}"
                    
                    # Try each Redis DB in priority order (DB 5 → DB 1 → DB 4)
                    value = None
                    for db_name, redis_client in redis_clients:
                        try:
                            # Try retrieve_by_data_type first (matches scanner storage)
                            if hasattr(redis_client, 'retrieve_by_data_type'):
                                try:
                                    value = redis_client.retrieve_by_data_type(redis_key, "analysis_cache")
                                    if value:
                                        break
                                except Exception:
                                    pass
                            
                            # Fallback to direct GET
                            if not value:
                                try:
                                    value = redis_client.get(redis_key)
                                    if value:
                                        break
                                except Exception:
                                    continue
                        except Exception as e:
                            logger.debug(f"Error checking {db_name} for {redis_key}: {e}")
                            continue
                    
                    if value:
                        try:
                            # Handle bytes response
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            
                            # Try parsing as JSON first (for complex indicators)
                            try:
                                parsed = json.loads(value)
                                if isinstance(parsed, dict):
                                    # Check for nested structure with 'value' field (from redis_storage)
                                    if 'value' in parsed:
                                        # For MACD, extract the macd value from the dict
                                        if indicator_name == 'macd' and isinstance(parsed['value'], dict):
                                            indicator_value = parsed['value'].get('macd') or parsed['value']
                                        else:
                                            indicator_value = parsed['value']
                                    else:
                                        indicator_value = parsed
                                else:
                                    indicator_value = parsed
                                
                                indicators[indicator_name] = indicator_value
                                # ✅ SMART CACHING: Cache the fetched indicator
                                self._cache_indicator(symbol, indicator_name, indicator_value)
                                found_any = True
                            except (json.JSONDecodeError, TypeError):
                                # Simple numeric/string value (RSI, EMA, ATR, VWAP, etc.)
                                try:
                                    indicator_value = float(value)
                                except (ValueError, TypeError):
                                    indicator_value = value
                                
                                indicators[indicator_name] = indicator_value
                                # ✅ SMART CACHING: Cache the fetched indicator
                                self._cache_indicator(symbol, indicator_name, indicator_value)
                                found_any = True
                        except Exception as e:
                            logger.debug(f"Error parsing indicator {indicator_name} for {variant}: {e}")
                
                if found_any:
                    break  # Found indicators for this variant
            
            # Merge cached indicators back in (in case we missed some)
            indicators.update(cached_indicators)
            
            if indicators:
                logger.debug(f"✅ Loaded {len(indicators)} indicators from Redis for {symbol}: {list(indicators.keys())[:5]}")
            else:
                logger.debug(f"⚠️ No indicators found in Redis for {symbol}")
        
        except Exception as e:
            logger.debug(f"Error fetching indicators from Redis for {symbol}: {e}")
        
        return indicators
    
    def _fetch_greeks_from_redis(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch Options Greeks from Redis using the same logic as dashboard.
        This ensures alert payloads are enriched with real-time Greeks for option symbols.
        
        Redis Storage Schema:
        - DB 1 (realtime): indicators:{symbol}:greeks (combined dict) and indicators:{symbol}:{greek} (individual)
        - Format: Combined stored as JSON with {'value': {'delta': ..., 'gamma': ..., ...}, 'timestamp': ..., 'symbol': ...}
                  Individual Greeks stored as string/number
        - Calculated by: EnhancedGreekCalculator.black_scholes_greeks() using scipy.stats.norm (pure Python)
        """
        greeks = {}
        if not self.redis_client:
            return greeks
        
        try:
            # Get Redis client for DB 1 (realtime)
            redis_client = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') else self.redis_client
            
            # Normalize symbol for Redis key lookup (same variants as dashboard)
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol and ('NIFTY' in symbol.upper() or 'BANKNIFTY' in symbol.upper()) else symbol,
                f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # Greek fields to fetch
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
            
            # Try each symbol variant
            for variant in symbol_variants:
                # Try combined greeks first (preferred format)
                greeks_key = f"indicators:{variant}:greeks"
                try:
                    greeks_data = redis_client.get(greeks_key)
                    if greeks_data:
                        if isinstance(greeks_data, bytes):
                            greeks_data = greeks_data.decode('utf-8')
                        
                        # Parse JSON if string
                        parsed = json.loads(greeks_data) if isinstance(greeks_data, str) else greeks_data
                        
                        if isinstance(parsed, dict):
                            # Extract from nested 'value' field (from redis_storage format)
                            if 'value' in parsed and isinstance(parsed['value'], dict):
                                greeks.update(parsed['value'])
                            else:
                                # Direct greeks dict
                                greeks.update(parsed)
                            break  # Found combined greeks, no need to try individual
                except Exception as e:
                    logger.debug(f"Error getting combined Greeks for {variant}: {e}")
                
                # Try individual Greeks if combined not found
                found_any = False
                for greek_name in greek_names:
                    greek_key = f"indicators:{variant}:{greek_name}"
                    try:
                        greek_value = redis_client.get(greek_key)
                        if greek_value:
                            if isinstance(greek_value, bytes):
                                greek_value = greek_value.decode('utf-8')
                            
                            # Try to parse as float
                            try:
                                greeks[greek_name] = float(greek_value)
                            except (ValueError, TypeError):
                                # If not a number, try JSON parsing
                                try:
                                    parsed = json.loads(greek_value) if isinstance(greek_value, str) else greek_value
                                    if isinstance(parsed, dict) and 'value' in parsed:
                                        greeks[greek_name] = parsed['value']
                                    else:
                                        greeks[greek_name] = parsed
                                except (json.JSONDecodeError, TypeError):
                                    greeks[greek_name] = greek_value
                            found_any = True
                    except Exception as e:
                        logger.debug(f"Error getting {greek_name} for {variant}: {e}")
                
                if found_any or greeks:
                    break  # Found Greeks for this variant
            
            if greeks:
                logger.debug(f"✅ Loaded Greeks from Redis for {symbol}: {list(greeks.keys())}")
            else:
                logger.debug(f"⚠️ No Greeks found in Redis for {symbol}")
        
        except Exception as e:
            logger.debug(f"Error fetching Greeks from Redis for {symbol}: {e}")
        
        return greeks
    
    def _attach_market_context(self, alert_data: Dict[str, Any]) -> None:
        """Include VIX context in alert data for downstream consumers."""
        if not isinstance(alert_data, dict):
            return

        try:
            vix_value = alert_data.get("vix_value")
            if vix_value is None:
                session_vix = self.redis_client.get_index("indiavix") if hasattr(self.redis_client, "get_index") else None
                if isinstance(session_vix, dict) and session_vix.get("last_price"):
                    vix_value = float(session_vix.get("last_price", 0.0))
                else:
                    vix_value = get_current_vix() if callable(get_current_vix) else None

            if vix_value is not None:
                try:
                    vix_float = float(vix_value)
                except (TypeError, ValueError):
                    vix_float = None
                if vix_float is not None and vix_float > 0:
                    alert_data["vix_value"] = vix_float
        except Exception:
            pass

    def _ensure_news_context(self, alert_data: Dict[str, Any]) -> None:
        """Attach latest news metadata to the alert payload."""
        if not isinstance(alert_data, dict):
            return

        existing_news = any(
            self._has_meaningful_value(alert_data.get(field))
            for field in ("news_title", "news_summary", "news_context", "news_sentiment")
        )

        if not existing_news:
            news_payload = self._fetch_latest_news(alert_data.get("symbol"))
            if news_payload:
                alert_data.update(news_payload)
                existing_news = True

        if existing_news:
            news_context = {}
            current_context = alert_data.get("news_context")
            if isinstance(current_context, dict):
                news_context.update(current_context)

            mapping = {
                "title": alert_data.get("news_title"),
                "source": alert_data.get("news_source"),
                "link": alert_data.get("news_link"),
                "sentiment": alert_data.get("news_sentiment_label"),
                "sentiment_score": alert_data.get("news_sentiment"),
                "impact": alert_data.get("news_impact"),
                "summary": alert_data.get("news_summary"),
                "timestamp": alert_data.get("news_timestamp"),
                "freshness": alert_data.get("news_freshness"),
            }

            for key, value in mapping.items():
                if self._has_meaningful_value(value) and key not in news_context:
                    news_context[key] = value

            if news_context:
                alert_data["news_context"] = news_context
                alert_data["has_news"] = True
                alert_data.setdefault("news_sentiment_label", news_context.get("sentiment"))

    def _fetch_latest_news(self, symbol: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch the most relevant recent news item for a symbol from Redis."""
        if not symbol or not self.redis_client:
            return None

        now_ts = time.time()
        keys_to_try = self._news_keys_for_symbol(symbol)
        best_payload: Optional[Dict[str, Any]] = None
        best_score = -1.0

        try:
            news_db = self.redis_client.get_database_for_data_type("news_data")
        except Exception:
            news_db = 11

        news_client = self.redis_client.get_client(news_db) if self.redis_client else None

        for redis_key in keys_to_try:
            try:
                if not news_client:
                    raise RuntimeError("No Redis client available for news lookups")

                items = news_client.zrevrangebyscore(
                    redis_key,
                    "+inf",
                    now_ts - 3600 * 3,  # last 3 hours
                    start=0,
                    num=5,
                )
            except Exception as exc:
                logger.debug(f"News lookup failed for {symbol} ({redis_key}): {exc}")
                continue

            if not items:
                continue

            for raw_item in items:
                try:
                    payload_str = raw_item.decode("utf-8") if isinstance(raw_item, (bytes, bytearray)) else raw_item
                    payload = json.loads(payload_str)
                except Exception:
                    continue

                news_data = payload.get("data")
                if not isinstance(news_data, dict):
                    news_data = payload

                sentiment_score = news_data.get("sentiment_score")
                sentiment_label = news_data.get("sentiment")

                score_value: float
                try:
                    score_value = float(sentiment_score)
                except (TypeError, ValueError):
                    sentiment_lower = (sentiment_label or "").lower()
                    if sentiment_lower == "positive":
                        score_value = 1.0
                    elif sentiment_lower == "negative":
                        score_value = -1.0
                    else:
                        score_value = 0.0

                impact = news_data.get("market_impact") or news_data.get("impact") or "MEDIUM"
                impact_weight = 1.5 if str(impact).upper() == "HIGH" else 1.2 if str(impact).upper() == "MEDIUM" else 1.0
                combined_score = abs(score_value) * impact_weight

                if combined_score >= best_score:
                    best_score = combined_score
                    timestamp = (
                        news_data.get("written_at")
                        or news_data.get("published_at")
                        or news_data.get("timestamp")
                        or payload.get("timestamp")
                    )

                    best_payload = {
                        "news_title": news_data.get("title") or payload.get("title"),
                        "news_source": news_data.get("publisher")
                        or news_data.get("source")
                        or payload.get("source"),
                        "news_link": news_data.get("link") or payload.get("link"),
                        "news_sentiment": score_value,
                        "news_sentiment_label": sentiment_label,
                        "news_impact": impact,
                        "news_summary": news_data.get("summary")
                        or news_data.get("description")
                        or news_data.get("title"),
                        "news_timestamp": timestamp,
                        "news_context": news_data,
                        "has_news": True,
                    }

                    freshness = self._derive_news_freshness(timestamp)
                    if freshness:
                        best_payload["news_freshness"] = freshness

        return best_payload

    def _news_keys_for_symbol(self, symbol: Optional[str]) -> List[str]:
        """Generate Redis news lookup keys for a given symbol."""
        if not symbol:
            return []

        keys: List[str] = [f"news:symbol:{symbol}"]

        if ":" in symbol:
            exchange, name = symbol.split(":", 1)
            keys.append(f"news:symbol:{exchange}:{name}")

            base_name = name
            base_name = re.sub(r"\\d{1,2}[A-Z]{3}\\d{2}", "", base_name)  # remove expiry like 28OCT25
            base_name = base_name.replace("FUT", "").replace("OPT", "").replace("CE", "").replace("PE", "")
            base_name = base_name.replace("-", "").replace("_", "")
            base_name = base_name.strip()

            if base_name:
                keys.append(f"news:symbol:{base_name}")
                keys.append(f"news:symbol:{exchange}:{base_name}")

            name_upper = name.upper()
            if any(bank in name_upper for bank in ("BANK", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "AXISBANK", "SBIN", "INDUSINDBK")):
                keys.append("news:symbol:NSE:NIFTY BANK")

            if any(stock in name_upper for stock in self.NIFTY50_SYMBOLS):
                keys.append("news:symbol:NSE:NIFTY 50")

        # Remove duplicates while preserving order
        seen = set()
        deduped_keys = []
        for key in keys:
            if key not in seen:
                seen.add(key)
                deduped_keys.append(key)

        return deduped_keys

    def _derive_news_freshness(self, timestamp_value: Optional[str]) -> Optional[str]:
        """Return qualitative freshness bucket for a news timestamp."""
        if not timestamp_value:
            return None

        timestamp = self._parse_news_timestamp(timestamp_value)
        if not timestamp:
            return None

        age_minutes = (datetime.now(timezone.utc) - timestamp).total_seconds() / 60.0
        if age_minutes <= 60:
            return "RECENT"
        if age_minutes <= 180:
            return "STALE"
        return "OLD"

    def _parse_news_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        """Parse ISO-like timestamp strings into aware UTC datetimes."""
        if not value:
            return None

        candidates = [value, value.replace("Z", "+00:00")]
        for candidate in candidates:
            try:
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        return None
    
    def _get_price_data_from_redis(self, symbol: str) -> List[float]:
        """Get last_price data from Redis for calculations."""
        try:
            # Try to get last_price history from Redis
            price_key = f"price_history:{symbol}"
            price_data = self.redis_client.get(price_key)
            if price_data:
                import json
                return json.loads(price_data)
            
            # Fallback: get from session data
            session_key = f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = self.redis_client.get(session_key)
            if session_data:
                import json
                data = json.loads(session_data)
                # Extract prices from time buckets
                prices = []
                time_buckets = data.get('time_buckets', {})
                for bucket_data in time_buckets.values():
                    if 'close' in bucket_data:
                        prices.append(bucket_data['close'])
                return prices[-50:] if prices else []  # Last 50 prices
            
            return []
        except Exception as e:
            logger.debug(f"Error getting last_price data for {symbol}: {e}")
            return []
    
    def _get_ohlc_data_from_redis(self, symbol: str) -> List[Dict]:
        """Get OHLC data from Redis for ATR calculation."""
        try:
            symbol_key = normalize_symbol(symbol)
            zset_key = ohlc_daily_zset(symbol_key)
            entries = self.redis_client.zrevrange(zset_key, 0, 19)
        except Exception as e:
            logger.debug(f"Error getting OHLC data for {symbol}: {e}")
            return []

        if not entries:
            return []

        ohlc_data: List[Dict[str, Any]] = []
        for entry in reversed(entries):
            if isinstance(entry, bytes):
                entry = entry.decode("utf-8")
            try:
                payload = json.loads(entry)
            except Exception:
                continue
            ohlc_data.append(
                {
                    "date": payload.get("d"),
                    "open": payload.get("o") or payload.get("open"),
                    "high": payload.get("h") or payload.get("high"),
                    "low": payload.get("l") or payload.get("low"),
                    "close": payload.get("c") or payload.get("close"),
                    "bucket_incremental_volume": payload.get("v") or payload.get("bucket_incremental_volume"),
                }
            )
        return ohlc_data
    
    def _get_volume_data_from_redis(self, symbol: str) -> List[Dict]:
        """Get bucket_incremental_volume data from Redis for VWAP calculation."""
        try:
            # Get bucket_incremental_volume data from session
            session_key = f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = self.redis_client.get(session_key)
            if session_data:
                import json
                data = json.loads(session_data)
                volume_data = []
                time_buckets = data.get('time_buckets', {})
                for bucket_data in time_buckets.values():
                    if 'bucket_incremental_volume' in bucket_data and 'close' in bucket_data:
                        volume_data.append({
                            'bucket_incremental_volume': bucket_data['bucket_incremental_volume'],
                            'last_price': bucket_data['close']
                        })
                return volume_data[-20:] if volume_data else []  # Last 20 buckets
            return []
        except Exception as e:
            return []
    
    
    
    def _apply_confidence_thresholds(self, alerts):
        """Apply confidence gating and enrich alert payloads."""
        filtered = []
        for alert in alerts:
            pattern_value = alert.get('pattern') or alert.get('pattern_type') or 'unknown'
            alert['pattern'] = pattern_value
            confidence = alert.get('confidence', 0.0)
            symbol = alert.get('symbol', '')
            
            # Use centralized confidence threshold
            try:
                if THRESHOLD_CONFIG_AVAILABLE:
                    min_confidence = get_confidence_threshold(pattern_value, 'NORMAL')
                else:
                    min_confidence = 0.90  # Fallback
            except Exception:
                min_confidence = 0.90  # Fallback
            
            if confidence >= min_confidence:
                indicator_snapshot = self._fetch_indicators_from_redis(symbol, alert)
                if indicator_snapshot:
                    existing_indicators = alert.get('indicators') if isinstance(alert.get('indicators'), dict) else {}
                    merged_indicators = {**existing_indicators, **indicator_snapshot}
                    alert['indicators'] = merged_indicators

                    for key in ("rsi", "atr", "volume_ratio", "price_change", "ema_20", "ema_50", "vwap"):
                        if key in merged_indicators and not self._has_meaningful_value(alert.get(key)):
                            alert[key] = merged_indicators[key]
                
                # Fetch Greeks for option symbols
                is_option = any(x in symbol.upper() for x in ['CE', 'PE']) or 'NFO:' in symbol.upper()
                if is_option:
                    greeks_snapshot = self._fetch_greeks_from_redis(symbol)
                    if greeks_snapshot:
                        existing_greeks = alert.get('greeks') if isinstance(alert.get('greeks'), dict) else {}
                        merged_greeks = {**existing_greeks, **greeks_snapshot}
                        alert['greeks'] = merged_greeks
                        
                        # Also add individual Greek fields to top level for dashboard compatibility
                        for key in ("delta", "gamma", "theta", "vega", "rho"):
                            if key in merged_greeks and not self._has_meaningful_value(alert.get(key)):
                                alert[key] = merged_greeks[key]

                self._ensure_pattern_metadata(alert)
                self._ensure_news_context(alert)
                self._attach_market_context(alert)

                filtered.append(alert)
                logger.info(f"✅ Alert approved: {pattern_value} confidence {confidence:.2f} >= {min_confidence:.2f}")
            else:
                logger.debug(f"❌ Alert filtered out: {pattern_value} confidence {confidence:.2f} < {min_confidence:.2f}")

        return filtered

    def _apply_quality_filter(self, alerts):
        """Apply quality filtering based on bucket_incremental_volume and move thresholds using centralized config."""
        filtered = []
        for alert in alerts:
            volume_ratio = alert.get('volume_ratio', 1.0)
            expected_move = alert.get('expected_move', 0.0)
            pattern = alert.get('pattern', 'unknown')
            
            # Use centralized bucket_incremental_volume threshold
            try:
                if THRESHOLD_CONFIG_AVAILABLE:
                    volume_threshold = get_volume_threshold(pattern, None, 'NORMAL')
                else:
                    volume_threshold = 1.0  # Fallback
            except Exception:
                volume_threshold = 1.0  # Fallback
            
            # Use centralized move threshold
            try:
                if THRESHOLD_CONFIG_AVAILABLE:
                    move_threshold = get_move_threshold(pattern, 'NORMAL')
                else:
                    move_threshold = 0.15  # Fallback
            except Exception:
                move_threshold = 0.15  # Fallback
            
            if volume_ratio >= volume_threshold and expected_move >= move_threshold:
                filtered.append(alert)
            else:
                logger.debug(f"Alert filtered out: volume_ratio {volume_ratio:.2f} < {volume_threshold:.2f}, expected_move {expected_move:.2f} < {move_threshold:.2f}")
        
        return filtered
    
    # Cooldown methods removed


class ProductionAlertManager(EnhancedAlertManager):
    """Production-ready alert manager with full functionality."""
    
    def __init__(self, quant_calculator=None, telemetry_client=None, redis_client=None, config=None):
        super().__init__(quant_calculator, telemetry_client, redis_client, config)
        
        # Production-specific settings
        self.production_mode = True
        self.alert_history = []
        self.max_history_size = 1000
        
        # Alert grouping is integrated into enhanced_validator (inherited from base classes)
        # Use enhanced_validator.add_alert_to_group() for grouping functionality
        
        # Initialize Risk Manager
        try:
            from alerts.risk_manager import RiskManager
            self.risk_manager = RiskManager(config=config, redis_client=redis_client)
            logger.info("✅ Risk Manager initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Risk Manager: {e}")
            self.risk_manager = None
        
        # Initialize notifiers
        try:
            from alerts.notifiers import TelegramNotifier, RedisNotifier, MacOSNotifier
            self.telegram_notifier = TelegramNotifier()
            self.redis_notifier = RedisNotifier(redis_client)
            self.macos_notifier = MacOSNotifier()
            
            # Store reference to scanner instance for indicator access
            self.scanner_instance = None
        except ImportError as e:
            logger.warning(f"Could not import notifiers: {e}")
            self.telegram_notifier = None
            self.redis_notifier = None
            self.macos_notifier = None
            self.scanner_instance = None

    def get_config(self) -> Dict[str, Any]:
        """Returns the configuration of the alert manager."""
        return self.config if self.config is not None else {}

    def process_alert(self, alert_data: Dict[str, Any]):
        """Processes a single alert, applying validation and sending notifications."""
        # Placeholder for actual implementation
        logger.info(f"Processing alert: {alert_data.get('pattern')}")
        # Example: validate and send
        if self.validate_alert(alert_data):
            self.send_alert(alert_data)

    def validate_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Validates an alert based on predefined criteria."""
        # Placeholder for actual implementation
        confidence = alert_data.get('confidence', 0.0)
        if confidence >= 0.90: # Example threshold
            logger.info(f"Alert {alert_data.get('pattern')} validated with confidence {confidence}")
            return True
        logger.warning(f"Alert {alert_data.get('pattern')} failed validation (confidence {confidence})")
        return False

    def _prepare_alert(self, pattern: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize and enrich alert payload prior to delivery."""
        if not isinstance(pattern, dict):
            return None

        pattern.setdefault("pattern", pattern.get("pattern_type"))
        symbol = pattern.get("symbol", "")
        
        # Resolve token to symbol if symbol is a numeric token or UNKNOWN token
        symbol = self._resolve_symbol_token(symbol, pattern)
        pattern["symbol"] = symbol  # Update pattern with resolved symbol

        # Ensure pattern metadata is present
        self._ensure_pattern_metadata(pattern)

        indicator_snapshot = self._fetch_indicators_from_redis(symbol, pattern)
        if indicator_snapshot:
            existing_indicators = pattern.get("indicators") if isinstance(pattern.get("indicators"), dict) else {}
            merged_indicators = {**existing_indicators, **indicator_snapshot}
            pattern["indicators"] = merged_indicators

            for key in ("rsi", "atr", "volume_ratio", "price_change", "ema_20", "ema_50", "vwap"):
                if key in merged_indicators and not self._has_meaningful_value(pattern.get(key)):
                    pattern[key] = merged_indicators[key]

        # Fetch Greeks for option symbols
        is_option = any(x in symbol.upper() for x in ['CE', 'PE']) or 'NFO:' in symbol.upper()
        if is_option:
            greeks_snapshot = self._fetch_greeks_from_redis(symbol)
            if greeks_snapshot:
                existing_greeks = pattern.get("greeks") if isinstance(pattern.get("greeks"), dict) else {}
                merged_greeks = {**existing_greeks, **greeks_snapshot}
                pattern["greeks"] = merged_greeks
                
                # Also add individual Greek fields to top level for dashboard compatibility
                for key in ("delta", "gamma", "theta", "vega", "rho"):
                    if key in merged_greeks and not self._has_meaningful_value(pattern.get(key)):
                        pattern[key] = merged_greeks[key]

        self._ensure_news_context(pattern)
        self._attach_market_context(pattern)

        if self.risk_manager:
            risk_metrics = pattern.get("risk_metrics")
            missing_risk = not isinstance(risk_metrics, dict) or risk_metrics.get("error")
            if missing_risk:
                try:
                    self.risk_manager.calculate_risk_metrics(pattern)
                except Exception as exc:
                    logger.debug(f"Risk metrics enrichment failed for {symbol}: {exc}")

        return pattern

    def _build_validation_payload(self, pattern: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Construct payload for the validation stream."""
        symbol = pattern.get("symbol")
        pattern_name = pattern.get("pattern") or pattern.get("pattern_type")
        if not symbol or not pattern_name:
            return None

        timestamp_ms = pattern.get("timestamp_ms")
        if timestamp_ms is None:
            timestamp_source = (
                pattern.get("timestamp")
                or pattern.get("published_at")
                or datetime.now().isoformat()
            )
            timestamp_ms = TimestampNormalizer.to_epoch_ms(timestamp_source)

        last_price = (
            pattern.get("last_price")
            or pattern.get("last_price")
            or pattern.get("close")
            or 0.0
        )
        expected_move = (
            pattern.get("expected_move")
            or pattern.get("expected_move_pct")
            or pattern.get("projected_move_pct")
            or 0.0
        )

        payload = {
            "symbol": symbol,
            "pattern": pattern_name,
            "confidence": float(pattern.get("confidence", 0.0) or 0.0),
            "signal": pattern.get("signal", "NEUTRAL"),
            "timestamp_ms": int(timestamp_ms),
            "last_price": float(last_price or 0.0),
            "volume_ratio": float(pattern.get("volume_ratio", 0.0) or 0.0),
            "expected_move": float(expected_move or 0.0),
        }
        validation_id = pattern.get("validation_id")
        if validation_id:
            payload["alert_id"] = validation_id

        return payload
    
    def send_alert(self, pattern):
        """Send alert with full production pipeline."""
        try:
            logger.info(f"🔍 DEBUG: ProductionAlertManager.send_alert called for {pattern.get('symbol', 'Unknown') if pattern else 'None'}")

            pattern = self._prepare_alert(pattern)
            if pattern is None:
                logger.error("❌ Alert payload could not be prepared - aborting send")
                return False
            
            # NOTE: Removed premature should_send_alert check here - filter already handles cooldowns
            # The filter's should_send_alert() includes cooldown checking, so we don't need to check twice
            
            # STEP 1: Emergency Filter Check (before validation and filtering)
            if self.emergency_filter and self.emergency_filter.should_send_immediately(pattern):
                logger.info(f"🚨 EMERGENCY: Alert {pattern.get('symbol', 'UNKNOWN')} {pattern.get('pattern', 'UNKNOWN')} bypassing filters and grouping - sending immediately")
                # Emergency alerts bypass normal filters and grouping - send immediately
                self._send_notifications(pattern)
                self._update_history(pattern)
                self._mark_alert_sent(pattern)  # Update cooldown AFTER sending
                return True
            
            # Apply filters
            if self.filter:
                logger.info(f"🔍 DEBUG: Applying filter for {pattern.get('symbol', 'UNKNOWN')}")
                result = self.filter.should_send_alert(pattern)
                logger.info(f"🔍 DEBUG: Filter returned {result}")
                if not result:
                    logger.info(f"🔍 DEBUG: Alert {pattern.get('symbol', 'UNKNOWN')} rejected by filter")
                    return False
            
            # ALERT GROUPING: Apply grouping logic AFTER validation, BEFORE sending (via enhanced_validator)
            logger.info(f"🔍 DEBUG: Applying grouping for {pattern.get('symbol', 'UNKNOWN')}")
            grouping_result = self.enhanced_validator.add_alert_to_group(pattern) if self.enhanced_validator else {'action': 'SEND_IMMEDIATE', 'alert': pattern}
            logger.info(f"🔍 DEBUG: Grouping result: {grouping_result}")
            
            if grouping_result['action'] == 'SEND_IMMEDIATE':
                # Critical or high-confidence alert - send immediately
                logger.info(f"🔍 DEBUG: Sending immediately for {pattern.get('symbol', 'UNKNOWN')}")
                self._send_notifications(pattern)
                self._update_history(pattern)
                return True
                
            elif grouping_result['action'] == 'SEND_GROUPED':
                # Group flushed - send summary alert
                summary_alert = grouping_result['alert']
                if summary_alert:
                    self._send_notifications(summary_alert)
                    self._update_history(summary_alert)
                    self._mark_alert_sent(summary_alert)  # Update cooldown AFTER sending
                    logger.info(f"📊 Grouped alert sent: {summary_alert.get('symbol')} {summary_alert.get('pattern')} ({summary_alert.get('grouped_count', 0)} alerts)")
                    return True
                else:
                    logger.warning("Grouped alert is None, sending original alert as fallback")
                    self._send_notifications(pattern)
                    self._update_history(pattern)
                    self._mark_alert_sent(pattern)  # Update cooldown AFTER sending
                    return True
                
            elif grouping_result['action'] == 'GROUPED':
                # Alert grouped - wait for flush (NOT SENT YET)
                group_key = grouping_result.get('group_key', 'unknown')
                logger.info(f"📦 Alert GROUPED (not sent yet, waiting for flush): {pattern.get('symbol')} {pattern.get('pattern')} (group_key: {group_key})")
                # Check if any existing groups should be flushed before returning
                flush_result = self.enhanced_validator._check_and_flush_stale_groups() if self.enhanced_validator else None
                if flush_result:
                    if flush_result.get('action') == 'SEND_GROUPED':
                        # A group was flushed - send the summary alert
                        summary_alert = flush_result.get('alert')
                        if summary_alert:
                            logger.info(f"📊 Sending auto-flushed grouped alert: {summary_alert.get('symbol')} {summary_alert.get('pattern')}")
                            self._send_notifications(summary_alert)
                            self._update_history(summary_alert)
                            self._mark_alert_sent(summary_alert)
                    elif flush_result.get('action') == 'SEND_IMMEDIATE':
                        # Group below min size - send individual alert
                        individual_alert = flush_result.get('alert')
                        if individual_alert:
                            logger.info(f"📊 Sending auto-flushed individual alert: {individual_alert.get('symbol')} {individual_alert.get('pattern')}")
                            self._send_notifications(individual_alert)
                            self._update_history(individual_alert)
                            self._mark_alert_sent(individual_alert)
                return True  # Return True to indicate alert was processed (grouped)
            
            # Fallback (should not reach here, but preserve original behavior)
            self._send_notifications(pattern)
            self._update_history(pattern)
            self._mark_alert_sent(pattern)  # Update cooldown AFTER sending
            return True
            
        except Exception as e:
            logger.error(f"Error in production send_alert: {e}")
            return False
    
    def send_news_alert(self, symbol: str, news_data: dict):
        """Send high-impact news alert independently of patterns."""
        try:
            logger.info(f"📰 [NEWS_ALERT] Processing news alert for {symbol}")
            
            # Create news alert payload
            news_alert = {
                "symbol": symbol,
                "pattern": "NEWS_ALERT",
                "confidence": 0.9,  # High confidence for news alerts
                "signal": "NEWS",
                "news_context": news_data,
                "has_news": True,
                "news_impact": news_data.get("impact", "HIGH"),
                "news_sentiment": news_data.get("sentiment", "neutral"),
                "news_title": news_data.get("title", ""),
                "news_source": news_data.get("source", ""),
                "timestamp": int(time.time() * 1000),
                "allow_premarket": True,  # News can be sent premarket
            }
            
            # Send directly without pattern filtering
            self._send_notifications(news_alert)
            logger.info(f"✅ [NEWS_ALERT] News alert sent for {symbol}: {news_data.get('title', 'No title')}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending news alert: {e}")
            return False
    
    # Cooldown methods removed
    
    # NOTE: format_actionable_alert and _format_news_context removed - use HumanReadableAlertTemplates in notifiers.py instead
    
    def _get_order_type(self, action):
        """Determine order type from action"""
        if 'LIMIT' in action:
            return 'LIMIT ORDER'
        elif 'MARKET' in action:
            return 'MARKET ORDER'
        return 'MONITOR ONLY'

    def send_trading_alert(self, signal):
        """Send truly actionable trading alert"""
        try:
            logger.info(f"🔍 DEBUG: send_trading_alert called for {signal.get('symbol', 'Unknown')} {signal.get('pattern', 'Unknown')}")
            logger.info(f"🔍 DEBUG: Signal data: {signal}")
            
            # Ensure risk metrics are calculated
            if not signal.get('risk_metrics') or signal.get('risk_metrics', {}).get('error'):
                if self.risk_manager:
                    logger.info(f"🔍 DEBUG: Calculating risk metrics for {signal.get('symbol', 'UNKNOWN')}")
                    self.risk_manager.calculate_risk_metrics(signal)
                else:
                    logger.warning(f"No risk manager available for {signal.get('symbol', 'UNKNOWN')}")
            
            # Extract trading parameters from risk_metrics if available
            risk_metrics = signal.get('risk_metrics', {})
            logger.info(f"🔍 DEBUG: Risk metrics: {risk_metrics}")
            if risk_metrics and not risk_metrics.get('error'):
                # Map risk metrics to required trading parameters
                signal['action'] = 'BUY' if signal.get('signal', '').upper() in ['BULLISH', 'BUY', 'LONG'] else 'SELL'
                signal['entry_price'] = signal.get('last_price', 0)
                signal['stop_loss'] = risk_metrics.get('stop_loss', 0)
                signal['target'] = risk_metrics.get('target_price', 0)
                signal['quantity'] = risk_metrics.get('position_size', 0) / signal.get('last_price', 1) if signal.get('last_price', 0) > 0 else 0
                
                logger.info(f"🔍 DEBUG: Trading parameters extracted for {signal.get('symbol', 'UNKNOWN')}: action={signal.get('action')}, entry={signal.get('entry_price')}, sl={signal.get('stop_loss')}, target={signal.get('target')}, qty={signal.get('quantity')}")
            else:
                logger.warning(f"No valid risk metrics for {signal.get('symbol', 'UNKNOWN')}: {risk_metrics.get('error', 'No risk_metrics')}")
            
            # Validate we have trading parameters
            if not self._is_actionable_signal(signal):
                pattern_name = signal.get('pattern') or signal.get('pattern_type') or 'unknown'
                symbol = signal.get('symbol', 'UNKNOWN')
                missing_fields = [field for field in ['action', 'entry_price', 'stop_loss', 'target', 'quantity'] if not signal.get(field)]
                logger.info(f"🔍 NON-ACTIONABLE: {pattern_name} for {symbol} - Missing: {missing_fields}")
                logger.info(f"🔍 Signal data: {signal}")
                return
            
            # Send to all channels (notifiers will automatically use actionable format when trading params exist)
            logger.info(f"🔍 DEBUG: Sending notifications for {signal.get('symbol', 'Unknown')}")
            if self.telegram_notifier:
                logger.info(f"🔍 DEBUG: Sending to Telegram")
                self.telegram_notifier.send_alert(signal)
            
            if self.redis_notifier:
                logger.info(f"🔍 DEBUG: Sending to Redis")
                self.redis_notifier.send_alert(signal)
            
            if self.macos_notifier:
                logger.info(f"🔍 DEBUG: Sending to macOS")
                self.macos_notifier.send_alert(signal)
            
            # Publish to Redis for standalone validator
            self._publish_to_validator(signal)
            
            logger.info(f"Sent actionable alert: {signal.get('pattern_type')} for {signal.get('symbol')}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending trading alert: {e}")
            return False

    def _is_actionable_signal(self, signal):
        """Check if signal has required trading parameters"""
        required = ['action', 'entry_price', 'stop_loss', 'target', 'quantity']
        return all(signal.get(field) for field in required)

    def _send_notifications(self, pattern):
        """Send notifications via all available channels."""
        logger.info(f"🔍 DEBUG: _send_notifications called for {pattern.get('symbol', 'Unknown') if pattern else 'None'}")
        
        # Check if pattern is None or invalid
        if pattern is None:
            logger.error("❌ Pattern is None - cannot send notifications")
            return
        
        if not isinstance(pattern, dict):
            logger.error(f"❌ Pattern is not a dict: {type(pattern)} - cannot send notifications")
            return
        
        # Use the new trading alert method
        logger.info(f"🔍 DEBUG: Calling send_trading_alert for {pattern.get('symbol', 'Unknown')}")
        self.send_trading_alert(pattern)
    
    def _publish_to_validator(self, pattern):
        """Publish alert to Redis for standalone validator."""
        try:
            if self.redis_client:
                # Add timestamp and validation metadata
                alert_data = {
                    **pattern,
                    "published_at": datetime.now().isoformat(),
                    "validator_ready": True,
                    "source": "scanner_main"
                }
                
                # Publish to alerts channel for validator
                self.redis_client.publish("alerts:notifications", json.dumps(alert_data))
                logger.debug(f"📡 Published alert to validator: {pattern.get('symbol', 'UNKNOWN')} {pattern.get('pattern', 'UNKNOWN')}")
            else:
                logger.warning("❌ Redis client not available for validator publishing")

        except Exception as e:
            logger.error(f"❌ Failed to publish alert to validator: {e}")
    
    def _update_history(self, pattern):
        """Update alert history."""
        self.alert_history.append({
            'timestamp': datetime.now().isoformat(),
            'pattern': pattern
        })
        
        # Trim history if too large
        if len(self.alert_history) > self.max_history_size:
            self.alert_history = self.alert_history[-self.max_history_size:]
    
    def _resolve_symbol_token(self, symbol: str, pattern: Dict[str, Any]) -> str:
        """Resolve instrument token to human-readable symbol."""
        try:
            # Check if symbol is a numeric token or UNKNOWN token format
            symbol_str = str(symbol) if symbol else ""
            
            # Check if we need to resolve (numeric, TOKEN_ prefix, or UNKNOWN_ prefix)
            if not symbol_str or symbol_str == "UNKNOWN":
                # Try to get instrument_token from pattern
                instrument_token = pattern.get("instrument_token") or pattern.get("token")
                if instrument_token:
                    try:
                        from crawlers.utils.instrument_mapper import InstrumentMapper
                        mapper = InstrumentMapper()
                        resolved = mapper.token_to_symbol(int(instrument_token))
                        if resolved and not resolved.startswith("UNKNOWN_"):
                            logger.debug(f"✅ Resolved token {instrument_token} to {resolved}")
                            return resolved
                    except Exception as e:
                        logger.debug(f"Token resolution failed for {instrument_token}: {e}")
            
            # Check if symbol itself is a token number
            elif symbol_str.isdigit() or symbol_str.startswith("TOKEN_") or symbol_str.startswith("UNKNOWN_"):
                try:
                    # Extract token number
                    if symbol_str.startswith("TOKEN_"):
                        token_str = symbol_str.replace("TOKEN_", "")
                    elif symbol_str.startswith("UNKNOWN_"):
                        token_str = symbol_str.replace("UNKNOWN_", "")
                    else:
                        token_str = symbol_str
                    
                    token = int(token_str)
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    mapper = InstrumentMapper()
                    resolved = mapper.token_to_symbol(token)
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        logger.debug(f"✅ Resolved token {token} to {resolved}")
                        return resolved
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not parse token from symbol '{symbol_str}': {e}")
                except Exception as e:
                    logger.debug(f"Token resolution failed for '{symbol_str}': {e}")
            
            # Return original symbol if already resolved or resolution failed
            return symbol
            
        except Exception as e:
            logger.error(f"Error in _resolve_symbol_token: {e}")
            return symbol  # Return original on error
    
    def _ensure_pattern_metadata(self, pattern: Dict[str, Any]) -> None:
        """Ensure pattern has required metadata fields for formatting."""
        if not isinstance(pattern, dict):
            return
        
        pattern_name = pattern.get('pattern') or pattern.get('pattern_type') or 'unknown'
        
        # Get pattern description from templates
        try:
            from alerts.notifiers import HumanReadableAlertTemplates
            pattern_desc = HumanReadableAlertTemplates.get_pattern_description(pattern_name)
        except Exception:
            pattern_desc = {
                "title": f"📊 {pattern_name.replace('_', ' ').title()}",
                "description": f"{pattern_name.replace('_', ' ').title()} pattern detected",
                "trading_instruction": "Monitor closely",
            }
        
        # Set metadata fields if not present
        pattern.setdefault('pattern_title', pattern_desc.get('title'))
        pattern.setdefault('pattern_description', pattern_desc.get('description'))
        if not pattern.get('trading_instruction'):
            pattern['trading_instruction'] = pattern_desc.get('trading_instruction') or pattern.get('trading_instruction', 'Monitor closely')
    
    def _mark_alert_sent(self, pattern: Dict[str, Any]) -> None:
        """Mark alert as sent and update cooldown tracker in filter."""
        try:
            if self.filter and hasattr(self.filter, '_mark_cooldown'):
                self.filter._mark_cooldown(pattern)
        except Exception as e:
            logger.debug(f"Error marking alert sent: {e}")

