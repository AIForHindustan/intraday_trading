"""
Patterns Package
================

Centralized pattern detection components including the 8-core detector and ICT modules.
"""

# Base pattern detector
from .base_detector import BasePatternDetector

# Simplified pattern detector (8 core patterns only)
from .pattern_detector import PatternDetector
from .ict import ICTPatternDetector

__all__ = [
    # Base detector
    "BasePatternDetector",
    
    # Simplified detector (8 core patterns)
    "PatternDetector",
    
    # ICT detector
    "ICTPatternDetector",
]
