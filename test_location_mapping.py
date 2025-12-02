#!/usr/bin/env python3
"""
Test script to verify location mapping functionality in dynamic_parser.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.dynamic_parser import normalize_location
from config.settings import LOCATION_ALIASES
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_location_mapping():
    """Test the location mapping functionality"""
    logger.info("=" * 60)
    logger.info("TESTING LOCATION MAPPING FUNCTIONALITY")
    logger.info("=" * 60)
    
    # Test cases
    test_cases = [
        "YASH IT Part",
        "YIT", 
        "Mindspace",
        "BTC",
        "Hyd",
        "BNG",
        "Unknown Location",
        "",
        None
    ]
    
    logger.info("Testing normalize_location function:")
    logger.info("-" * 40)
    
    for test_location in test_cases:
        normalized = normalize_location(test_location)
        logger.info(f"'{test_location}' → '{normalized}'")
    
    logger.info("-" * 40)
    logger.info("Available location aliases:")
    for alias, normalized in LOCATION_ALIASES.items():
        logger.info(f"  '{alias}' → '{normalized}'")
    
    logger.info("=" * 60)
    logger.info("LOCATION MAPPING TEST COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    test_location_mapping()