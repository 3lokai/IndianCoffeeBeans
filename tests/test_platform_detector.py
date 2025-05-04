#!/usr/bin/env python3
"""
Test script for platform detector.
Usage: python -m tests.test_detector <website_url>
"""

import asyncio
import sys
import logging
import json
from scrapers.platform_detector import PlatformDetector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    if len(sys.argv) < 2:
        print("Usage: python -m tests.test_detector <website_url>")
        sys.exit(1)
    
    url = sys.argv[1]
    logger.info(f"Testing platform detection for: {url}")
    
    detector = PlatformDetector()
    try:
        result = await detector.detect(url)
        print(json.dumps(result, indent=2))
        logger.info(f"Detected platform: {result['platform']}")
        
        if result['api_endpoints']:
            logger.info(f"API endpoints to try: {result['api_endpoints']}")
        
        if result['structured_data_paths']:
            logger.info(f"Structured data paths: {result['structured_data_paths']}")
    finally:
        await detector.close()

if __name__ == "__main__":
    asyncio.run(main())