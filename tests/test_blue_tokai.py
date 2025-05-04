#!/usr/bin/env python3
"""
Real-world test script for RoasterPipeline using Blue Tokai Coffee Roasters
"""
import asyncio
import json
import logging
import os
import sys

# Add parent directory to the Python path so we can find the modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scrapers.roaster_pipeline import RoasterPipeline
from common.utils import setup_logging

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_test():
    # Blue Tokai test data
    test_roaster = {
        "name": "Blue Tokai Coffee Roasters",
        "website": "https://bluetokaicoffee.com",
        "city": "Bangalore"  # They have multiple locations, but we'll use Bangalore
    }
    
    logger.info(f"Testing pipeline with {test_roaster['name']}")
    
    # Initialize pipeline with refresh_cache=True to ensure fresh data
    pipeline = RoasterPipeline(refresh_cache=True)
    
    try:
        # Process the roaster
        result = await pipeline.process_roaster(test_roaster)
        
        if result:
            # Print results in a nice format
            logger.info(f"Successfully extracted data for {result['name']}:")
            logger.info(f"Description: {result.get('description', 'N/A')}")
            logger.info(f"Logo URL: {result.get('logo_url', 'N/A')}")
            logger.info(f"Social Links: {result.get('social_links', [])}")
            logger.info(f"Instagram: {result.get('instagram_handle', 'N/A')}")
            logger.info(f"Location: {result.get('city', 'N/A')}, {result.get('state', 'N/A')}")
            logger.info(f"Founded: {result.get('founded_year', 'N/A')}")
            logger.info(f"Has Subscription: {result.get('has_subscription', False)}")
            logger.info(f"Has Physical Store: {result.get('has_physical_store', False)}")
            
            # Save the result as JSON for inspection
            with open('blue_tokai_result.json', 'w') as f:
                json.dump(result, f, indent=2)
            logger.info("Results saved to blue_tokai_result.json")
            
        else:
            logger.error(f"Failed to extract data for {test_roaster['name']}")
    
    except Exception as e:
        logger.exception(f"Error testing pipeline: {str(e)}")
    
    finally:
        # Close the pipeline
        await pipeline.close()

if __name__ == "__main__":
    # Run the test
    asyncio.run(run_test())