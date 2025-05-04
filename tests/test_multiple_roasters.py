#!/usr/bin/env python3
"""
Comprehensive test script for RoasterPipeline with multiple real Indian coffee roasters
"""
import asyncio
import json
import logging
import os
from scrapers.roaster_pipeline import RoasterPipeline
from common.utils import setup_logging, slugify

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test data for multiple Indian coffee roasters
TEST_ROASTERS = [
    {
        "name": "Blue Tokai Coffee Roasters",
        "website": "https://bluetokaicoffee.com",
        "city": "Bangalore"
    },
    {
        "name": "Third Wave Coffee Roasters",
        "website": "https://www.thirdwavecoffeeroasters.com",
        "city": "Bangalore"
    },
    {
        "name": "Subko Coffee Roasters",
        "website": "https://www.subko.coffee",
        "city": "Mumbai"
    },
    {
        "name": "KC Roasters",
        "website": "https://kcroasters.com",
        "city": "Mumbai"
    },
    {
        "name": "Roastery Coffee House",
        "website": "https://roasterycoffee.co.in",
        "city": "Hyderabad"
    },
    {
        "name": "Bombay Island Coffee",
        "website": "https://www.bombayisland.com",
        "city": "Mumbai"
    },
    {
        "name": "Grey Soul Coffee Roasters",
        "website": "https://greysoul.coffee",
        "city": "Bangalore"
    },
    {
        "name": "Naivo Coffee Company",
        "website": "https://naivo.in",
        "city": "Bangalore"
    }
]

async def test_single_roaster(pipeline, roaster_data):
    """Test the pipeline with a single roaster"""
    logger.info(f"Testing pipeline with {roaster_data['name']}")
    
    try:
        # Process the roaster
        result = await pipeline.process_roaster(roaster_data)
        
        if result:
            # Print results in a nice format
            logger.info(f"✅ Successfully extracted data for {result['name']}:")
            logger.info(f"  Description: {result.get('description', 'N/A')[:100]}...")
            logger.info(f"  Logo URL: {result.get('logo_url', 'N/A')}")
            logger.info(f"  Social Links: {len(result.get('social_links', []))} links found")
            logger.info(f"  Instagram: {result.get('instagram_handle', 'N/A')}")
            logger.info(f"  Location: {result.get('city', 'N/A')}, {result.get('state', 'N/A')}")
            logger.info(f"  Founded: {result.get('founded_year', 'N/A')}")
            logger.info(f"  Has Subscription: {result.get('has_subscription', False)}")
            logger.info(f"  Has Physical Store: {result.get('has_physical_store', False)}")
            
            # Save the result as JSON for inspection
            output_dir = "test_results"
            os.makedirs(output_dir, exist_ok=True)
            
            filename = os.path.join(output_dir, f"{slugify(result['name'])}_result.json")
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2)
            logger.info(f"  Results saved to {filename}")
            
            return result
        else:
            logger.error(f"❌ Failed to extract data for {roaster_data['name']}")
            return None
    
    except Exception as e:
        logger.exception(f"Error testing pipeline for {roaster_data['name']}: {str(e)}")
        return None

async def run_tests():
    """Run tests for all roasters"""
    # Initialize pipeline
    pipeline = RoasterPipeline(refresh_cache=True)
    
    try:
        # Test each roaster
        results = []
        errors = []
        
        # Process roasters with parallel processing (with a limit)
        tasks = []
        for roaster_data in TEST_ROASTERS:
            task = asyncio.create_task(test_single_roaster(pipeline, roaster_data))
            tasks.append((roaster_data["name"], task))
        
        # Wait for all tasks to complete
        for name, task in tasks:
            try:
                result = await task
                if result:
                    results.append(result)
                else:
                    errors.append(name)
            except Exception as e:
                logger.error(f"Error processing {name}: {str(e)}")
                errors.append(name)
        
        # Print summary
        logger.info("\n===== TEST SUMMARY =====")
        logger.info(f"Total roasters tested: {len(TEST_ROASTERS)}")
        logger.info(f"Successful: {len(results)}")
        logger.info(f"Failed: {len(errors)}")
        
        if errors:
            logger.warning(f"Failed roasters: {', '.join(errors)}")
        
        # Create a consolidated summary JSON
        summary = {
            "total_tested": len(TEST_ROASTERS),
            "successful": len(results),
            "failed": len(errors),
            "failed_roasters": errors,
            "results": {r["name"]: {
                "has_description": bool(r.get("description")),
                "has_logo": bool(r.get("logo_url")),
                "social_links_count": len(r.get("social_links", [])),
                "has_instagram": bool(r.get("instagram_handle")),
                "has_location": bool(r.get("city") and r.get("state")),
                "has_founded_year": bool(r.get("founded_year")),
                "has_subscription": r.get("has_subscription", False),
                "has_physical_store": r.get("has_physical_store", False),
            } for r in results}
        }
        
        # Save summary
        output_dir = "test_results"
        with open(os.path.join(output_dir, "summary.json"), 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary saved to {os.path.join(output_dir, 'summary.json')}")
    
    except Exception as e:
        logger.exception(f"Error running tests: {str(e)}")
    
    finally:
        # Close the pipeline
        await pipeline.close()

if __name__ == "__main__":
    # Run all tests
    asyncio.run(run_tests())