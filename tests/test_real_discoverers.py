#!/usr/bin/env python3
"""
Real-world test script for the product discoverers
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.platform_detector import PlatformDetector
from common.utils import setup_logging

# Setup logging
log_file = setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test data for real Indian coffee roasters - optimized for different platforms
TEST_ROASTERS = [
    # Likely Shopify
    {
        "name": "Blue Tokai Coffee Roasters",
        "website_url": "https://bluetokaicoffee.com",
        "slug": "blue-tokai-coffee-roasters"
    },
    # Likely WooCommerce
    {
        "name": "Subko Coffee Roasters",
        "website_url": "https://www.subko.coffee",
        "slug": "subko-coffee-roasters"
    },
    # Generic website
    {
        "name": "Corridor Seven Coffee Roasters",
        "website_url": "https://www.corridorseven.coffee",
        "slug": "corridor-seven-coffee-roasters"
    },
    # Another platform to test
    {
        "name": "KC Roasters",
        "website_url": "https://kcroasters.com", 
        "slug": "kc-roasters"
    }
]

async def test_discovery_for_roaster(discovery_manager, roaster_data):
    """Test product discovery for a single roaster"""
    logger.info(f"Testing discovery for {roaster_data['name']}")
    
    try:
        # First detect platform
        async with PlatformDetector() as platform_detector:
            platform_info = await platform_detector.detect(roaster_data["website_url"])
        logger.info(f"Detected platform: {platform_info.get('platform', 'unknown')}")
        
        # Add platform info to roaster data
        roaster_data["_platform"] = platform_info
        
        # Discover products
        start_time = datetime.now()
        products = await discovery_manager.discover_products(roaster_data)
        end_time = datetime.now()
        time_taken = (end_time - start_time).total_seconds()
        
        if products:
            logger.info(f"✅ Successfully discovered {len(products)} products for {roaster_data['name']} in {time_taken:.2f} seconds")
            
            # Analyze discovery methods to highlight Crawl4AI usage
            discovery_methods = {}
            for product in products:
                method = product.get("discovery_method", "unknown")
                discovery_methods[method] = discovery_methods.get(method, 0) + 1
            
            logger.info(f"Discovery methods breakdown:")
            for method, count in discovery_methods.items():
                logger.info(f"  - {method}: {count} products ({count/len(products)*100:.1f}%)")
            
            # Highlight Crawl4AI-related methods
            crawl4ai_methods = [m for m in discovery_methods.keys() 
                               if m.startswith('crawl4ai_')]
            if crawl4ai_methods:
                crawl4ai_count = sum(discovery_methods[m] for m in crawl4ai_methods)
                logger.info(f"  → Total Crawl4AI: {crawl4ai_count} products ({crawl4ai_count/len(products)*100:.1f}%)")
            
            # Print some sample products
            sample_size = min(3, len(products))
            logger.info(f"Sample of discovered products:")
            for i, product in enumerate(products[:sample_size]):
                logger.info(f"  {i+1}. {product.get('name', 'Unknown')}")
                logger.info(f"     URL: {product.get('direct_buy_url', 'N/A')}")
                logger.info(f"     Discovery method: {product.get('discovery_method', 'N/A')}")
                logger.info(f"     Image URL: {product.get('image_url', 'N/A')[:50]}...")
            
            # Save the result as JSON for inspection
            output_dir = "test_results/discoverers"
            os.makedirs(output_dir, exist_ok=True)
            
            filename = os.path.join(output_dir, f"{roaster_data['slug']}_products.json")
            with open(filename, 'w') as f:
                json.dump(products, f, indent=2)
            logger.info(f"  Products saved to {filename}")
            
            return {
                "name": roaster_data["name"],
                "platform": platform_info.get("platform"),
                "products_count": len(products),
                "products_with_image": sum(1 for p in products if p.get("image_url")),
                "discovery_methods": {
                    method: count for method, count in discovery_methods.items()
                }
            }
        else:
            logger.error(f"❌ Failed to discover products for {roaster_data['name']}")
            return {
                "name": roaster_data["name"],
                "platform": platform_info.get("platform"),
                "products_count": 0,
                "error": "No products discovered"
            }
    
    except Exception as e:
        logger.exception(f"Error discovering products for {roaster_data['name']}: {str(e)}")
        return {
            "name": roaster_data["name"],
            "error": str(e)
        }

async def run_tests():
    """Run discovery tests for all roasters"""
    # Initialize discovery manager with refresh_cache=True to ensure fresh data
    discovery_manager = DiscoveryManager(refresh_cache=True)
    
    try:
        # Test each roaster
        results = []
        errors = []
        
        for roaster_data in TEST_ROASTERS:
            # Process each roaster
            result = await test_discovery_for_roaster(discovery_manager, roaster_data)
            
            if result and result.get("products_count", 0) > 0:
                results.append(result)
            else:
                errors.append(roaster_data["name"])
        
        # Print summary
        logger.info("\n===== DISCOVERY TEST SUMMARY =====")
        logger.info(f"Total roasters tested: {len(TEST_ROASTERS)}")
        logger.info(f"Successful: {len(results)}")
        logger.info(f"Failed: {len(errors)}")
        
        if errors:
            logger.warning(f"Failed roasters: {', '.join(errors)}")
        
        # Analyze which discoverers performed best
        all_methods = set()
        for r in results:
            if "discovery_methods" in r:
                all_methods.update(r["discovery_methods"].keys())
        
        method_counts = {method: sum(r.get("discovery_methods", {}).get(method, 0) for r in results) 
                        for method in all_methods}
        
        logger.info("\n===== DISCOVERY METHODS PERFORMANCE =====")
        for method, count in sorted(method_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"{method}: {count} products")
        
        # Group Crawl4AI methods
        crawl4ai_methods = [m for m in all_methods if m.startswith('crawl4ai_')]
        if crawl4ai_methods:
            crawl4ai_count = sum(method_counts[m] for m in crawl4ai_methods)
            logger.info(f"\nTotal Crawl4AI: {crawl4ai_count} products")
        
        # Create a consolidated summary JSON
        summary = {
            "total_tested": len(TEST_ROASTERS),
            "successful": len(results),
            "failed": len(errors),
            "failed_roasters": errors,
            "discovery_methods_performance": method_counts,
            "results": {r["name"]: {
                "platform": r.get("platform", "unknown"),
                "products_count": r.get("products_count", 0),
                "products_with_image": r.get("products_with_image", 0),
                "discovery_methods": r.get("discovery_methods", {})
            } for r in results}
        }
        
        # Save summary
        output_dir = "test_results/discoverers"
        with open(os.path.join(output_dir, "discovery_summary.json"), 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary saved to {os.path.join(output_dir, 'discovery_summary.json')}")
        
        return results
    
    finally:
        # Close the discovery manager
        await discovery_manager.close()

if __name__ == "__main__":
    # Run all tests
    asyncio.run(run_tests())