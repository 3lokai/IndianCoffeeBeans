#!/usr/bin/env python3
"""
Real-world test script for product extractors using real Indian coffee websites
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.extractors.json_css_extractor import JsonCssExtractor
from scrapers.extractors.deepseek_extractor import DeepseekExtractor
from common.utils import setup_logging
from common.models import RoastLevel, BeanType, ProcessingMethod
from crawl4ai import BrowserConfig

# Setup logging
log_file = setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test data: Sample coffee product URLs from different roasters
TEST_PRODUCTS = [
    # Blue Tokai - Shopify based
    {
        "name": "Attikan Estate | Medium Roast",
        "slug": "attikan-estate-medium-roast",
        "direct_buy_url": "https://bluetokaicoffee.com/products/attikan-estate",
        "platform": "shopify",
        "roaster_name": "Blue Tokai Coffee Roasters",
        "roaster_slug": "blue-tokai-coffee-roasters",
        "description": "A classic South Indian Coffee with notes of chocolate and nuts"
    },
    # Subko - WooCommerce based
    {
        "name": "Nachammai Natural",
        "slug": "nachammai-natural",
        "direct_buy_url": "https://www.subko.coffee/product/nachammai-natural/",
        "platform": "woocommerce",
        "roaster_name": "Subko Coffee Roasters",
        "roaster_slug": "subko-coffee-roasters",
        "description": "Juicy and sweet coffee from Tamil Nadu"
    },
    # KC Roasters - Custom/Generic
    {
        "name": "Ethiopia Kayon Mountain",
        "slug": "ethiopia-kayon-mountain",
        "direct_buy_url": "https://kcroasters.com/products/ethiopia-kayon-mountain",
        "platform": "generic",
        "roaster_name": "KC Roasters",
        "roaster_slug": "kc-roasters",
        "description": "Vibrant and fruity Ethiopian coffee"
    },
    # Corridor Seven - Custom/Generic
    {
        "name": "Kalledevarapura Estate",
        "slug": "kalledevarapura-estate",
        "direct_buy_url": "https://www.corridorseven.coffee/collections/coffee/products/kalledevarapura-estate",
        "platform": "generic",
        "roaster_name": "Corridor Seven Coffee Roasters",
        "roaster_slug": "corridor-seven-coffee-roasters",
        "description": "Elegant and smooth coffee from Karnataka"
    }
]

async def test_extractor(product, use_deepseek=False):
    """Test extraction for a single product"""
    logger.info(f"Testing extraction for {product['name']} from {product['roaster_name']}")
    logger.info(f"URL: {product['direct_buy_url']}")
    
    # Create browser config
    browser_config = BrowserConfig(
        headless=True,
        ignore_https_errors=True,
        viewport_width=1280,
        viewport_height=800
    )
    
    # Initialize extractors
    json_css_extractor = JsonCssExtractor(browser_config=browser_config)
    deepseek_extractor = DeepseekExtractor(browser_config=browser_config) if use_deepseek else None
    
    try:
        # First try the JSON-CSS extractor
        start_time = datetime.now()
        enhanced_product = await json_css_extractor.extract(product)
        primary_time = (datetime.now() - start_time).total_seconds()
        
        # Add extraction source
        enhanced_product["extracted_by"] = "json_css"
        
        # If DeepSeek is enabled and product needs enrichment, try that too
        if use_deepseek and deepseek_extractor:
            # Check if we need enrichment
            missing_attrs = 0
            for attr in ["roast_level", "bean_type", "processing_method", "flavor_profiles"]:
                if attr not in enhanced_product or enhanced_product.get(attr) == "unknown":
                    missing_attrs += 1
            
            if missing_attrs >= 2:
                logger.info(f"Product needs DeepSeek enrichment (missing {missing_attrs} attributes)")
                start_time = datetime.now()
                enhanced_product = await deepseek_extractor.extract(enhanced_product)
                deepseek_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"DeepSeek extraction took {deepseek_time:.2f} seconds")
        
        # Format and print results
        logger.info(f"✅ Extraction completed in {primary_time:.2f} seconds")
        logger.info(f"Extracted data:")
        logger.info(f"  Name: {enhanced_product.get('name', 'Unknown')}")
        
        # Print description (truncated)
        description = enhanced_product.get('description', 'N/A')
        if description:
            truncated = description[:100] + "..." if len(description) > 100 else description
            logger.info(f"  Description: {truncated}")
        
        # Print core coffee attributes
        logger.info(f"  Roast Level: {enhanced_product.get('roast_level', 'Unknown')}")
        logger.info(f"  Bean Type: {enhanced_product.get('bean_type', 'Unknown')}")
        logger.info(f"  Processing Method: {enhanced_product.get('processing_method', 'Unknown')}")
        logger.info(f"  Region: {enhanced_product.get('region_name', 'Unknown')}")
        
        # Print prices
        price_fields = [k for k in enhanced_product.keys() if k.startswith('price_')]
        if price_fields:
            logger.info("  Prices:")
            for price_field in price_fields:
                logger.info(f"    {price_field}: {enhanced_product.get(price_field)}")
        
        # Print flavor profiles
        flavor_profiles = enhanced_product.get('flavor_profiles', [])
        if flavor_profiles:
            logger.info(f"  Flavor Profiles: {', '.join(flavor_profiles)}")
        
        # Print extraction source
        logger.info(f"  Extracted by: {enhanced_product.get('extracted_by', 'unknown')}")
        logger.info(f"  DeepSeek enriched: {enhanced_product.get('deepseek_enriched', False)}")
        
        # Save the result as JSON for inspection
        output_dir = "test_results/extractors"
        os.makedirs(output_dir, exist_ok=True)
        
        filename = os.path.join(output_dir, f"{product['slug']}_extracted.json")
        with open(filename, 'w') as f:
            # Convert any enum values to strings for JSON serialization
            serializable_product = {k: (str(v) if isinstance(v, (RoastLevel, BeanType, ProcessingMethod)) else v) 
                                   for k, v in enhanced_product.items()}
            json.dump(serializable_product, f, indent=2)
        logger.info(f"  Results saved to {filename}")
        
        return enhanced_product
    
    except Exception as e:
        logger.exception(f"Error extracting product {product['name']}: {str(e)}")
        return None

async def run_tests():
    """Run extraction tests for all test products"""
    results = []
    errors = []
    
    try:
        # Test each product
        for product in TEST_PRODUCTS:
            # Process with DeepSeek enabled
            enhanced_product = await test_extractor(product, use_deepseek=True)
            
            if enhanced_product:
                results.append({
                    "name": enhanced_product.get("name"),
                    "roaster": product["roaster_name"],
                    "platform": product["platform"],
                    "extracted_by": enhanced_product.get("extracted_by"),
                    "deepseek_enriched": enhanced_product.get("deepseek_enriched", False),
                    "has_roast_level": enhanced_product.get("roast_level") != "unknown",
                    "has_bean_type": enhanced_product.get("bean_type") != "unknown",
                    "has_processing": enhanced_product.get("processing_method") != "unknown",
                    "has_region": bool(enhanced_product.get("region_name")),
                    "has_flavors": bool(enhanced_product.get("flavor_profiles")),
                    "has_prices": any(k.startswith("price_") for k in enhanced_product.keys())
                })
            else:
                errors.append(product["name"])
        
        # Print summary
        logger.info("\n===== EXTRACTION TEST SUMMARY =====")
        logger.info(f"Total products tested: {len(TEST_PRODUCTS)}")
        logger.info(f"Successful: {len(results)}")
        logger.info(f"Failed: {len(errors)}")
        
        if errors:
            logger.warning(f"Failed products: {', '.join(errors)}")
        
        # Create a consolidated summary JSON
        summary = {
            "total_tested": len(TEST_PRODUCTS),
            "successful": len(results),
            "failed": len(errors),
            "failed_products": errors,
            "results": {r["name"]: {
                "roaster": r.get("roaster"),
                "platform": r.get("platform"),
                "extracted_by": r.get("extracted_by"),
                "deepseek_enriched": r.get("deepseek_enriched"),
                "has_roast_level": r.get("has_roast_level"),
                "has_bean_type": r.get("has_bean_type"),
                "has_processing": r.get("has_processing"),
                "has_region": r.get("has_region"),
                "has_flavors": r.get("has_flavors"),
                "has_prices": r.get("has_prices")
            } for r in results}
        }
        
        # Calculate DeepSeek usage statistics
        deepseek_count = sum(1 for r in results if r.get("deepseek_enriched"))
        summary["deepseek_usage"] = {
            "count": deepseek_count,
            "percentage": round(deepseek_count / len(results) * 100 if results else 0, 2)
        }
        
        # Save summary
        output_dir = "test_results/extractors"
        with open(os.path.join(output_dir, "extraction_summary.json"), 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary saved to {os.path.join(output_dir, 'extraction_summary.json')}")
        
        return results
    
    except Exception as e:
        logger.exception(f"Error running extraction tests: {str(e)}")
        return []

if __name__ == "__main__":
    # Run all tests
    asyncio.run(run_tests())