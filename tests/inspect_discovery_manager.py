# tests/inspect_discovery_manager.py
import asyncio
import inspect
import logging
import os
import sys
from pprint import pformat

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module to inspect
from scrapers.discoverers.discovery_manager import DiscoveryManager
from common.product_classifier import is_likely_coffee_product

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def inspect_discovery_manager():
    """Inspect the DiscoveryManager class to understand its structure and methods"""
    logger.info("=== Discovery Manager Inspection ===")
    
    # Print all methods in the DiscoveryManager class
    logger.info("Methods in DiscoveryManager:")
    for name, method in inspect.getmembers(DiscoveryManager, predicate=inspect.isfunction):
        if not name.startswith('__'):
            logger.info(f"  - {name}{inspect.signature(method)}")
    
    # Print the source code of key methods
    methods_to_inspect = [
        '_discover_shopify_products',
        '_discover_woocommerce_products',
        'discover_products'
    ]
    
    for method_name in methods_to_inspect:
        method = getattr(DiscoveryManager, method_name)
        logger.info(f"\n=== Source code for {method_name} ===")
        source = inspect.getsource(method)
        logger.info(source)
    
    # Check the product classifier
    logger.info("\n=== Product Classifier ===")
    logger.info(f"Module: {is_likely_coffee_product.__module__}")
    logger.info(f"Function: {is_likely_coffee_product.__name__}")
    logger.info(f"Source location: {inspect.getfile(is_likely_coffee_product)}")
    
    # Test the classifier with some examples
    test_cases = [
        {"name": "Ethiopia Yirgacheffe", "url": "https://example.com/products/ethiopia"},
        {"name": "Coffee Mug", "url": "https://example.com/products/mug"},
        {"name": "Dark Roast Blend", "description": "A rich, bold coffee blend."}
    ]
    
    logger.info("\nProduct classifier test cases:")
    for case in test_cases:
        result = is_likely_coffee_product(**case)
        logger.info(f"  - {case} -> {result}")
    
    # Create an instance for testing
    logger.info("\nCreating DiscoveryManager instance...")
    discovery_manager = DiscoveryManager()
    
    logger.info("Instance attributes:")
    for name, value in vars(discovery_manager).items():
        logger.info(f"  - {name}: {type(value)}")
    
    await discovery_manager.close()
    logger.info("Inspection completed")

if __name__ == "__main__":
    asyncio.run(inspect_discovery_manager())