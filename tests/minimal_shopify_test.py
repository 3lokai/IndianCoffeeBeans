# tests/minimal_shopify_test.py
import asyncio
import json
import logging
import os
import sys
from pprint import pformat

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Sample Shopify products response
SHOPIFY_SAMPLE = {
    "products": [
        {
            "id": 123456789,
            "title": "Ethiopia Yirgacheffe",
            "handle": "ethiopia-yirgacheffe",
            "body_html": "<p>A fruity, floral coffee from Ethiopia.</p>",
            "product_type": "Coffee",
            "tags": ["Single Origin", "Light Roast"],
            "variants": [{"price": "550.00", "available": True}],
            "images": [{"src": "https://example.com/test.jpg"}]
        }
    ]
}

# Ensure the Shopify URL gets passed and processed correctly
async def test_shopify_discovery():
    import aiohttp
    
    logger.info("Starting minimal Shopify discovery test")
    
    # Base URL to test with
    base_url = "https://example.com"
    products_url = f"{base_url}/products.json?limit=250&page=1"
    
    try:
        async with aiohttp.ClientSession() as session:
            # Manually perform the request that should happen in _discover_shopify_products
            logger.info(f"Requesting: {products_url}")
            async with session.get(products_url) as response:
                status = response.status
                logger.info(f"Response status: {status}")
                
                if status == 200:
                    try:
                        data = await response.json()
                        products = data.get('products', [])
                        logger.info(f"Found {len(products)} products in response")
                        
                        # Log a sample of the first product
                        if products:
                            logger.info(f"First product: {pformat(products[0])}")
                    except Exception as e:
                        logger.error(f"Error parsing JSON response: {e}")
                else:
                    logger.warning(f"Non-200 response: {status}")
                    
    except Exception as e:
        logger.exception(f"Error during request: {e}")
    
    logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(test_shopify_discovery())