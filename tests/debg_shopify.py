# tests/debug_shopify.py
import asyncio
import json
import logging
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.discoverers.discovery_manager import DiscoveryManager
from unittest.mock import patch, MagicMock

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Create mock for aiohttp.ClientSession
class MockResponse:
    def __init__(self, text_data=None, json_data=None, status=200):
        self._text = text_data
        self._json = json_data
        self.status = status
        
    async def text(self):
        return self._text if self._text is not None else json.dumps(self._json)
        
    async def json(self):
        return self._json
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        pass

class MockSession:
    def __init__(self, responses):
        self.responses = responses or []
        self.request_count = 0
        self.closed = False
        self.requests = []  # Record requests
        
    def get(self, url, *args, **kwargs):
        self.requests.append(("GET", url, args, kwargs))
        logger.debug(f"MockSession.get called with URL: {url}")
        
        if not self.responses:
            resp = MockResponse(json_data={})
        elif self.request_count < len(self.responses):
            resp = self.responses[self.request_count]
        else:
            resp = self.responses[-1]
            
        self.request_count += 1
        return resp
        
    async def close(self):
        self.closed = True
        logger.debug("MockSession.close called")
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

# Sample shopify product data
shopify_products_json = {
    "products": [
        {
            "id": 123456789,
            "title": "Ethiopia Yirgacheffe",
            "handle": "ethiopia-yirgacheffe",
            "body_html": "<p>A fruity, floral coffee from Ethiopia.</p>",
            "published_at": "2024-01-01T12:00:00-05:00",
            "product_type": "Coffee Beans",
            "tags": ["Single Origin", "Light Roast", "Fruity"],
            "variants": [
                {
                    "id": 987654321,
                    "price": "18.00",
                    "available": True,
                }
            ],
            "images": [
                {"src": "https://example.com/yirgacheffe.jpg"}
            ]
        },
        {
            "id": 987654321,
            "title": "Brazil Santos",
            "handle": "brazil-santos",
            "body_html": "<p>A smooth, nutty coffee from Brazil.</p>",
            "published_at": "2024-01-02T10:00:00-05:00",
            "product_type": "Whole Bean Coffee",
            "tags": ["Single Origin", "Medium Roast", "Nutty"],
            "variants": [
                {
                    "id": 123456789,
                    "price": "16.50",
                    "available": True,
                }
            ],
            "images": [
                {"src": "https://example.com/santos.jpg"}
            ]
        }
    ]
}

async def debug_shopify_discovery():
    """Debug test for Shopify product discovery"""
    logger.info("Starting debug Shopify discovery test")
    
    # Create a mock session and pass it to patch aiohttp.ClientSession
    mock_responses = [
        MockResponse(json_data=shopify_products_json),
        MockResponse(json_data={"products": []}),
    ]
    mock_session = MockSession(mock_responses)
    
    # Trace all function calls to _discover_shopify_products
    original_discover_shopify = DiscoveryManager._discover_shopify_products
    
    async def traced_discover_shopify(self, base_url):
        logger.debug(f"TRACE: _discover_shopify_products called with base_url={base_url}")
        products = await original_discover_shopify(self, base_url)
        logger.debug(f"TRACE: _discover_shopify_products returning {len(products)} products")
        return products
    
    # Patch aiohttp.ClientSession and the is_likely_coffee_product function
    with patch('aiohttp.ClientSession', return_value=mock_session):
        with patch('scrapers.discoverers.discovery_manager.is_likely_coffee_product', return_value=True):
            # Patch the _discover_shopify_products method with our traced version
            DiscoveryManager._discover_shopify_products = traced_discover_shopify
            
            # Create discovery manager and run discovery
            discovery_manager = DiscoveryManager()
            
            # Try to run discovery
            try:
                logger.info("Calling _discover_shopify_products")
                products = await discovery_manager._discover_shopify_products("https://example.com")
                
                logger.info(f"Discovery returned {len(products)} products")
                if products:
                    for i, product in enumerate(products):
                        logger.info(f"Product {i+1}: {product.get('name', 'Unknown')}")
                else:
                    logger.error("No products were returned!")
                    
                # Check the mock session's request history
                logger.info(f"Requests made: {len(mock_session.requests)}")
                for i, (method, url, args, kwargs) in enumerate(mock_session.requests):
                    logger.info(f"Request {i+1}: {method} {url}")
                    
            except Exception as e:
                logger.exception(f"Error during discovery: {str(e)}")
                
            finally:
                # Restore the original method
                DiscoveryManager._discover_shopify_products = original_discover_shopify
                await discovery_manager.close()
                logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(debug_shopify_discovery())