# tests/patch_test.py
import asyncio
import logging
import json
from unittest.mock import patch, MagicMock, AsyncMock

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Test data
SAMPLE_DATA = {
    "products": [
        {
            "title": "Test Coffee",
            "handle": "test-coffee",
            "body_html": "<p>Test description</p>",
            "variants": [{"price": "10.00"}],
            "images": [{"src": "https://example.com/test.jpg"}]
        }
    ]
}

class MockResponse:
    def __init__(self, status=200, json_data=None):
        self.status = status
        self._json = json_data

    async def json(self):
        return self._json

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

class MockClientSession:
    def __init__(self, responses=None):
        self.responses = responses or []
        self.current_response = 0
        self.requests = []

    def get(self, url, *args, **kwargs):
        self.requests.append((url, args, kwargs))
        logger.debug(f"MockClientSession.get called with URL: {url}")
        
        if self.current_response < len(self.responses):
            resp = self.responses[self.current_response]
            self.current_response += 1
            return resp
        return MockResponse(json_data={})

    async def close(self):
        logger.debug("MockClientSession.close called")
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

# The function we're trying to test - simplified version of _discover_shopify_products
async def fetch_shopify_products(base_url):
    import aiohttp
    
    logger.debug(f"fetch_shopify_products called with base_url={base_url}")
    products = []
    
    # Create session and make request
    async with aiohttp.ClientSession() as session:
        products_url = f"{base_url}/products.json?limit=250&page=1"
        logger.debug(f"Making request to {products_url}")
        
        async with session.get(products_url) as response:
            logger.debug(f"Got response with status {response.status}")
            
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Response data: {json.dumps(data)[:100]}...")
                
                batch_products = data.get('products', [])
                logger.debug(f"Found {len(batch_products)} products")
                
                for product in batch_products:
                    name = product.get('title')
                    if name:
                        handle = product.get('handle', '')
                        product_url = f"{base_url}/products/{handle}"
                        
                        products.append({
                            "name": name,
                            "direct_buy_url": product_url,
                            "description": product.get('body_html', '')
                        })
                        
                        logger.debug(f"Added product: {name}")
    
    logger.debug(f"Returning {len(products)} products")
    return products

async def test_with_mock():
    """Test with manually created mock"""
    logger.info("=== Testing with manually created mock ===")
    
    # Create mock responses
    mock_resp = MockResponse(json_data=SAMPLE_DATA)
    mock_session = MockClientSession(responses=[mock_resp])
    
    # Patch aiohttp.ClientSession with our mock
    with patch('aiohttp.ClientSession', return_value=mock_session):
        # Call the function
        products = await fetch_shopify_products("https://example.com")
        
        # Log results
        logger.info(f"Got {len(products)} products")
        for product in products:
            logger.info(f"Product: {product['name']}")
        
        # Log requests made to mock
        logger.info(f"Requests made to mock session: {len(mock_session.requests)}")
        for url, args, kwargs in mock_session.requests:
            logger.info(f"Request: GET {url}")

async def test_with_pytest_mock():
    """Test with pytest-style mock (fixed to use MockClientSession for async context)"""
    logger.info("\n=== Testing with pytest-style mock ===")
    
    # Create mock responses
    mock_resp = MockResponse(json_data=SAMPLE_DATA)
    mock_session = MockClientSession(responses=[mock_resp])

    # Patch aiohttp.ClientSession with our async-compatible mock
    with patch('aiohttp.ClientSession', return_value=mock_session):
        # Call the function
        products = await fetch_shopify_products("https://example.com")
        
        # Log results
        logger.info(f"Got {len(products)} products")
        for product in products:
            logger.info(f"Product: {product['name']}")
        
        # Log requests made to mock
        logger.info(f"Requests made to mock session: {len(mock_session.requests)}")
        for url, args, kwargs in mock_session.requests:
            logger.info(f"Request: GET {url}")

async def main():
    await test_with_mock()
    await test_with_pytest_mock()

if __name__ == "__main__":
    asyncio.run(main())