# tests/fixed_simple_tests.py
import sys
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import json
import os
import logging
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules to test
from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.discoverers.sitemap_discoverer import SitemapDiscoverer
from scrapers.discoverers.html_discoverer import HtmlDiscoverer
from scrapers.discoverers.structured_data_discoverer import StructuredDataDiscoverer
from scrapers.discoverers.crawl4ai_discoverer import Crawl4AIDiscoverer
from common.product_classifier import is_likely_coffee_product

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Sample test data
SHOPIFY_SAMPLE = {
    "products": [
        {
            "id": 123456789,
            "title": "Ethiopia Yirgacheffe",
            "handle": "ethiopia-yirgacheffe",
            "body_html": "<p>A fruity, floral coffee from Ethiopia.</p>",
            "product_type": "Coffee",
            "variants": [{"price": "550.00", "available": True}],
            "images": [{"src": "https://example.com/test.jpg"}]
        },
        {
            "id": 987654321,
            "title": "Brazil Santos",
            "handle": "brazil-santos",
            "body_html": "<p>A smooth, nutty coffee from Brazil.</p>",
            "product_type": "Coffee",
            "variants": [{"price": "500.00", "available": True}],
            "images": [{"src": "https://example.com/test2.jpg"}]
        }
    ]
}

WOO_SAMPLE = [
    {
        "id": 123,
        "name": "Colombia Supremo",
        "slug": "colombia-supremo",
        "permalink": "https://example.com/product/colombia-supremo",
        "description": "<p>A balanced coffee from Colombia.</p>",
        "categories": [{"id": 1, "name": "Coffee"}],
        "images": [{"src": "https://example.com/test.jpg"}]
    }
]

HTML_SAMPLE = """
<html>
<body>
    <div class="product-grid">
        <div class="product-item">
            <a href="/products/light-roast">Light Roast</a>
            <img src="/images/light-roast.jpg" alt="Light Roast">
        </div>
        <div class="product-item">
            <a href="/products/dark-roast">Dark Roast</a>
            <img src="/images/dark-roast.jpg" alt="Dark Roast">
        </div>
    </div>
</body>
</html>
"""

@pytest.mark.asyncio
async def test_shopify_discovery_direct():
    """Test Shopify discovery with direct mocking"""
    # Create a mock for ClientSession
    session_mock = AsyncMock()
    get_mock = AsyncMock()
    
    # Setup response for get method
    response_mock = AsyncMock()
    response_mock.status = 200
    response_mock.json.return_value = SHOPIFY_SAMPLE
    
    # Connect the mocks
    get_mock.return_value.__aenter__.return_value = response_mock
    session_mock.get = get_mock
    session_mock.__aenter__.return_value = session_mock

    # Create discoverer and patch relevant parts
    discovery_manager = DiscoveryManager()
    
    # Patch the session constructor
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # Patch product classifier to include all test products
        with patch('common.product_classifier.is_likely_coffee_product', return_value=True):
            # Call the method directly
            products = await discovery_manager._discover_shopify_products("https://example.com")
            
            # Log details
            logger.info(f"Got {len(products)} products from Shopify API")
            for i, p in enumerate(products):
                logger.info(f"Product {i+1}: {p.get('name')}")
            
            # Verify results
            assert len(products) > 0
            assert any("Ethiopia" in p.get('name', '') for p in products)
            assert all("direct_buy_url" in p for p in products)
            
            # Verify session was used
            session_mock.get.assert_called()
    
    await discovery_manager.close()

@pytest.mark.asyncio
async def test_woocommerce_discovery_direct():
    """Test WooCommerce discovery with direct mocking"""
    # Create a mock for ClientSession
    session_mock = AsyncMock()
    get_mock = AsyncMock()
    
    # Setup response for get method
    response_mock = AsyncMock()
    response_mock.status = 200
    response_mock.json.return_value = WOO_SAMPLE
    
    # Connect the mocks
    get_mock.return_value.__aenter__.return_value = response_mock
    session_mock.get = get_mock
    session_mock.__aenter__.return_value = session_mock

    # Create discoverer and patch relevant parts
    discovery_manager = DiscoveryManager()
    
    # Patch the session constructor
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # Patch product classifier to include all test products
        with patch('common.product_classifier.is_likely_coffee_product', return_value=True):
            # Call the method directly
            products = await discovery_manager._discover_woocommerce_products("https://example.com")
            
            # Log details
            logger.info(f"Got {len(products)} products from WooCommerce API")
            for i, p in enumerate(products):
                logger.info(f"Product {i+1}: {p.get('name')}")
            
            # Verify results
            assert len(products) > 0
            assert any("Colombia" in p.get('name', '') for p in products)
            assert all("direct_buy_url" in p for p in products)
            
            # Verify session was used
            session_mock.get.assert_called()
    
    await discovery_manager.close()

@pytest.mark.asyncio
async def test_html_discoverer_direct():
    """Test HTML discoverer with direct mocking"""
    # Create a mock for ClientSession
    session_mock = AsyncMock()
    get_mock = AsyncMock()
    
    # Setup response for get method
    response_mock = AsyncMock()
    response_mock.status = 200
    response_mock.text.return_value = HTML_SAMPLE
    response_mock.headers = {"Content-Type": "text/html"}
    
    # Connect the mocks
    get_mock.return_value.__aenter__.return_value = response_mock
    session_mock.get = get_mock
    session_mock.__aenter__.return_value = session_mock

    # Create discoverer
    html_discoverer = HtmlDiscoverer()
    
    # Patch session and discoverer internals
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # Patch product classifier to include all test products
        with patch('scrapers.discoverers.html_discoverer.is_likely_coffee_product', return_value=True):
            # Patch queue method to avoid trying to crawl more pages
            with patch.object(HtmlDiscoverer, '_queue_additional_pages', return_value=None):
                # Call the method directly
                products = await html_discoverer.discover("https://example.com")
                
                # Log details
                logger.info(f"Got {len(products)} products from HTML")
                for i, p in enumerate(products):
                    name = p.get('name') or ''
                    url = p.get('url') or p.get('direct_buy_url') or ''
                    logger.info(f"Product {i+1}: {name} ({url})")
                
                # Verify results
                assert len(products) > 0
                
                # Verify session was used
                session_mock.get.assert_called()
    
    await html_discoverer.close()

# Run the tests
if __name__ == "__main__":
    # Run each test separately
    asyncio.run(test_shopify_discovery_direct())
    asyncio.run(test_woocommerce_discovery_direct())
    asyncio.run(test_html_discoverer_direct())