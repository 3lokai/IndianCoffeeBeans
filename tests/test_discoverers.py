# Add these imports at the top of your test_discoverers.py file
import sys
import pytest
import asyncio
from unittest.mock import patch, MagicMock
import json
import os
from pathlib import Path
from tests.conftest import MockResponse, MockSession

# Add missing imports for the discoverer classes
from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.discoverers.html_discoverer import HtmlDiscoverer

# At the beginning of your test file, add this helper function
# Updated: Use MockSession/MockResponse from conftest.py for aiohttp session mocking
def create_session_mock(responses):
    """Create a properly configured mock for aiohttp.ClientSession using MockSession."""
    return MockSession(responses)

# Now modify your test functions to use this helper:

@pytest.mark.asyncio
async def test_shopify_discovery(mock_aiohttp_client, shopify_products_json):
    # Create a proper session mock
    session_mock = create_session_mock([
        MockResponse(json_data=shopify_products_json),
        MockResponse(json_data={"products": []})
    ])
    
    # Patch aiohttp.ClientSession with our mock
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # Patch the product classifier to allow all test products
        with patch('common.product_classifier.is_likely_coffee_product', return_value=True):
            discovery_manager = DiscoveryManager()
            
            # Call the method
            products = await discovery_manager._discover_shopify_products("https://example.com")
            
            # Check results
            assert len(products) > 0
            assert "Ethiopia Yirgacheffe" in products[0]["name"]
            assert "direct_buy_url" in products[0]
            assert "platform" in products[0] and products[0]["platform"] == "shopify"
            
            await discovery_manager.close()

@pytest.mark.asyncio
async def test_woocommerce_discovery(mock_aiohttp_client, woocommerce_products_json):
    # Create a proper session mock
    session_mock = create_session_mock([
        MockResponse(json_data=woocommerce_products_json),
        MockResponse(json_data=[]),
        MockResponse(json_data=[])
    ])
    
    # Patch aiohttp.ClientSession with our mock
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # Patch the product classifier to allow all test products
        with patch('common.product_classifier.is_likely_coffee_product', return_value=True):
            discovery_manager = DiscoveryManager()
            
            # Call the method
            products = await discovery_manager._discover_woocommerce_products("https://example.com")
            
            # Check results
            assert len(products) > 0
            assert "Colombia Supremo" in products[0]["name"]
            assert "direct_buy_url" in products[0]
            assert "platform" in products[0] and products[0]["platform"] == "woocommerce"
            
            await discovery_manager.close()

@pytest.mark.asyncio
async def test_html_discoverer(mock_aiohttp_client, html_with_products, product_html):
    # Create a proper session mock with multiple responses
    session_mock = create_session_mock([
        MockResponse(text_data=html_with_products, headers={"Content-Type": "text/html"}),
        MockResponse(text_data=product_html, headers={"Content-Type": "text/html"}),
        MockResponse(text_data=product_html, headers={"Content-Type": "text/html"}),
        MockResponse(text_data=product_html, headers={"Content-Type": "text/html"})
    ])
    
    # Patch aiohttp.ClientSession with our mock
    with patch('aiohttp.ClientSession', return_value=session_mock):
        # No need to patch is_likely_coffee_product here, as filtering is done in DiscoveryManager, not html_discoverer
        # Patch _queue_additional_pages if needed (optional, can be left as is)
        html_discoverer = HtmlDiscoverer()
        
        # Call the method
        products = await html_discoverer.discover("https://example.com")
        
        # Check results - we expect products to contain at least the links from html_with_products
        assert len(products) > 0
        # Optionally, check that at least one product has expected structure
        assert any("coffee" in p["name"].lower() for p in products)
        
        await html_discoverer.close()