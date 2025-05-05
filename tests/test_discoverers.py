# Add these imports at the top of your test_discoverers.py file
import sys
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import json
import os
from pathlib import Path
from tests.conftest import MockResponse

# Add missing imports for the discoverer classes
from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.discoverers.html_discoverer import HtmlDiscoverer

# At the beginning of your test file, add this helper function
def create_session_mock(responses):
    """Create a properly configured mock for aiohttp.ClientSession"""
    session_mock = AsyncMock()
    response_iterator = iter(responses)

    async def get_side_effect(url, *args, **kwargs): # Renamed for clarity
        """Side effect for session.get() call"""
        try:
            current_resp = next(response_iterator)
            resp = AsyncMock()
            resp.status = current_resp.status
            if hasattr(current_resp, '_json') and current_resp._json is not None:
                resp.json = AsyncMock(return_value=current_resp._json)
            if hasattr(current_resp, '_text') and current_resp._text is not None:
                resp.text = AsyncMock(return_value=current_resp._text)
            resp.headers = getattr(current_resp, 'headers', {})
            resp.__aenter__.return_value = resp # Configure context manager behavior
            resp.__aexit__.return_value = None
            return resp # Return the configured response mock
        except StopIteration:
            resp = AsyncMock()
            resp.status = 404
            resp.json = AsyncMock(return_value={})
            resp.text = AsyncMock(return_value="")
            resp.headers = {}
            resp.__aenter__.return_value = resp # Configure context manager behavior
            resp.__aexit__.return_value = None
            return resp

    # Configure session_mock.get
    # It needs to be an AsyncMock that executes our side_effect logic when called
    session_mock.get = AsyncMock(side_effect=get_side_effect)

    # Make the session usable as a context manager
    session_mock.__aenter__.return_value = session_mock
    session_mock.__aexit__.return_value = None

    return session_mock

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
        # Patch various methods to simplify testing
        with patch('scrapers.discoverers.html_discoverer.is_likely_coffee_product', return_value=True):
            with patch.object(HtmlDiscoverer, '_queue_additional_pages', return_value=None):
                html_discoverer = HtmlDiscoverer()
                
                # Call the method
                products = await html_discoverer.discover("https://example.com")
                
                # Check results - we expect products to contain at least the links from html_with_products
                assert len(products) > 0
                
                await html_discoverer.close()