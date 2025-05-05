# tests/test_discoverers.py
import sys
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import json
import os
from pathlib import Path
from common.product_classifier import is_likely_coffee_product

# Import modules to test
from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.discoverers.sitemap_discoverer import SitemapDiscoverer
from scrapers.discoverers.html_discoverer import HtmlDiscoverer
from scrapers.discoverers.structured_data_discoverer import StructuredDataDiscoverer
from scrapers.discoverers.crawl4ai_discoverer import Crawl4AIDiscoverer
from scrapers.platform_detector import PlatformDetector
from common.utils import load_from_cache, save_to_cache

# Setup test directory for fixtures
TEST_DIR = Path(__file__).parent
FIXTURES_DIR = TEST_DIR / "fixtures"
os.makedirs(FIXTURES_DIR, exist_ok=True)


# Mock responses
@pytest.fixture
def shopify_products_json():
    return {
        "products": [
            {
                "id": 123456789,
                "title": "Ethiopia Yirgacheffe",
                "body_html": "<p>A fruity, floral coffee from Ethiopia.</p>",
                "vendor": "Test Roaster",
                "product_type": "Coffee",
                "handle": "ethiopia-yirgacheffe",
                "images": [{"src": "https://example.com/test.jpg"}],
                "variants": [
                    {"title": "250g", "price": "550.00", "available": True},
                    {"title": "500g", "price": "1000.00", "available": True},
                ]
            },
            {
                "id": 987654321,
                "title": "Colombia Supremo",
                "body_html": "<p>A smooth, balanced coffee from Colombia.</p>",
                "vendor": "Test Roaster",
                "product_type": "Coffee",
                "handle": "colombia-supremo",
                "images": [{"src": "https://example.com/test2.jpg"}],
                "variants": [
                    {"title": "250g", "price": "500.00", "available": True},
                ]
            },
            {
                "id": 111222333,
                "title": "Coffee Mug",
                "body_html": "<p>A ceramic mug for your coffee.</p>",
                "vendor": "Test Roaster",
                "product_type": "Merchandise",
                "handle": "coffee-mug",
                "images": [{"src": "https://example.com/mug.jpg"}],
                "variants": [
                    {"title": "Standard", "price": "250.00", "available": True},
                ]
            }
        ]
    }


@pytest.fixture
def sitemap_xml():
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
  <url>
    <loc>https://example.com/</loc>
    <lastmod>2025-01-01</lastmod>
  </url>
  <url>
    <loc>https://example.com/products/arabica-blend</loc>
    <lastmod>2025-01-02</lastmod>
    <image:image>
      <image:loc>https://example.com/image1.jpg</image:loc>
      <image:title>Arabica Blend</image:title>
    </image:image>
  </url>
  <url>
    <loc>https://example.com/products/robusta-single-origin</loc>
    <lastmod>2025-01-03</lastmod>
    <image:image>
      <image:loc>https://example.com/image2.jpg</image:loc>
      <image:title>Robusta Single Origin</image:title>
    </image:image>
  </url>
  <url>
    <loc>https://example.com/blog/coffee-brewing</loc>
    <lastmod>2025-01-04</lastmod>
  </url>
</urlset>"""


@pytest.fixture
def product_html():
    return """<!DOCTYPE html>
<html>
<head>
    <title>Dark Roast Coffee - Example Roaster</title>
    <meta name="description" content="Our signature dark roast coffee.">
    <meta property="og:image" content="https://example.com/dark-roast.jpg">
</head>
<body>
    <h1 class="product-title">Dark Roast Coffee</h1>
    <div class="product-description">
        <p>A bold, rich dark roast coffee with notes of chocolate and caramel.</p>
    </div>
    <div class="product-details">
        <div class="detail-item">
            <span class="detail-name">Roast Level:</span>
            <span class="detail-value">Dark</span>
        </div>
        <div class="detail-item">
            <span class="detail-name">Origin:</span>
            <span class="detail-value">Brazil</span>
        </div>
        <div class="detail-item">
            <span class="detail-name">Process:</span>
            <span class="detail-value">Natural</span>
        </div>
    </div>
    <div class="price">₹550</div>
    <div class="related-products">
        <h3>You might also like</h3>
        <div class="product-grid">
            <div class="product-item">
                <a href="/products/medium-roast">Medium Roast Coffee</a>
            </div>
            <div class="product-item">
                <a href="/products/espresso-blend">Espresso Blend</a>
            </div>
        </div>
    </div>
</body>
</html>"""


@pytest.fixture
def structured_data_html():
    return """<!DOCTYPE html>
<html>
<head>
    <title>Light Roast Coffee - Example Roaster</title>
    <script type="application/ld+json">
    {
        "@context": "https://schema.org/",
        "@type": "Product",
        "name": "Light Roast Coffee",
        "description": "A bright, fruity light roast coffee with notes of berries and citrus.",
        "image": "https://example.com/light-roast.jpg",
        "offers": {
            "@type": "Offer",
            "price": "500.00",
            "priceCurrency": "INR",
            "availability": "https://schema.org/InStock"
        }
    }
    </script>
</head>
<body>
    <h1>Light Roast Coffee</h1>
    <p>A bright, fruity light roast coffee with notes of berries and citrus.</p>
</body>
</html>"""


@pytest.fixture
def html_with_products():
    return """<!DOCTYPE html>
<html>
<head>
    <title>Our Coffee - Example Roaster</title>
</head>
<body>
    <h1>Our Coffee Selection</h1>
    <div class="product-grid">
        <div class="product-item">
            <a href="/products/light-roast">
                <img src="/images/light-roast.jpg" alt="Light Roast Coffee">
                <h3>Light Roast Coffee</h3>
            </a>
            <p>A bright, fruity coffee</p>
            <span class="price">₹500</span>
        </div>
        <div class="product-item">
            <a href="/products/medium-roast">
                <img src="/images/medium-roast.jpg" alt="Medium Roast Coffee">
                <h3>Medium Roast Coffee</h3>
            </a>
            <p>A balanced, smooth coffee</p>
            <span class="price">₹525</span>
        </div>
        <div class="product-item">
            <a href="/products/dark-roast">
                <img src="/images/dark-roast.jpg" alt="Dark Roast Coffee">
                <h3>Dark Roast Coffee</h3>
            </a>
            <p>A bold, rich coffee</p>
            <span class="price">₹550</span>
        </div>
        <div class="product-item">
            <a href="/products/coffee-mug">
                <img src="/images/mug.jpg" alt="Coffee Mug">
                <h3>Coffee Mug</h3>
            </a>
            <p>A ceramic mug</p>
            <span class="price">₹250</span>
        </div>
    </div>
</body>
</html>"""


@pytest.fixture
def woocommerce_products_json():
    return [
        {
            "id": 123,
            "name": "Ethiopian Yirgacheffe",
            "slug": "ethiopian-yirgacheffe",
            "permalink": "https://example.com/product/ethiopian-yirgacheffe/",
            "description": "<p>A fruity, floral coffee from Ethiopia.</p>",
            "categories": [
                {"id": 11, "name": "Coffee"}
            ],
            "images": [{"src": "https://example.com/test.jpg"}],
            "attributes": [
                {"name": "Roast", "options": ["Light"]},
                {"name": "Process", "options": ["Washed"]}
            ],
            "variations": [111, 112]
        },
        {
            "id": 456,
            "name": "Coffee Brewing Kit",
            "slug": "coffee-brewing-kit",
            "permalink": "https://example.com/product/coffee-brewing-kit/",
            "description": "<p>Complete kit for brewing coffee.</p>",
            "categories": [
                {"id": 12, "name": "Equipment"}
            ],
            "images": [{"src": "https://example.com/kit.jpg"}]
        }
    ]

@pytest.fixture
def mock_crawler():
    with patch('crawl4ai.AsyncWebCrawler') as mock_crawler_class:
        mock_crawler = MagicMock()
        mock_crawler_class.return_value.__aenter__.return_value = mock_crawler
        
        # Create mock result
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.markdown = MagicMock()
        mock_result.markdown.fit_markdown = "Mocked markdown content"
        mock_result.html = "<html>Mocked HTML content</html>"
        mock_result.extracted_content = json.dumps({"test": "data"})
        
        mock_crawler.arun.return_value = mock_result
        
        yield mock_crawler


import pytest
from unittest.mock import AsyncMock
from scrapers.platform_detector import PlatformDetector

import pytest
from scrapers.platform_detector import PlatformDetector

@pytest.mark.skipif(sys.platform == "win32", reason="Windows + Proactor loop bug")
@pytest.mark.asyncio
async def test_platform_detector():
    async with PlatformDetector() as detector:
        # Test Shopify detection
        html_shopify = """
            <html><head><link rel="stylesheet" href="//cdn.shopify.com/s/files/1/0123/4567/t/1/assets/theme.css"></head></html>
        """
        result = await detector.detect("https://example.com", html=html_shopify)
        assert result["platform"] == "shopify"

        # Test WooCommerce detection
        html_woocommerce = """
            <html><body class="woocommerce"><div class="woocommerce-product"></div></body></html>
        """
        result = await detector.detect("https://example.com", html=html_woocommerce)
        assert result["platform"] == "woocommerce"

        # Test static/generic site
        html_static = """
            <html><body>Just a normal site with no platform indicators.</body></html>
        """
        result = await detector.detect("https://example.com", html=html_static)
        assert result["platform"] == "static"


@pytest.mark.asyncio
async def test_shopify_discovery(mock_aiohttp_client, shopify_products_json):
    mock_session, mock_resp = mock_aiohttp_client
    mock_resp.json.return_value = shopify_products_json
    
    discovery_manager = DiscoveryManager()
    
    roaster = {
        "name": "Test Roaster",
        "website_url": "https://example.com",
        "slug": "test-roaster",
        "_platform": {"platform": "shopify"}
    }
    
    products = await discovery_manager._discover_shopify_products("https://example.com")
    
    assert len(products) == 2  # Should only include coffee products, not merchandise
    assert products[0]["name"] == "Ethiopia Yirgacheffe"
    assert products[1]["name"] == "Colombia Supremo"
    
    await discovery_manager.close()


@pytest.mark.asyncio
async def test_woocommerce_discovery(mock_aiohttp_client, woocommerce_products_json):
    mock_session, mock_resp = mock_aiohttp_client
    mock_resp.json.return_value = woocommerce_products_json
    
    discovery_manager = DiscoveryManager()
    
    roaster = {
        "name": "Test Roaster",
        "website_url": "https://example.com",
        "slug": "test-roaster",
        "_platform": {"platform": "woocommerce"}
    }
    
    products = await discovery_manager._discover_woocommerce_products("https://example.com")
    
    assert len(products) == 1  # Should only include coffee products
    assert products[0]["name"] == "Ethiopian Yirgacheffe"
    
    await discovery_manager.close()


@pytest.mark.asyncio
async def test_sitemap_discoverer(mock_aiohttp_client, sitemap_xml, product_html):
    mock_session, mock_resp = mock_aiohttp_client
    
    # Set up responses
    mock_resp.text.side_effect = [
        sitemap_xml,  # First response for sitemap.xml
        product_html,  # Second response for product page
        product_html   # Third response for another product page
    ]
    
    sitemap_discoverer = SitemapDiscoverer()
    
    products = await sitemap_discoverer.discover("https://example.com")
    
    assert len(products) > 0
    # At least the product URLs from the sitemap should be found
    assert any(p.get("direct_buy_url") == "https://example.com/products/arabica-blend" for p in products)
    assert any(p.get("direct_buy_url") == "https://example.com/products/robusta-single-origin" for p in products)
    
    await sitemap_discoverer.close()


@pytest.mark.asyncio
async def test_structured_data_discoverer(mock_aiohttp_client, structured_data_html):
    mock_session, mock_resp = mock_aiohttp_client
    mock_resp.text.return_value = structured_data_html
    
    structured_data_discoverer = StructuredDataDiscoverer()
    
    products = await structured_data_discoverer.discover("https://example.com")
    
    assert len(products) > 0
    assert products[0]["name"] == "Light Roast Coffee"
    assert products[0]["description"] == "A bright, fruity light roast coffee with notes of berries and citrus."
    assert products[0]["image_url"] == "https://example.com/light-roast.jpg"
    
    await structured_data_discoverer.close()


@pytest.mark.asyncio
async def test_html_discoverer(mock_aiohttp_client, html_with_products, product_html):
    mock_session, mock_resp = mock_aiohttp_client
    
    # Set up responses
    mock_resp.text.side_effect = [
        html_with_products,  # First response for main page
        product_html,        # Subsequent responses for product pages
        product_html,
        product_html
    ]
    
    html_discoverer = HtmlDiscoverer()
    
    products = await html_discoverer.discover("https://example.com")
    
    assert len(products) > 0
    # Should find coffee products but not the mug
    assert any(p.get("name") == "Light Roast Coffee" for p in products)
    assert any(p.get("name") == "Medium Roast Coffee" for p in products)
    assert any(p.get("name") == "Dark Roast Coffee" for p in products)
    assert not any(p.get("name") == "Coffee Mug" for p in products)
    
    await html_discoverer.close()


@pytest.mark.asyncio
async def test_crawl4ai_discoverer(mock_crawler):
    crawl4ai_discoverer = Crawl4AIDiscoverer()
    
    products = await crawl4ai_discoverer.discover("https://example.com")
    
    # The mock doesn't return actual products, but this tests that the function runs
    assert isinstance(products, list)


@pytest.mark.asyncio
async def test_discovery_manager_full_flow(mock_aiohttp_client, mock_crawler, shopify_products_json):
    mock_session, mock_resp = mock_aiohttp_client
     # Mock Shopify products
    mock_resp.json.return_value = shopify_products_json
    
    # Patch the detect method to avoid real network call
    with patch.object(PlatformDetector, 'detect', new=AsyncMock(return_value={
        "platform": "shopify",
        "api_endpoints": ["/products.json"]
    })):
   
    
    # Set up the discovery manager
        discovery_manager = DiscoveryManager(refresh_cache=True)
    
        roaster = {
            "name": "Test Roaster",
            "website_url": "https://example.com",
            "slug": "test-roaster"
        }
    
    products = await discovery_manager.discover_products(roaster)
    
    assert len(products) > 0
    assert all(p.get("roaster_name") == "Test Roaster" for p in products)
    assert all(p.get("roaster_slug") == "test-roaster" for p in products)
    
    await discovery_manager.close()


@pytest.mark.asyncio
async def test_discovery_manager_with_cache(mock_aiohttp_client):
    # Create a test cache file
    cache_data = [
        {
            "name": "Cached Coffee",
            "slug": "cached-coffee",
            "direct_buy_url": "https://example.com/product/cached-coffee",
            "roaster_name": "Test Roaster",
            "roaster_slug": "test-roaster"
        }
    ]
    
    # Save to cache
    os.makedirs(os.path.join(os.getcwd(), "data", "cache", "products"), exist_ok=True)
    save_to_cache("products_test-roaster", cache_data, "products")
    
    # Test with cache
    discovery_manager = DiscoveryManager()
    
    roaster = {
        "name": "Test Roaster",
        "website_url": "https://example.com",
        "slug": "test-roaster"
    }
    
    products = await discovery_manager.discover_products(roaster)
    
    assert len(products) == 1
    assert products[0]["name"] == "Cached Coffee"
    
    # Clean up cache
    cache_path = os.path.join(os.getcwd(), "data", "cache", "products", "products_test-roaster.json")
    if os.path.exists(cache_path):
        os.remove(cache_path)
    
    await discovery_manager.close()


def test_is_coffee_product():
    # --- Test Cases: Should be INCLUDED ---
    # 1. Clear coffee keywords, no negative keywords
    assert is_likely_coffee_product(name="Ethiopia Yirgacheffe Coffee Beans") is True
    assert is_likely_coffee_product(name="Dark Roast Blend") is True
    assert is_likely_coffee_product(name="Monsooned Malabar Arabica") is True
    assert is_likely_coffee_product(description="Rich espresso blend") is True
    assert is_likely_coffee_product(name="Coffee Sample Pack", description="Try our best beans") is True # Sample packs often contain coffee

    # 2. Ambiguous name, but NO negative keywords/patterns (NEW LOGIC: should be included)
    assert is_likely_coffee_product(name="Morning Ritual") is True
    assert is_likely_coffee_product(name="House Special", url="/product/house-special") is True
    assert is_likely_coffee_product(name="Festive Delight", categories=["Gifts"]) is True # Ambiguous category, no negative keywords

    # --- Test Cases: Should be EXCLUDED ---
    # 1. Clear non-product keywords in name/description/category
    assert is_likely_coffee_product(name="Coffee Mug") is False
    assert is_likely_coffee_product(name="Cool Beans T-Shirt", categories=["Merchandise"]) is False
    assert is_likely_coffee_product(name="Premium Coffee Grinder") is False
    assert is_likely_coffee_product(name="Monthly Coffee Subscription") is False
    assert is_likely_coffee_product(description="Sign up for our brewing workshop") is False
    assert is_likely_coffee_product(name="Gift Card", categories=["Gifts"]) is False # Specific non-product keyword

    # 2. Non-product URL patterns
    assert is_likely_coffee_product(name="Our Story", url="/about-us") is False
    assert is_likely_coffee_product(name="Contact Information", url="/pages/contact") is False
    assert is_likely_coffee_product(name="Latest News", url="/blog/exciting-updates") is False
    assert is_likely_coffee_product(name="All Equipment", url="/collections/equipment") is False
    assert is_likely_coffee_product(name="Roasting Course", url="/workshops/learn-to-roast") is False

    # 3. Mixed signals (should prioritize exclusion rule)
    assert is_likely_coffee_product(name="Coffee Brewing Kit", url="/products/brewing-kit") is False # 'kit' and 'brewing' are negative
    assert is_likely_coffee_product(name="Espresso Machine Bundle", description="Includes free beans!") is False # 'machine' is negative