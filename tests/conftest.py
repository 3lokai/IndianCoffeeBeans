import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
import asyncio

# Expose MockResponse and MockSession for test usage
class MockResponse:
    def __init__(self, text_data=None, json_data=None, status=200, headers=None):
        self._text = text_data
        self._json = json_data
        self.status = status
        self.headers = headers or {"Content-Type": "text/html"}
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
        self.responses = responses
        self.request_count = 0
        self.closed = False
        self.default_response = MockResponse(json_data={})
    def get(self, *args, **kwargs):
        if not self.responses:
            resp = self.default_response
        elif self.request_count < len(self.responses):
            resp = self.responses[self.request_count]
        else:
            resp = self.responses[-1]
        self.request_count += 1
        return resp
    def head(self, *args, **kwargs):
        if not self.responses:
            resp = self.default_response
        elif self.request_count < len(self.responses):
            resp = self.responses[self.request_count]
        else:
            resp = self.responses[-1]
        self.request_count += 1
        return resp
    async def close(self):
        self.closed = True
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

@pytest.fixture
def mock_aiohttp_client(monkeypatch):
    responses = []
    mock_session = MockSession(responses)
    def make_mock_session(*args, **kwargs):
        return mock_session
    monkeypatch.setattr("aiohttp.ClientSession", make_mock_session)
    # For backwards compatibility, allow test to use .responses, or unpack as (session, resp)
    mock_resp = MagicMock(spec=MockResponse)
    mock_aiohttp_client.responses = responses
    mock_aiohttp_client.mock_session = mock_session
    mock_aiohttp_client.mock_resp = mock_resp
    # Allow unpacking for tests that expect (session, resp)
    def unpackable():
        return (mock_session, mock_resp)
    mock_aiohttp_client.__iter__ = lambda self=mock_aiohttp_client: iter((mock_session, mock_resp))
    return mock_aiohttp_client

# Mock for Crawl4AI library
class MockCrawlResult:
    """Mock for a Crawl4AI crawl result"""
    def __init__(self, url, html, success=True, depth=0):
        self.url = url
        self.html = html
        self.success = success
        self.metadata = {"depth": depth}
        # Create a mock for markdown object that has fit_markdown attribute
        self.markdown = MagicMock()
        self.markdown.fit_markdown = f"Mock markdown for {url}"
        # Mock extracted content (JSON string)
        self.extracted_content = "{}"

@pytest.fixture
def mock_crawler():
    """
    Fixture that provides a mock for AsyncWebCrawler
    """
    with patch('crawl4ai.AsyncWebCrawler') as mock_crawler_class:
        mock_crawler = AsyncMock()
        mock_crawler_class.return_value.__aenter__.return_value = mock_crawler
        
        # Default behavior - will need to be customized in specific tests
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.html = "<html>Mocked HTML content</html>"
        mock_result.url = "https://example.com"
        mock_result.metadata = {"depth": 0}
        mock_result.markdown = MagicMock()
        mock_result.markdown.fit_markdown = "Mocked markdown content"
        mock_result.extracted_content = json.dumps({"test": "data"})
        
        mock_crawler.arun.return_value = mock_result
        
        yield mock_crawler

@pytest.fixture
def mock_deep_crawl_results():
    """
    Fixture that provides pre-configured mock results for deep crawling tests
    """
    # Create mock results for a product listing page and a product page
    listing_page = MockCrawlResult(
        url="https://example.com/products",
        html="""
        <html>
            <body>
                <div class="product-grid">
                    <div class="product-item">
                        <a href="/products/coffee1">Coffee 1</a>
                        <img src="/images/coffee1.jpg" alt="Coffee 1">
                    </div>
                    <div class="product-item">
                        <a href="/products/coffee2">Coffee 2</a>
                        <img src="/images/coffee2.jpg" alt="Coffee 2">
                    </div>
                </div>
            </body>
        </html>
        """,
        depth=0
    )
    
    product_page1 = MockCrawlResult(
        url="https://example.com/products/coffee1",
        html="""
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "@context": "https://schema.org/",
                    "@type": "Product",
                    "name": "Ethiopian Yirgacheffe",
                    "description": "A fruity coffee with floral notes",
                    "image": "/images/coffee1.jpg",
                    "url": "/products/coffee1"
                }
                </script>
            </head>
            <body>
                <h1>Ethiopian Yirgacheffe</h1>
                <p>A fruity coffee with floral notes</p>
                <div class="price">₹550</div>
            </body>
        </html>
        """,
        depth=1
    )
    
    product_page2 = MockCrawlResult(
        url="https://example.com/products/coffee2",
        html="""
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "@context": "https://schema.org/",
                    "@type": "Product",
                    "name": "Colombia Supremo",
                    "description": "A balanced coffee with chocolate notes",
                    "image": "/images/coffee2.jpg",
                    "url": "/products/coffee2"
                }
                </script>
            </head>
            <body>
                <h1>Colombia Supremo</h1>
                <p>A balanced coffee with chocolate notes</p>
                <div class="price">₹500</div>
            </body>
        </html>
        """,
        depth=1
    )
    
    return [listing_page, product_page1, product_page2]

@pytest.fixture
def roaster_data():
    """Provides sample roaster data for tests."""
    return {
        "name": "Blue Tokai Coffee Roasters",
        "website_url": "https://bluetokaicoffee.com/",
        "slug": "blue-tokai-coffee-roasters",
        "instagram": "https://www.instagram.com/bluetokaicoffee/"
    }

@pytest.fixture
def mock_html():
    """Provides sample HTML content for tests."""
    return """
    <html>
    <head><title>Test Page</title></head>
    <body>
        <h1>Welcome</h1>
        <p>This is a test.</p>
        <a href='/product1'>Product 1</a>
        <a href='/product2'>Product 2</a>
    </body>
    </html>
    """

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
def shopify_products_json():
    """Fixture for Shopify products API response."""
    return {
        "products": [
            {
                "id": 123456789,
                "title": "Ethiopia Yirgacheffe",
                "handle": "ethiopia-yirgacheffe",
                "body_html": "<p>A fruity, floral coffee from Ethiopia.</p>",
                "published_at": "2024-01-01T12:00:00-05:00",
                "product_type": "Coffee Beans",
                "vendor": "Test Roaster",
                "tags": ["Single Origin", "Light Roast", "Fruity"],
                "variants": [
                    {
                        "id": 987654321,
                        "price": "18.00",
                        "sku": "ETH-YIRG-12OZ",
                        "available": True,
                        "weight": 12,
                        "weight_unit": "oz"
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
                "vendor": "Test Roaster",
                "tags": ["Single Origin", "Medium Roast", "Nutty"],
                "variants": [
                    {
                        "id": 123456789,
                        "price": "16.50",
                        "sku": "BRA-SAN-1LB",
                        "available": True,
                        "weight": 1,
                        "weight_unit": "lb"
                    }
                ],
                "images": [
                    {"src": "https://example.com/santos.jpg"}
                ]
            },
            {
                "id": 111222333,
                "title": "Coffee Mug",
                "handle": "coffee-mug",
                "body_html": "<p>A sturdy ceramic mug for your coffee.</p>",
                "published_at": "2024-01-03T14:30:00-05:00",
                "product_type": "Merchandise",
                "vendor": "Test Roaster",
                "tags": ["Mug", "Ceramic"],
                "variants": [
                    {
                        "id": 444555666,
                        "price": "12.00",
                        "sku": "MUG-CLASSIC",
                        "available": True
                    }
                ],
                "images": [
                    {"src": "https://example.com/mug.jpg"}
                ]
            }
        ]
    }

@pytest.fixture
def woocommerce_products_json():
    """Fixture for WooCommerce products API response."""
    return [
        {
            "id": 123,
            "name": "Colombia Supremo",
            "slug": "colombia-supremo",
            "permalink": "https://example.com/product/colombia-supremo",
            "description": "<p>A balanced, rich coffee from Colombia.</p>",
            "short_description": "<p>Classic Colombian coffee.</p>",
            "price": "15.00",
            "regular_price": "15.00",
            "sale_price": "",
            "on_sale": False,
            "stock_status": "instock",
            "categories": [
                {"id": 1, "name": "Coffee"}
            ],
            "tags": [
                {"id": 10, "name": "Balanced"},
                {"id": 11, "name": "Rich"}
            ],
            "images": [
                {"src": "https://example.com/supremo.jpg"}
            ],
            "attributes": [
                {"name": "Roast", "options": ["Medium"]},
                {"name": "Process", "options": ["Washed"]}
            ]
        },
        {
            "id": 456,
            "name": "Coffee Brewing Kit",
            "slug": "coffee-brewing-kit",
            "permalink": "https://example.com/product/coffee-brewing-kit",
            "description": "<p>Complete kit for brewing coffee.</p>",
            "short_description": "<p>Everything you need.</p>",
            "price": "75.00",
            "regular_price": "75.00",
            "sale_price": "",
            "on_sale": False,
            "stock_status": "instock",
            "categories": [
                {"id": 3, "name": "Equipment"}
            ],
            "tags": [
                {"id": 12, "name": "Kit"},
                {"id": 13, "name": "Brewing"}
            ],
            "images": [
                {"src": "https://example.com/kit.jpg"}
            ],
            "attributes": []
        }
    ]