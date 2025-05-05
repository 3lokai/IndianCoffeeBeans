# tests/mock_crawl4ai.py
"""
Mock objects for testing with Crawl4AI library
"""

import pytest
from unittest.mock import MagicMock, AsyncMock

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
    
    Use this in tests that interact with Crawl4AI:
    
    @pytest.mark.asyncio
    async def test_something(mock_crawler):
        # Configure mock return values
        mock_crawler.arun.return_value = [
            MockCrawlResult("https://example.com", "<html>Product page</html>")
        ]
        # Use the component that needs a crawler
        result = await your_function()
        # Assert the crawler was used correctly
        mock_crawler.arun.assert_called_once()
    """
    with pytest.MonkeyPatch().context() as mp:
        # Create mock for AsyncWebCrawler
        mock_crawler_class = MagicMock()
        mock_crawler = AsyncMock()
        
        # Setup the enter/exit for context manager
        mock_crawler_class.return_value.__aenter__.return_value = mock_crawler
        mock_crawler_class.return_value.__aexit__.return_value = None
        
        # Add the mock run method
        mock_crawler.arun = AsyncMock()
        
        # Apply the mock to the module
        mp.setattr("crawl4ai.AsyncWebCrawler", mock_crawler_class)
        
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