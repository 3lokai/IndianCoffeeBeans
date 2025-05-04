"""
Platform detection for coffee roaster websites.
Identifies the e-commerce platform a website is running on.
"""
import aiohttp
import logging
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional, Literal, List
import asyncio
from config import USER_AGENT, REQUEST_TIMEOUT, PLATFORM_SPECIFIC

logger = logging.getLogger(__name__)

# Define platform types
PlatformType = Literal[
    "shopify", "woocommerce", "bigcommerce", "magento", "wordpress",
    "static", "framer", "webflow", "squarespace", "wix", "custom", "unknown"
]

class PlatformDetector:
    """Detects e-commerce platform from website URL or content"""
    
    def __init__(self):
        """Initialize platform detector"""
        self.session = None  # Initialize as None first
        
        # Then initialize the session properly through the correct method
        # Do not try to access self.session here directly
          
    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
        return self.session
    
    async def detect_from_url(self, url: str) -> Optional[PlatformType]:
        """
        Attempt to detect platform from URL patterns
        
        Args:
            url (str): Website URL
            
        Returns:
            Optional[PlatformType]: Detected platform or None
        """
        if any(x in url for x in ["/products/", "/collections/", "myshopify.com"]):
            return "shopify"
        if "/catalog/product/" in url:
            return "magento"
        if any(x in url for x in ["/product/", "/product-category/", "/shop/"]):
            return "woocommerce"
        if "/bc-product/" in url:
            return "bigcommerce"
        return None
    
    async def detect_from_content(self, url: str, html: Optional[str] = None, crawler=None) -> PlatformType:
        """
        Attempt to detect platform from page content using text and DOM parsing
        
        Args:
            url (str): Website URL
            html (Optional[str]): HTML content if already fetched
            crawler: Optional crawler instance to use
            
        Returns:
            PlatformType: Detected platform
        """
        try:
            # Use provided HTML or fetch it
            if html is None:
                # If a crawler is provided, use it (to benefit from caching)
                if crawler:
                    result = await crawler.arun(url=url)
                    if result and result.success:
                        html = str(result.html)
                    else:
                        return "unknown"
                else:
                    session = await self._get_session()
                    async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                        if response.status != 200:
                            return "unknown"
                        html = await response.text()
            
            # --- Content-based heuristics ---
            # Shopify
            if 'cdn.shopify.com' in html or 'data-shopify' in html:
                return "shopify"
            # WooCommerce
            if 'woocommerce' in html:
                return "woocommerce"
            # Magento
            if 'Magento' in html or 'mage-' in html:
                return "magento"
            # WordPress
            if 'wp-content' in html or 'wp-includes' in html:
                return "wordpress"
            # Framer
            if 'framer.com' in html or 'framerusercontent.com' in html:
                return "framer"
            # Webflow
            if 'webflow.com' in html:
                return "webflow"
            # Squarespace
            if 'squarespace.com' in html:
                return "squarespace"
            # Wix
            if 'wix.com' in html or 'wixsite.com' in html:
                return "wix"
            
            # --- DOM-based heuristics ---
            soup = BeautifulSoup(html, 'html.parser')
            
            # Shopify
            if soup.find('script', src=lambda x: x and 'cdn.shopify.com' in x):
                return "shopify"
            if soup.find(attrs={"data-shopify": True}):
                return "shopify"
            
            # WooCommerce
            if soup.find('body', class_=lambda c: c and 'woocommerce' in c):
                return "woocommerce"
            if soup.find('link', href=lambda x: x and 'woocommerce' in x):
                return "woocommerce"
            
            # Magento
            if soup.find('meta', attrs={"name": "generator", "content": lambda x: x and 'Magento' in x}):
                return "magento"
            
            # WordPress
            if soup.find('meta', attrs={"name": "generator", "content": lambda x: x and 'WordPress' in x}):
                return "wordpress"
            if soup.find(class_=lambda x: x and 'wp-' in x):
                return "wordpress"
            
            return "static"
        except Exception as e:
            logger.warning(f"Error detecting platform from content for {url}: {e}")
            return "unknown"
    
    async def detect(self, url: str, html: Optional[str] = None, crawler=None) -> Dict[str, Any]:
        """
        Detect platform using both URL and content heuristics
        
        Args:
            url (str): Website URL
            html (Optional[str]): HTML content if already fetched
            crawler: Optional crawler instance to use
            
        Returns:
            Dict[str, Any]: Platform information with type and potential API endpoints
        """
        # First try from URL
        platform = await self.detect_from_url(url)
        
        # If that fails, try from content
        if not platform:
            platform = await self.detect_from_content(url, html, crawler)
        
        # If we still don't have a result, default to custom
        if not platform:
            platform = "custom"
        
        # Get API endpoints for this platform
        api_endpoints = self.get_api_endpoints(platform)
        
        # Get structured data paths
        data_paths = self.get_structured_data_paths(platform)
        
        return {
            "platform": platform,
            "api_endpoints": api_endpoints,
            "structured_data_paths": data_paths
        }
    
    def get_api_endpoints(self, platform: PlatformType) -> List[str]:
        """
        Get potential API endpoints for a given platform.
        
        Args:
            platform (PlatformType): Detected platform
            
        Returns:
            List[str]: List of API endpoints to try
        """
        endpoints = {
            "shopify": [
                "/products.json",
                "/collections/all/products.json",
                "/collections/all.json"
            ],
            "woocommerce": [
                "/wp-json/wc/v3/products",
                "/wp-json/wc/store/products"
            ],
            "wordpress": [
                "/wp-json/wp/v2/posts",
                "/wp-json/wp/v2/pages"
            ],
            "magento": [
                "/rest/V1/products"
            ]
        }
        
        return endpoints.get(platform, [])
    
    def get_structured_data_paths(self, platform: PlatformType) -> List[str]:
        """
        Get recommended paths for structured data based on platform.
        
        Args:
            platform (PlatformType): Detected platform
            
        Returns:
            List[str]: List of relevant paths to check for structured data
        """
        paths = {
            "shopify": [
                "//script[@type='application/ld+json']",
                "//meta[@property='og:type']"
            ],
            "woocommerce": [
                "//script[@type='application/ld+json']",
                "//div[@class='product']//script[@type='application/ld+json']"
            ],
            "wordpress": [
                "//script[@type='application/ld+json']"
            ],
            "custom": [
                "//script[@type='application/ld+json']",
                "//meta[@property='og:type']"
            ]
        }
        
        return paths.get(platform, paths["custom"])
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()