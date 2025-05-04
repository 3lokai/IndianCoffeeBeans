# scrapers/discoverers/discovery_manager.py
import logging
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin

from scrapers.platform_detector import PlatformDetector
from scrapers.discoverers.sitemap_discoverer import SitemapDiscoverer
from scrapers.discoverers.html_discoverer import HtmlDiscoverer
from scrapers.discoverers.structured_data_discoverer import StructuredDataDiscoverer
from scrapers.discoverers.crawl4ai_discoverer import Crawl4AIDiscoverer
from common.utils import load_from_cache, save_to_cache, slugify
from config import CRAWL_DELAY

logger = logging.getLogger(__name__)

class DiscoveryManager:
    """
    Orchestrates the product discovery process using multiple discovery strategies
    based on the platform type and available data sources.
    """
    
    def __init__(self, db_client=None, refresh_cache=False):
        self.db_client = db_client
        self.refresh_cache = refresh_cache
        self.platform_detector = PlatformDetector()
        
        # Initialize discoverers
        self.sitemap_discoverer = SitemapDiscoverer()
        self.html_discoverer = HtmlDiscoverer()
        self.structured_data_discoverer = StructuredDataDiscoverer()
        self.crawl4ai_discoverer = Crawl4AIDiscoverer()
        
    async def discover_products(self, roaster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Discover product URLs for a roaster using multiple strategies,
        prioritized based on the platform and available data sources.
        
        Args:
            roaster: Dict containing roaster information with at least 'name' and 'website_url'
            
        Returns:
            List of discovered product URLs with basic metadata
        """
        name = roaster.get("name")
        website = roaster.get("website_url")
        
        if not name or not website:
            logger.error(f"Missing required fields for roaster: {roaster}")
            return []
            
        # Normalize website URL
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
            
        # Check cache first
        cache_key = f"products_{slugify(name)}"
        if not self.refresh_cache:
            cached_data = load_from_cache(cache_key, "products")
            if cached_data:
                logger.info(f"Found {len(cached_data)} cached products for {name}")
                return cached_data
        
        # Try to detect platform if not already provided
        if '_platform' not in roaster:
            platform_info = await self.platform_detector.detect(website)
            platform_type = platform_info.get("platform", "unknown")
        else:
            platform_type = roaster.get('_platform', {}).get('platform', 'unknown')
            
        logger.info(f"Platform type for {name}: {platform_type}")
        
        # Select discovery strategy based on platform
        discovered_products = []
        
        # 1. Try platform-specific API endpoints (most efficient)
        if platform_type == "shopify":
            products = await self._discover_shopify_products(website)
            if products:
                discovered_products.extend(products)
                
        elif platform_type == "woocommerce":
            products = await self._discover_woocommerce_products(website)
            if products:
                discovered_products.extend(products)
        
        # 2. If no products found yet, try sitemap discovery
        if not discovered_products:
            logger.info(f"Trying sitemap discovery for {name}")
            products = await self.sitemap_discoverer.discover(website)
            if products:
                discovered_products.extend(products)
                
        # 3. If still no products, try structured data discovery
        if not discovered_products:
            logger.info(f"Trying structured data discovery for {name}")
            products = await self.structured_data_discoverer.discover(website)
            if products:
                discovered_products.extend(products)
                
        # 4. If still no products, try Crawl4AI discovery
        if not discovered_products:
            logger.info(f"Trying Crawl4AI discovery for {name}")
            products = await self.crawl4ai_discoverer.discover(website)
            if products:
                discovered_products.extend(products)
                
        # 5. Last resort: HTML crawling
        if not discovered_products:
            logger.info(f"Trying HTML discovery for {name}")
            products = await self.html_discoverer.discover(website)
            if products:
                discovered_products.extend(products)
                
        # Post-process and deduplicate products
        unique_products = self._deduplicate_products(discovered_products)
        
        # Add roaster metadata to each product
        for product in unique_products:
            product["roaster_name"] = name
            product["roaster_slug"] = roaster.get("slug") or slugify(name)
            product["roaster_id"] = roaster.get("id")
            
        # Cache the results
        if unique_products:
            save_to_cache(cache_key, unique_products, "products")
            
        logger.info(f"Discovered {len(unique_products)} unique products for {name}")
        return unique_products
    
    async def _discover_shopify_products(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover products from a Shopify store using the products.json API endpoint.
        
        Args:
            base_url: Base URL of the Shopify store
            
        Returns:
            List of discovered products
        """
        import aiohttp
        import json
        
        products = []
        page = 1
        has_more = True
        
        # Initialize aiohttp session
        async with aiohttp.ClientSession() as session:
            while has_more:
                # Shopify pagination: /products.json?limit=250&page=X
                products_url = f"{base_url.rstrip('/')}/products.json?limit=250&page={page}"
                
                try:
                    async with session.get(products_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch Shopify products: {response.status}")
                            break
                            
                        products_json = await response.json()
                        
                        batch_products = products_json.get('products', [])
                        
                        # If we got fewer than 250 products, we've reached the end
                        if len(batch_products) < 250:
                            has_more = False
                        
                        if not batch_products:
                            break
                        
                        for product in batch_products:
                            # Extract core product data
                            name = product.get('title')
                            if not name:
                                continue
                                
                            # Check if it's a coffee product
                            product_type = product.get('product_type', '').lower()
                            if not self._is_coffee_product(name, product_type):
                                continue
                                
                            # Create basic product object
                            product_data = {
                                "name": name,
                                "slug": slugify(name),
                                "direct_buy_url": f"{base_url.rstrip('/')}/products/{product.get('handle')}",
                                "description": product.get('body_html', ''),
                                "is_available": not product.get('is_out_of_stock', False),
                                "platform": "shopify",
                                "source_data": product  # Store original data for later extraction
                            }
                            
                            # Get product image
                            if product.get('images') and len(product.get('images')) > 0:
                                product_data["image_url"] = product['images'][0].get('src')
                            
                            products.append(product_data)
                        
                        page += 1
                        await asyncio.sleep(CRAWL_DELAY)  # Be nice to servers
                        
                except Exception as e:
                    logger.error(f"Error fetching Shopify products: {str(e)}")
                    break
                    
        logger.info(f"Discovered {len(products)} products via Shopify API")
        return products
    
    async def _discover_woocommerce_products(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover products from a WooCommerce store using various API endpoints.
        
        Args:
            base_url: Base URL of the WooCommerce store
            
        Returns:
            List of discovered products
        """
        import aiohttp
        import json
        
        products = []
        
        # WooCommerce API endpoints to try
        api_endpoints = [
            "/wp-json/wc/v3/products",
            "/wp-json/wc/v2/products",
            "/wp-json/wp/v2/product"
        ]
        
        # Initialize aiohttp session
        async with aiohttp.ClientSession() as session:
            for endpoint in api_endpoints:
                products_url = f"{base_url.rstrip('/')}{endpoint}?per_page=100"
                
                try:
                    async with session.get(products_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch WooCommerce products from {endpoint}: {response.status}")
                            continue
                            
                        data = await response.json()
                        
                        if not isinstance(data, list) or not data:
                            continue
                            
                        for product in data:
                            name = product.get('name', product.get('title', {}).get('rendered', ''))
                            if not name:
                                continue
                                
                            # Check if it's a coffee product
                            product_type = self._extract_woo_product_type(product)
                            if not self._is_coffee_product(name, product_type):
                                continue
                                
                            # Get product URL
                            product_url = product.get('permalink', 
                                            product.get('link', 
                                                    f"{base_url.rstrip('/')}/product/{product.get('slug', slugify(name))}"))
                            
                            # Create basic product object
                            product_data = {
                                "name": name,
                                "slug": slugify(name),
                                "direct_buy_url": product_url,
                                "description": self._extract_woo_description(product),
                                "is_available": not product.get('is_out_of_stock', False),
                                "platform": "woocommerce",
                                "source_data": product  # Store original data for later extraction
                            }
                            
                            # Get product image
                            image_url = self._extract_woo_image(product)
                            if image_url:
                                product_data["image_url"] = image_url
                            
                            products.append(product_data)
                        
                        break  # Found products, no need to try other endpoints
                        
                except Exception as e:
                    logger.error(f"Error fetching WooCommerce products from {endpoint}: {str(e)}")
                    continue
                    
        logger.info(f"Discovered {len(products)} products via WooCommerce API")
        return products
    
    def _extract_woo_product_type(self, product: Dict[str, Any]) -> str:
        """Extract product type from WooCommerce product data"""
        if 'categories' in product:
            categories = product.get('categories', [])
            for cat in categories:
                if isinstance(cat, dict) and 'name' in cat:
                    cat_name = cat['name'].lower()
                    if 'coffee' in cat_name or 'bean' in cat_name:
                        return 'coffee'
        return ''
    
    def _extract_woo_description(self, product: Dict[str, Any]) -> str:
        """Extract description from WooCommerce product data"""
        if 'description' in product:
            if isinstance(product['description'], str):
                return product['description']
            elif isinstance(product['description'], dict) and 'rendered' in product['description']:
                return product['description']['rendered']
        elif 'content' in product and 'rendered' in product['content']:
            return product['content']['rendered']
        return ""
    
    def _extract_woo_image(self, product: Dict[str, Any]) -> Optional[str]:
        """Extract image URL from WooCommerce product data"""
        if 'images' in product and product['images'] and len(product['images']) > 0:
            image = product['images'][0]
            if isinstance(image, dict):
                return image.get('src', image.get('source_url', ''))
        return None
    
    def _is_coffee_product(self, name: str, product_type: str) -> bool:
        """
        Determine if a product is a coffee product based on name and type.
        
        Args:
            name: Product name
            product_type: Product type or category
            
        Returns:
            True if it's a coffee product, False otherwise
        """
        name_lower = name.lower()
        
        # Check product type first
        if product_type and ('coffee' in product_type or 'bean' in product_type):
            return True
            
        # Check name for coffee-related keywords
        coffee_keywords = [
            'coffee', 'bean', 'roast', 'brew', 'espresso', 'arabica', 
            'robusta', 'blend', 'single origin', 'estate'
        ]
        
        non_product_keywords = [
            'mug', 'cup', 'filter', 'brewer', 'grinder', 'equipment', 'machine', 
            'maker', 'merch', 'merchandise', 't-shirt', 'subscription'
        ]
        
        # Check for coffee keywords in name
        has_coffee_keyword = any(keyword in name_lower for keyword in coffee_keywords)
        
        # Check for non-product keywords that would indicate it's not a coffee bean product
        has_non_product_keyword = any(keyword in name_lower for keyword in non_product_keywords)
        
        # It's a coffee product if it has a coffee keyword and doesn't have non-product keywords
        return has_coffee_keyword and not has_non_product_keyword
    
    def _deduplicate_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate products based on URL.
        
        Args:
            products: List of product dicts
            
        Returns:
            Deduplicated list of products
        """
        unique_products = {}
        
        for product in products:
            url = product.get("direct_buy_url")
            
            if not url:
                continue
                
            # Use URL as unique key
            # If duplicate, only overwrite if new product has more data
            if url not in unique_products or len(product) > len(unique_products[url]):
                unique_products[url] = product
                
        return list(unique_products.values())
        
    async def close(self):
        """Close resources"""
        await self.platform_detector.close()
        await self.sitemap_discoverer.close()
        await self.html_discoverer.close()
        await self.structured_data_discoverer.close()
        # No explicit close needed for crawl4ai_discoverer as it creates and closes 
        # crawler instances within each method call