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
from common.product_classifier import is_likely_coffee_product
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
            async with PlatformDetector() as detector:
                platform_info = await detector.detect(website)
            platform_type = platform_info.get("platform", "unknown")
        else:
            platform_type = roaster.get('_platform', {}).get('platform', 'unknown')
            
        logger.info(f"Platform type for {name}: {platform_type}")
        
        # Select discovery strategy based on platform
        all_discovered_items = []
        seen_urls = set()
        
        # 1. Try platform-specific API endpoints (most efficient)
        if platform_type == "shopify":
            logger.info(f"Trying Shopify API discovery for {name}")
            products = await self._discover_shopify_products(website)
            if products:
                for p in products:
                    url = p.get("direct_buy_url")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_discovered_items.append(p)
        elif platform_type == "woocommerce":
            logger.info(f"Trying WooCommerce API discovery for {name}")
            products = await self._discover_woocommerce_products(website)
            if products:
                for p in products:
                    url = p.get("direct_buy_url")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_discovered_items.append(p)

        # --- New: Run discoverers concurrently with priority using as_completed ---
        from asyncio import create_task, as_completed
        discoverer_tasks = [
            ("sitemap", create_task(self.sitemap_discoverer.discover(website))),
            ("structured", create_task(self.structured_data_discoverer.discover(website))),
            ("html", create_task(self.html_discoverer.discover(website))),
            ("crawl4ai", create_task(self.crawl4ai_discoverer.discover(website)))
        ]
        # Priority order: sitemap > structured > html > crawl4ai
        priority = {"sitemap": 0, "structured": 1, "html": 2, "crawl4ai": 3}
        html_cap = 20
        crawl4ai_cap = 20
        html_count = 0
        crawl4ai_count = 0
        for fut in as_completed([t[1] for t in discoverer_tasks]):
            idx = [t[1] for t in discoverer_tasks].index(fut)
            name = discoverer_tasks[idx][0]
            try:
                products = await fut
            except Exception as e:
                logger.error(f"{name.capitalize()} discovery failed for {roaster.get('name')}: {e}")
                continue
            if products:
                for p in products:
                    url = p.get("direct_buy_url")
                    if not url or url in seen_urls:
                        continue
                    if name == "html":
                        if html_count >= html_cap:
                            continue
                        html_count += 1
                    if name == "crawl4ai":
                        if crawl4ai_count >= crawl4ai_cap:
                            continue
                        crawl4ai_count += 1
                    seen_urls.add(url)
                    all_discovered_items.append(p)
        # --- Centralized Filtering --- 
        logger.info(f"Aggregated {len(all_discovered_items)} potential items before filtering.")
        filtered_products = []
        for item in all_discovered_items:
            url = item.get("direct_buy_url")
            item_name = item.get("name")
            description = item.get("description")
            # Attempt to get product_type if discoverer provided it
            product_type = item.get("product_type") 
            if is_likely_coffee_product(name=item_name, url=url, description=description, product_type=product_type):
                filtered_products.append(item)
            else:
                logger.debug(f"Filtering out non-coffee item: {item_name} ({url})")
        
        logger.info(f"Filtered down to {len(filtered_products)} likely coffee products.")
        
        # Post-process and deduplicate products
        unique_products = self._deduplicate_products(filtered_products)
        
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
                                
                            # Construct URL assuming standard Shopify structure
                            handle = product.get('handle')
                            if not handle:
                                logger.warning(f"Skipping Shopify product with no handle: {name}")
                                continue
                            product_url = f"{base_url.rstrip('/')}/products/{handle}"
                            # Extract description if available (might be HTML)
                            description = product.get('body_html', '') 
                            
                            # Create basic product object
                            product_data = {
                                "name": name,
                                "slug": slugify(name),
                                "direct_buy_url": product_url,
                                "description": description,
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
                                
                            product_url = product.get('permalink', 
                                            product.get('link', 
                                                    f"{base_url.rstrip('/')}/product/{product.get('slug', slugify(name))}"))
                            description = self._extract_woo_description(product)
                            
                            # Create basic product object
                            product_data = {
                                "name": name,
                                "slug": slugify(name),
                                "direct_buy_url": product_url,
                                "description": description,
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
        await self.sitemap_discoverer.close()
        await self.html_discoverer.close()
        await self.structured_data_discoverer.close()
        # No explicit close needed for crawl4ai_discoverer as it creates and closes 
        # crawler instances within each method call