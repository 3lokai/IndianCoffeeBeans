# scrapers/discoverers/sitemap_discoverer.py
import logging
import asyncio
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
import re
import hashlib
from common.utils import slugify, load_from_cache, save_to_cache
from config import USER_AGENT, REQUEST_TIMEOUT, CRAWL_DELAY, CACHE_ENABLED, CACHE_EXPIRY
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class SitemapDiscoverer:
    """
    Discovers product URLs from sitemap.xml files.
    
    Handles both standard sitemaps and sitemap indexes.
    """
    
    def __init__(self):
        """Initialize the sitemap discoverer"""
        self.session = None
        
    async def __aenter__(self):
        await self._init_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _init_session(self):
        """Initialize aiohttp session if needed"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
            
    async def close(self):
        """Close aiohttp session if open."""
        if self.session and not self.session.closed:
            await self.session.close()

    async def discover(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover product URLs from sitemap.xml.
        
        Args:
            base_url: Base URL of the website
            
        Returns:
            List of discovered product data
        """
        await self._init_session()
        
        # Normalize URL
        if not base_url.startswith(('http://', 'https://')):
            base_url = 'https://' + base_url
            
        base_url = base_url.rstrip('/')
        
        # Try common sitemap paths
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap_products_1.xml',
            '/sitemap/sitemap.xml',
            '/product-sitemap.xml',
            '/wp-sitemap.xml',
            '/post-sitemap.xml'
        ]
        
        discovered_products = []
        seen_urls = set()
        
        for path in sitemap_paths:
            sitemap_url = base_url + path
            logger.info(f"Trying sitemap at: {sitemap_url}")

            # --- Caching logic ---
            cache_key = f"sitemap_{hashlib.md5(sitemap_url.encode()).hexdigest()}"
            sitemap_content = None
            if CACHE_ENABLED:
                sitemap_content = load_from_cache(cache_key, "sitemaps")
                if sitemap_content:
                    logger.info(f"Loaded sitemap content from cache for {sitemap_url}")
            if not sitemap_content:
                try:
                    # Fetch sitemap
                    async with self.session.get(sitemap_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch sitemap at {sitemap_url}: {response.status}")
                            continue
                        sitemap_content = await response.text()
                        # Save to cache
                        if CACHE_ENABLED:
                            save_to_cache(cache_key, sitemap_content, "sitemaps")
                except Exception as e:
                    logger.warning(f"Error processing sitemap at {sitemap_url}: {str(e)}")
                    continue
            try:
                root = ET.fromstring(sitemap_content)
            except ET.ParseError:
                logger.warning(f"Failed to parse XML from {sitemap_url}")
                continue
            nsmap = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'image': 'http://www.google.com/schemas/sitemap-image/1.1'
            }
            sitemaps = root.findall('.//ns:sitemap/ns:loc', nsmap)
            if sitemaps:
                logger.info(f"Found sitemap index with {len(sitemaps)} sitemaps")
                for sitemap in sitemaps:
                    sitemap_loc = sitemap.text
                    if sitemap_loc and self._is_product_sitemap(sitemap_loc):
                        sub_products = await self._process_sitemap(sitemap_loc, seen_urls)
                        if sub_products:
                            discovered_products.extend(sub_products)
                            # Early exit if products found
                            if discovered_products:
                                logger.info("Early exit: products found in sub-sitemap.")
                                break
                # Early exit from main loop if products found
                if discovered_products:
                    logger.info("Early exit: products found in sitemap index.")
                    break
            else:
                products = await self._process_sitemap_urls(root, sitemap_url, nsmap, seen_urls)
                if products:
                    discovered_products.extend(products)
                    # Early exit if products found
                    if discovered_products:
                        logger.info("Early exit: products found in sitemap URLs.")
                        break
        logger.info(f"Discovered {len(discovered_products)} products from sitemaps")
        return discovered_products
    
    def _is_product_sitemap(self, url: str) -> bool:
        """
        Determine if a sitemap URL is likely to contain product pages.
        
        Args:
            url: Sitemap URL
            
        Returns:
            True if likely a product sitemap, False otherwise
        """
        url_lower = url.lower()
        
        # Check for common product sitemap patterns
        product_indicators = [
            'product', 'shop', 'store', 'coffee', 'bean', 'catalog',
            'item', 'merchandise', 'collection'
        ]
        
        return any(indicator in url_lower for indicator in product_indicators)
    
    async def _process_sitemap(self, sitemap_url: str, seen_urls: set) -> List[Dict[str, Any]]:
        """
        Process a specific sitemap file to extract product URLs.

        Args:
            sitemap_url: URL of the sitemap
            seen_urls: Set of URLs already seen (for deduplication)
        Returns:
            List of discovered products
        """
        await self._init_session()
        
        # --- Caching logic ---
        cache_key = f"sitemap_{hashlib.md5(sitemap_url.encode()).hexdigest()}"
        sitemap_content = None
        if CACHE_ENABLED:
            sitemap_content = load_from_cache(cache_key, "sitemaps")
            if sitemap_content:
                logger.info(f"Loaded sitemap content from cache for {sitemap_url}")
        if not sitemap_content:
            try:
                async with self.session.get(sitemap_url) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch sub-sitemap at {sitemap_url}: {response.status}")
                        return []
                        
                    sitemap_content = await response.text()
                    # Save to cache
                    if CACHE_ENABLED:
                        save_to_cache(cache_key, sitemap_content, "sitemaps")
            except Exception as e:
                logger.warning(f"Error processing sub-sitemap at {sitemap_url}: {str(e)}")
                return []
                    
        # Parse the XML
        try:
            root = ET.fromstring(sitemap_content)
        except ET.ParseError:
            logger.warning(f"Failed to parse XML from {sitemap_url}")
            return []
                    
        # Define XML namespaces
        nsmap = {
            'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'image': 'http://www.google.com/schemas/sitemap-image/1.1'
        }
                
        # Process URLs in this sitemap
        return await self._process_sitemap_urls(root, sitemap_url, nsmap, seen_urls)
                
    async def _process_sitemap_urls(self, root, sitemap_url: str, nsmap: Dict[str, str], seen_urls: set) -> List[Dict[str, Any]]:
        """
        Process URLs from a parsed sitemap XML.

        Args:
            root: ElementTree root node
            sitemap_url: URL of the sitemap (for reference)
            nsmap: XML namespace mappings
            seen_urls: Set of seen URLs
        Returns:
            List of discovered products
        """
        products = []
        
        # Find all URL entries
        url_entries = root.findall('.//ns:url', nsmap)
        logger.info(f"Found {len(url_entries)} URLs in sitemap {sitemap_url}")
        
        # We want to process these concurrently but with a limit
        tasks = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        
        for url_entry in url_entries:
            loc = url_entry.find('ns:loc', nsmap)
            if loc is None or not loc.text:
                continue
                
            url = loc.text
            
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
            # Skip if not likely a product URL
            if not self._is_product_url(url):
                continue
                
            # Get last modified date if available
            lastmod = url_entry.find('ns:lastmod', nsmap)
            lastmod_str = lastmod.text if lastmod is not None else None
            
            # Look for title and image in the sitemap entry
            title = None
            image_url = None
            
            # Check for image tags
            image_tag = url_entry.find('.//image:title', nsmap)
            if image_tag is not None and image_tag.text:
                title = image_tag.text
                
            image_loc = url_entry.find('.//image:loc', nsmap)
            if image_loc is not None and image_loc.text:
                image_url = image_loc.text
                
            # Create a task to check if this is a product page
            tasks.append(self._check_product_url(url, title, image_url, lastmod_str, semaphore, seen_urls))
            
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        products = [p for p in results if p is not None]
        
        logger.info(f"Found {len(products)} product URLs in sitemap {sitemap_url}")
        return products
    
    def _is_product_url(self, url: str) -> bool:
        """
        Check if a URL is likely to be a product page.
        
        Args:
            url: URL to check
            
        Returns:
            True if likely a product page, False otherwise
        """
        url_lower = url.lower()
        
        # Check for common product URL patterns
        product_indicators = [
            '/product/', '/products/', '/shop/', '/store/',
            '/coffee/', '/bean/', '/item/', '/p/', 
            'product_id=', 'productid='
        ]
        
        # Exclude common non-product URL patterns
        exclude_indicators = [
            '/category/', '/tag/', '/author/', '/page/', 
            '/post/', '/article/', '/blog/', '/news/'
        ]
        
        has_product_indicator = any(indicator in url_lower for indicator in product_indicators)
        has_exclude_indicator = any(indicator in url_lower for indicator in exclude_indicators)
        
        return has_product_indicator and not has_exclude_indicator
    
    async def _check_product_url(self, url: str, title: Optional[str], image_url: Optional[str], 
                                lastmod: Optional[str], semaphore: asyncio.Semaphore, seen_urls: set) -> Optional[Dict[str, Any]]:
        """
        Check if a URL is a product page and extract basic info.

        Args:
            url: URL to check
            title: Pre-extracted title, if available
            image_url: Pre-extracted image URL, if available
            lastmod: Last modified date from sitemap
            semaphore: Semaphore for limiting concurrent requests
            seen_urls: Set of URLs already seen (for deduplication)
        Returns:
            Product data dict if it's a product page, None otherwise
        """
        async with semaphore:
            if url in seen_urls:
                return None
            seen_urls.add(url)
            
            description = None
            soup = None
            
            try:
                # We'll do a HEAD request first to check if the page is available
                async with self.session.head(url, allow_redirects=True) as head_response:
                    if head_response.status != 200:
                        return None
            
                # Only do a GET request if we need to extract more info
                if not title or not image_url:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return None
                        
                        html = await response.text()
                        
                        # Parse the HTML
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Extract title if not already available
                        if not title:
                            title_tag = soup.find('h1') or soup.find('title')
                            if title_tag:
                                title = title_tag.get_text(strip=True)
                                
                        # Extract image if not already available
                        if not image_url:
                            # Try Open Graph image
                            og_image = soup.find('meta', property='og:image')
                            if og_image and og_image.get('content'):
                                image_url = og_image.get('content')
                            else:
                                # Try first product image
                                img = soup.find('img', class_=lambda c: c and ('product' in c or 'main' in c))
                                if img and img.get('src'):
                                    image_url = img.get('src')
                        
                        # Extract description since we have the soup
                        description = self._extract_product_description(soup)
                        
                # Ensure title is present, extract from URL if needed
                if not title:
                    # Extract name from the URL path
                    path_parts = url.rstrip('/').split('/')
                    last_part = path_parts[-1]
                    
                    # Remove query parameters, if any
                    if '?' in last_part:
                        last_part = last_part.split('?')[0]
                        
                    # Remove file extension, if any
                    if '.' in last_part:
                        last_part = last_part.split('.')[0]
                        
                    # Replace hyphens and underscores with spaces and capitalize
                    title = last_part.replace('-', ' ').replace('_', ' ').title()
            
                # Create product entry
                product = {
                    "name": title,
                    "slug": slugify(title),
                    "direct_buy_url": url,
                    "discovery_method": "sitemap"
                }
                if image_url:
                    product["image_url"] = image_url
                if lastmod:
                    product["last_modified"] = lastmod

                # Use the centralized classifier to filter
                if is_likely_coffee_product(name=title, url=url, description=description):
                    return product
                else:
                    logger.debug(f"Filtered out non-coffee product from sitemap: {title} ({url})")
                    return None
            
            except Exception as e:
                logger.debug(f"Error checking product URL {url}: {str(e)}")
                return None
    
    def _extract_product_description(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract product description from a product page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            
        Returns:
            Description if found, None otherwise
        """
        # Try meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc.get('content')
            
        # Try common description selectors
        selectors = [
            '.product-description', 
            '.description', 
            '.product_description',
            '.woocommerce-product-details__short-description',
            '#product-description',
            '.product-short-description'
        ]
        
        for selector in selectors:
            desc = soup.select_one(selector)
            if desc:
                return desc.get_text(strip=True)
                
        return None