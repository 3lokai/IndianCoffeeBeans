# scrapers/discoverers/sitemap_discoverer.py
import logging
import asyncio
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
import re

import aiohttp
from bs4 import BeautifulSoup

from common.utils import slugify
from config import USER_AGENT, REQUEST_TIMEOUT, CRAWL_DELAY

logger = logging.getLogger(__name__)

class SitemapDiscoverer:
    """
    Discovers product URLs from sitemap.xml files.
    
    Handles both standard sitemaps and sitemap indexes.
    """
    
    def __init__(self):
        """Initialize the sitemap discoverer"""
        self.session = None
        
    async def _init_session(self):
        """Initialize aiohttp session if needed"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
            
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
        
        for path in sitemap_paths:
            sitemap_url = base_url + path
            logger.info(f"Trying sitemap at: {sitemap_url}")
            
            try:
                # Fetch sitemap
                async with self.session.get(sitemap_url) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch sitemap at {sitemap_url}: {response.status}")
                        continue
                        
                    sitemap_content = await response.text()
                    
                    # Parse the XML
                    try:
                        root = ET.fromstring(sitemap_content)
                    except ET.ParseError:
                        logger.warning(f"Failed to parse XML from {sitemap_url}")
                        continue
                        
                    # Define XML namespaces
                    nsmap = {
                        'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                        'image': 'http://www.google.com/schemas/sitemap-image/1.1'
                    }
                    
                    # Check if this is a sitemap index
                    sitemaps = root.findall('.//ns:sitemap/ns:loc', nsmap)
                    if sitemaps:
                        logger.info(f"Found sitemap index with {len(sitemaps)} sitemaps")
                        
                        # Process each sitemap in the index that looks product-related
                        for sitemap in sitemaps:
                            sitemap_loc = sitemap.text
                            if sitemap_loc and self._is_product_sitemap(sitemap_loc):
                                # Process this sub-sitemap
                                sub_products = await self._process_sitemap(sitemap_loc)
                                if sub_products:
                                    discovered_products.extend(sub_products)
                    else:
                        # This is a regular sitemap, process URLs directly
                        products = await self._process_sitemap_urls(root, sitemap_url, nsmap)
                        if products:
                            discovered_products.extend(products)
                    
                    # If we found products, no need to check other sitemap paths
                    if discovered_products:
                        break
                        
            except Exception as e:
                logger.warning(f"Error processing sitemap at {sitemap_url}: {str(e)}")
                continue
                
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
    
    async def _process_sitemap(self, sitemap_url: str) -> List[Dict[str, Any]]:
        """
        Process a specific sitemap file to extract product URLs.
        
        Args:
            sitemap_url: URL of the sitemap
            
        Returns:
            List of discovered products
        """
        await self._init_session()
        
        try:
            async with self.session.get(sitemap_url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch sub-sitemap at {sitemap_url}: {response.status}")
                    return []
                    
                sitemap_content = await response.text()
                
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
                return await self._process_sitemap_urls(root, sitemap_url, nsmap)
                
        except Exception as e:
            logger.warning(f"Error processing sub-sitemap at {sitemap_url}: {str(e)}")
            return []
    
    async def _process_sitemap_urls(self, root, sitemap_url: str, nsmap: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Process URLs from a parsed sitemap XML.
        
        Args:
            root: ElementTree root node
            sitemap_url: URL of the sitemap (for reference)
            nsmap: XML namespace mappings
            
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
            tasks.append(self._check_product_url(url, title, image_url, lastmod_str, semaphore))
            
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
                                lastmod: Optional[str], semaphore: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
        """
        Check if a URL is a product page and extract basic info.
        
        Args:
            url: URL to check
            title: Pre-extracted title, if available
            image_url: Pre-extracted image URL, if available
            lastmod: Last modified date from sitemap
            semaphore: Semaphore for limiting concurrent requests
            
        Returns:
            Product data dict if it's a product page, None otherwise
        """
        async with semaphore:
            # Rate limiting
            await asyncio.sleep(CRAWL_DELAY)
            
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
                        
                        # Check if this is a coffee product
                        if not self._is_coffee_product_page(soup, title or ''):
                            return None
                
                # If we still don't have a title, extract from URL
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
                    
                return product
                
            except Exception as e:
                logger.debug(f"Error checking product URL {url}: {str(e)}")
                return None
    
    def _is_coffee_product_page(self, soup: BeautifulSoup, title: str) -> bool:
        """
        Determine if a page is a coffee product page based on its content.
        
        Args:
            soup: BeautifulSoup parsed HTML
            title: Page title
            
        Returns:
            True if it's a coffee product page, False otherwise
        """
        # Check title first
        title_lower = title.lower()
        
        coffee_keywords = [
            'coffee', 'bean', 'roast', 'brew', 'espresso', 'arabica', 
            'robusta', 'blend', 'single origin', 'estate'
        ]
        
        non_product_keywords = [
            'mug', 'cup', 'filter', 'brewer', 'grinder', 'equipment', 'machine', 
            'maker', 'merch', 'merchandise', 't-shirt', 'subscription'
        ]
        
        # Check if title contains coffee keywords
        if any(keyword in title_lower for keyword in coffee_keywords):
            # Make sure it's not a non-product
            if not any(keyword in title_lower for keyword in non_product_keywords):
                return True
                
        # Check page content
        # 1. Look for structured data
        json_ld = soup.find('script', type='application/ld+json')
        if json_ld:
            try:
                import json
                data = json.loads(json_ld.string)
                
                # Check for product type in structured data
                if isinstance(data, dict):
                    if data.get('@type') == 'Product':
                        # Check if it's a coffee product
                        description = data.get('description', '').lower()
                        if any(keyword in description for keyword in coffee_keywords):
                            return True
            except:
                pass
                
        # 2. Check content keywords
        page_text = soup.get_text().lower()
        coffee_content_keywords = [
            'coffee bean', 'coffee blend', 'brewing', 'roast profile',
            'flavor notes', 'tasting notes', 'origin', 'altitude', 'aroma'
        ]
        
        if any(keyword in page_text for keyword in coffee_content_keywords):
            return True
            
        # 3. Check for coffee-specific form fields (weight selection, grind selection)
        grind_selector = soup.select_one('select[name*="grind"], select[id*="grind"]')
        if grind_selector:
            return True
            
        weight_selector = soup.select_one('select[name*="weight"], select[name*="size"]')
        if weight_selector:
            options = weight_selector.find_all('option')
            for option in options:
                option_text = option.get_text().lower()
                if 'g' in option_text or 'gram' in option_text or 'kg' in option_text:
                    return True
                    
        return False
        
    async def close(self):
        """Close resources"""
        if self.session and not self.session.closed:
            await self.session.close()