# scrapers/discoverers/html_discoverer.py
import logging
import asyncio
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse
import re

import aiohttp
from bs4 import BeautifulSoup

from common.utils import slugify
from common.product_classifier import is_likely_coffee_product
from config import USER_AGENT, REQUEST_TIMEOUT, CRAWL_DELAY

logger = logging.getLogger(__name__)

class HtmlDiscoverer:
    """
    Discovers product URLs by crawling HTML pages and looking for product links.
    
    This is typically a fallback method when sitemap and API methods fail.
    """
    
    def __init__(self):
        """Initialize the HTML discoverer"""
        self.session = None
        self.crawl_depth = 2  # Max depth for crawling
        self.max_pages = 50   # Max pages to crawl
        
    async def _init_session(self):
        """Initialize aiohttp session if needed"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
            
    async def discover(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover product URLs by crawling HTML pages.
        
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
        base_domain = urlparse(base_url).netloc
        
        # First, try common catalog/shop pages
        catalog_paths = [
            '/shop',
            '/products',
            '/collections/coffee',
            '/collections/all',
            '/product-category/coffee',
            '/coffee',
            '/store',
            '/beans'
        ]
        
        # Track visited URLs to avoid loops
        visited = set()
        # Track discovered product URLs
        product_urls = set()
        # Queue of URLs to visit
        to_visit = [base_url]
        
        # Add catalog paths to the queue
        for path in catalog_paths:
            catalog_url = base_url + path
            if catalog_url not in to_visit:
                to_visit.append(catalog_url)
                
        # Track crawl depth for each URL
        url_depth = {url: 0 for url in to_visit}
        
        # Process URLs in breadth-first order
        while to_visit and len(visited) < self.max_pages:
            current_url = to_visit.pop(0)
            current_depth = url_depth[current_url]
            
            # Skip if already visited
            if current_url in visited:
                continue
                
            visited.add(current_url)
            
            try:
                # Fetch the page
                async with self.session.get(current_url) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch {current_url}: {response.status}")
                        continue
                        
                    html = await response.text()
                    
                    # Parse the HTML
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extract product links
                    await self._extract_product_links(soup, current_url, base_url, product_urls)
                    
                    # If we've reached maximum depth, don't add more URLs to visit
                    if current_depth >= self.crawl_depth:
                        continue
                        
                    # Extract and queue additional pages to visit
                    if len(visited) < self.max_pages:
                        await self._queue_additional_pages(
                            soup, current_url, base_url, base_domain, 
                            to_visit, visited, url_depth, current_depth
                        )
                    
            except Exception as e:
                logger.warning(f"Error processing {current_url}: {str(e)}")
                continue
                
            # Be nice to the server
            await asyncio.sleep(CRAWL_DELAY)
            
        # Now fetch product details for each discovered product URL
        discovered_products = await self._process_product_urls(product_urls)
        
        logger.info(f"Discovered {len(discovered_products)} products via HTML crawling")
        return discovered_products
    
    async def _extract_product_links(self, soup: BeautifulSoup, current_url: str, 
                                     base_url: str, product_urls: Set[str]):
        """
        Extract product links from a page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            current_url: URL of the current page
            base_url: Base URL of the website
            product_urls: Set to store discovered product URLs
        """
        # Look for links that might be products
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Skip empty, fragment, or javascript links
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
                
            # Normalize URL
            full_url = urljoin(current_url, href)
            
            # Only keep URLs from the same domain
            if urlparse(base_url).netloc != urlparse(full_url).netloc:
                continue
                
            # Check if this looks like a product URL
            if self._is_product_url(full_url):
                product_urls.add(full_url)
    
    async def _queue_additional_pages(self, soup: BeautifulSoup, current_url: str, 
                                     base_url: str, base_domain: str,
                                     to_visit: List[str], visited: Set[str],
                                     url_depth: Dict[str, int], current_depth: int):
        """
        Queue additional pages to visit.
        
        Args:
            soup: BeautifulSoup parsed HTML
            current_url: URL of the current page
            base_url: Base URL of the website
            base_domain: Domain of the website
            to_visit: List of URLs to visit
            visited: Set of already visited URLs
            url_depth: Dict mapping URLs to their crawl depth
            current_depth: Depth of the current URL
        """
        # First, look for pagination links
        pagination_links = self._find_pagination_links(soup, current_url)
        
        # Then look for category/collection links
        category_links = self._find_category_links(soup, current_url)
        
        # Combine and prioritize
        additional_links = pagination_links + category_links
        
        # Add links to the queue
        for link in additional_links:
            # Only queue unvisited URLs from the same domain
            if link not in visited and link not in to_visit:
                # Make sure it's from the same domain
                if urlparse(link).netloc == base_domain:
                    to_visit.append(link)
                    url_depth[link] = current_depth + 1
    
    def _find_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Find pagination links in a page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            current_url: URL of the current page
            
        Returns:
            List of pagination URLs
        """
        pagination_links = []
        
        # Common pagination selectors
        pagination_selectors = [
            '.pagination a',
            '.pager a',
            '.pages a',
            'nav.woocommerce-pagination a',
            'a.page-numbers',
            '.next-page',
            '.paginator a',
            '.pagination-next'
        ]
        
        for selector in pagination_selectors:
            links = soup.select(selector)
            
            for link in links:
                if link.has_attr('href'):
                    href = link['href']
                    
                    # Skip empty, fragment, or javascript links
                    if not href or href.startswith('#') or href.startswith('javascript:'):
                        continue
                        
                    # Normalize URL
                    full_url = urljoin(current_url, href)
                    
                    # Add to the list
                    pagination_links.append(full_url)
                    
        return pagination_links
    
    def _find_category_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Find category/collection links in a page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            current_url: URL of the current page
            
        Returns:
            List of category/collection URLs
        """
        category_links = []
        
        # Look for links that might be categories/collections
        category_paths = [
            '/collections/', 
            '/product-category/', 
            '/category/', 
            '/coffee/'
        ]
        
        # Also look for links with these class names
        category_classes = [
            'category', 
            'collection', 
            'product-category', 
            'coffee-category'
        ]
        
        # First, check path-based links
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Skip empty, fragment, or javascript links
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
                
            # Check if the href contains any of the category paths
            if any(path in href for path in category_paths):
                # Normalize URL
                full_url = urljoin(current_url, href)
                
                # Add to the list
                category_links.append(full_url)
                
        # Then check class-based links
        for class_name in category_classes:
            for link in soup.find_all('a', class_=lambda c: c and class_name in c, href=True):
                href = link['href']
                
                # Skip empty, fragment, or javascript links
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                    
                # Normalize URL
                full_url = urljoin(current_url, href)
                
                # Add to the list if not already added
                if full_url not in category_links:
                    category_links.append(full_url)
                    
        return category_links
    
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
    
    async def _process_product_urls(self, product_urls: Set[str]) -> List[Dict[str, Any]]:
        """
        Process discovered product URLs to extract basic info.
        
        Args:
            product_urls: Set of product URLs
            
        Returns:
            List of discovered products
        """
        discovered_products = []
        
        # Process URLs in batches to avoid overwhelming the server
        batch_size = 5
        urls_list = list(product_urls)
        
        # We want to process these concurrently but with a limit
        for i in range(0, len(urls_list), batch_size):
            batch = urls_list[i:i+batch_size]
            
            # Create tasks
            tasks = [self._extract_product_info(url) for url in batch]
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)
            
            # Filter out None results
            batch_products = [p for p in results if p is not None]
            
            # Add to the list
            discovered_products.extend(batch_products)
            
            # Be nice to the server
            await asyncio.sleep(CRAWL_DELAY)
            
        return discovered_products
    
    async def _extract_product_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract basic product info from a product page.
        
        Args:
            url: Product URL
            
        Returns:
            Product data dict if successful, None otherwise
        """
        try:
            # Fetch the product page
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch product page {url}: {response.status}")
                    return None
                    
                html = await response.text()
                
                # Parse the HTML
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract product name
                name = self._extract_product_name(soup, url)
                if not name:
                    logger.warning(f"Could not extract product name from {url}")
                    return None
                    
                # Extract product description
                description = self._extract_product_description(soup)
                
                # REMOVED: Centralized filtering applied later in DiscoveryManager
                # Check if this is likely a coffee product using the central classifier
                # if not is_likely_coffee_product(name=name, url=url, description=description):
                #     logger.debug(f"HTML discoverer skipping non-coffee item: {name} ({url})")
                #     return None

                # Extract product image
                image_url = self._extract_product_image(soup)
                
                # Create product entry
                product = {
                    "name": name,
                    "slug": slugify(name),
                    "direct_buy_url": url,
                    "discovery_method": "html"
                }
                
                if image_url:
                    product["image_url"] = image_url
                    
                if description:
                    product["description"] = description
                    
                return product
                
        except Exception as e:
            logger.warning(f"Error extracting product info from {url}: {str(e)}")
            return None
    
    def _extract_product_name(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """
        Extract product name from a product page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            url: Product URL (for fallback extraction)
            
        Returns:
            Product name if found, None otherwise
        """
        # Try common heading selectors
        for selector in ['h1', '.product_title', '.product-title', '.title']:
            heading = soup.select_one(selector)
            if heading:
                name = heading.get_text(strip=True)
                if name:
                    return name
                    
        # Try page title as fallback
        title = soup.find('title')
        if title:
            # Clean up the title
            name = title.get_text(strip=True)
            
            # Remove site name if present
            if ' - ' in name:
                name = name.split(' - ')[0]
                
            if name:
                return name
                
        # Extract from URL as last resort
        path = urlparse(url).path
        if '/product/' in path or '/products/' in path:
            last_part = path.split('/')[-1]
            
            # Remove file extension if any
            if '.' in last_part:
                last_part = last_part.split('.')[0]
                
            # Replace hyphens and underscores with spaces
            name = last_part.replace('-', ' ').replace('_', ' ').title()
            
            return name
            
        return None
    
    def _extract_product_image(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract product image URL from a product page.
        
        Args:
            soup: BeautifulSoup parsed HTML
            
        Returns:
            Image URL if found, None otherwise
        """
        # Try Open Graph image
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            return og_image.get('content')
            
        # Try common product image selectors
        selectors = [
            '.product-image img', 
            '.product_image img', 
            '.woocommerce-product-gallery__image img',
            '.product-gallery img',
            '#product-image',
            '.main-image img'
        ]
        
        for selector in selectors:
            img = soup.select_one(selector)
            if img and img.get('src'):
                return img.get('src')
                
        # Try generic image with product-related class
        img = soup.find('img', class_=lambda c: c and ('product' in c or 'main' in c))
        if img and img.get('src'):
            return img.get('src')
            
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
    
    async def close(self):
        """Close resources"""
        if self.session and not self.session.closed:
            await self.session.close()