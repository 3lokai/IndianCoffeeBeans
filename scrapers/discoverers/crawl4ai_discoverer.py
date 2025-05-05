# scrapers/discoverers/crawl4ai_discoverer.py
import logging
import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse, unquote

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import FilterChain, URLPatternFilter, DomainFilter

from common.utils import slugify
from common.product_classifier import is_likely_coffee_product
from config import CRAWL_DELAY

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

class Crawl4AIDiscoverer:
    """
    Advanced discoverer using Crawl4AI to find products through JavaScript-rendered content
    and dynamic pages, with intelligent processing of the resulting markdown/HTML.
    
    This discoverer:
    1. Processes JavaScript-heavy pages with proper rendering
    2. Handles dynamic "Load More" buttons and pagination
    3. Cleans content with PruningContentFilter
    4. Extracts product information from the cleaned content
    """
    
    def __init__(self):
        """
        Initialize the Crawl4AI discoverer.
        
        Args:
            deepseek_api_key: Optional API key for DeepSeek enhancement
        """
        self.browser_config = BrowserConfig(
            headless=True,
            ignore_https_errors=True,
            viewport_width=1280,
            viewport_height=800,
        )
        
    async def discover(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover products using Crawl4AI's deep crawling capabilities.
        
        Args:
            base_url: Base URL of the website
                
        Returns:
            List of discovered product data
        """
        # Normalize URL
        if not base_url.startswith(('http://', 'https://')):
            base_url = 'https://' + base_url
                
        base_url = base_url.rstrip('/')
        base_domain = urlparse(base_url).netloc
        
        # Define starting points for crawling - we'll use fewer since deep crawl will explore
        shop_paths = [
            '',  # Homepage often has navigation to products
            '/shop',
            '/products'
        ]
        
        # Track discovered products
        discovered_products = []
        visited_urls = set()
        
        # Create crawler instance with the browser config
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            # We'll only need to process one or two starting points with deep crawling
            for path in shop_paths:
                shop_url = base_url + path
                if shop_url in visited_urls:
                    continue
                    
                visited_urls.add(shop_url)
                
                # Deep crawl from this starting point
                products_batch, _ = await self._process_page(crawler, shop_url, base_url, base_domain)
                discovered_products.extend(products_batch)
                
                # If we've found a good number of products, we can stop
                if len(discovered_products) >= 10:
                    logger.info(f"Found {len(discovered_products)} products, stopping early")
                    break
                    
                # Be nice to the server
                await asyncio.sleep(CRAWL_DELAY)
            
            # Deduplicate products based on URL
            unique_products = {}
            for product in discovered_products:
                url = product.get("direct_buy_url")
                if url and url not in unique_products:
                    unique_products[url] = product
                    
            result_products = list(unique_products.values())
            logger.info(f"Discovered {len(result_products)} unique products after deduplication")
            return result_products
            
    async def _process_page(self, crawler: AsyncWebCrawler, url: str, base_url: str, base_domain: str) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Process a single page to extract products and find next pages using deep crawling.
        """
        logger.info(f"Processing page with Crawl4AI deep crawling: {url}")
        
        try:
            # Set up URL filter for product pages only
            url_filter = URLPatternFilter(
                patterns=[
                    "*product*", 
                    "*coffee*", 
                    "*bean*",
                    "*/p/*",
                    "*/collections/*",
                    "*/shop*"
                ]
            )
            
            # Create domain filter to stay within the same site
            domain_filter = DomainFilter(
                allowed_domains=[base_domain],
                blocked_domains=[]
            )
            
            # Combine filters into a chain
            filter_chain = FilterChain([url_filter, domain_filter])
            
            # Configure deep crawl strategy
            deep_crawl_strategy = BFSDeepCrawlStrategy(
                max_depth=3,               # Crawl up to 3 levels deep
                include_external=False,    # Stay within the same domain
                max_pages=50,              # Limit total pages to avoid overwhelming
                filter_chain=filter_chain  # Apply our filtering rules
            )
            
            # Create run configuration with deep crawl strategy
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                wait_until="domcontentloaded",
                page_timeout=60000,  # 60 seconds timeout
                word_count_threshold=15, # Lower threshold for potentially smaller product blocks
                deep_crawl_strategy=deep_crawl_strategy,
                stream=False  # Get all results at once since we need to process them
            )
            
            # Run the crawler
            results = await crawler.arun(url=url, config=crawler_config)
            
            # Process results - deep crawl returns a list of CrawlResults
            all_products = []
            
            # Handle the results from deep crawling
            if isinstance(results, list):
                logger.info(f"Deep crawl found {len(results)} pages")
                
                for result in results:
                    if not result.success:
                        continue
                        
                    # Extract products from each page
                    structured_products = self._extract_from_structured_data(str(result.html), result.url)
                    all_products.extend(structured_products)
                    
                    html_products = self._extract_from_html(str(result.html), result.url, base_url)
                    all_products.extend(html_products)
                    
                    # Log the discovery depth
                    depth = result.metadata.get('depth', 0)
                    logger.debug(f"Processed page at depth {depth}: {result.url}")
            else:
                # Handle the case when results is a single CrawlResult (shouldn't happen with deep crawl)
                logger.warning("Expected list of results from deep crawl but got single result")
                if results.success:
                    structured_products = self._extract_from_structured_data(str(results.html), results.url)
                    all_products.extend(structured_products)
                    
                    html_products = self._extract_from_html(str(results.html), results.url, base_url)
                    all_products.extend(html_products)
            
            # We don't need next_pages anymore as deep crawling handles that
            return all_products, []
                
        except Exception as e:
            logger.error(f"Error processing {url} with Crawl4AI deep crawling: {str(e)}")
            return [], []
            
    def _extract_from_structured_data(self, html: str, current_url: str) -> List[Dict[str, Any]]:
        """
        Extract products from structured data in HTML.
        
        Args:
            html: HTML content
            current_url: Current page URL
            
        Returns:
            List of discovered products
        """
        products = []
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for JSON-LD structured data
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            for script in json_ld_scripts:
                try:
                    # Parse JSON
                    data = json.loads(script.string)
                    
                    # Handle both single objects and arrays
                    items = []
                    
                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict):
                        # Handle @graph structure
                        if '@graph' in data:
                            items = data['@graph']
                        else:
                            items = [data]
                    else:
                        continue
                        
                    # Process each item
                    for item in items:
                        # Check if it's a product
                        if isinstance(item, dict):
                            product_type = item.get('@type')
                            
                            # Handle array of types
                            if isinstance(product_type, list):
                                is_product = 'Product' in product_type
                            else:
                                is_product = product_type == 'Product'
                                
                            if is_product:
                                # Extract details for classification
                                name = item.get('name')
                                description = item.get('description')
                                item_url_prop = item.get('url') or item.get('offers', {}).get('url') # Offers might contain the specific variant URL
                                item_url = urljoin(current_url, item_url_prop) if item_url_prop else current_url # Fallback to current_url if no specific item URL
                                category = item.get('category') # Can be string or list
                                categories = [category] if isinstance(category, str) else category
                                
                                # Always extract if it's a Product type, filter later
                                # Extract basic info
                                # name, url, description already extracted above
                                    
                                # Normalize URL
                                if item_url and not item_url.startswith(('http://', 'https://')):
                                    item_url = urljoin(current_url, item_url)
                                    
                                # Create product entry
                                if name and item_url:
                                    product = {
                                        "name": name,
                                        "slug": slugify(name),
                                        "direct_buy_url": item_url,
                                        "discovery_method": "crawl4ai_structured"
                                    }
                                        
                                    if description:
                                        product["description"] = description
                                        
                                    # Get image
                                    image = item.get('image')
                                    if image:
                                        if isinstance(image, list) and image:
                                            image_url = image[0].get('url') if isinstance(image[0], dict) else image[0]
                                        elif isinstance(image, dict):
                                            image_url = image.get('url')
                                        else:
                                            image_url = image
                                            
                                        if image_url and not image_url.startswith(('http://', 'https://')):
                                            image_url = urljoin(current_url, image_url)
                                            
                                        product["image_url"] = image_url
                                        
                                    products.append(product)
                
                except Exception as e:
                    logger.debug(f"Error parsing JSON-LD: {str(e)}")
                    continue
                    
            return products
            
        except Exception as e:
            logger.error(f"Error extracting from structured data: {str(e)}")
            return []
            
    def _extract_from_html(self, html: str, current_url: str, base_url: str) -> List[Dict[str, Any]]:
        """
        Extract products from HTML patterns.
        
        Args:
            html: HTML content
            current_url: Current page URL
            base_url: Base URL of the website
            
        Returns:
            List of discovered products
        """
        products = []
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for common product grid patterns
            product_selectors = [
                '.products .product',
                '.product-grid .product',
                '.collection-grid .product',
                '.product-list .product',
                'li.product',
                '.product-item',
                '.product-card',
                '.card.product',
                '[data-product-id]',
                '[data-product]'
            ]
            
            found_products = set()
            
            for selector in product_selectors:
                items = soup.select(selector)
                
                for item in items:
                    # Find link
                    link = item.find('a', href=True)
                    if not link:
                        continue
                        
                    href = link['href']
                    
                    # Skip non-product links
                    if not self._is_product_url(href):
                        continue
                        
                    # Normalize URL
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(current_url, href)
                        
                    # Skip if already found
                    if href in found_products:
                        continue
                        
                    found_products.add(href)
                    
                    # Find name
                    name_elem = item.find('h2') or item.find('h3') or item.find('h4') or item.find('.product-title') or item.find('.title')
                    name = name_elem.get_text().strip() if name_elem else None
                    
                    # If no name, try to extract from link text
                    if not name and link.get_text().strip():
                        name = link.get_text().strip()
                        
                    # Skip if no name
                    if not name:
                        continue
                        
                    # Always extract, filter later
                    # Create product entry
                    product = {
                        "name": name,
                        "slug": slugify(name),
                        "direct_buy_url": href,
                        "discovery_method": "crawl4ai_html"
                    }
                    
                    # Try to find image
                    img = item.find('img')
                    if img and img.get('src'):
                        img_src = img['src']
                        
                        # Normalize image URL
                        if not img_src.startswith(('http://', 'https://')):
                            img_src = urljoin(current_url, img_src)
                            
                        product["image_url"] = img_src
                        
                    # Try to find price
                    price_elem = item.select_one('.price, .product-price, .amount')
                    if price_elem:
                        product["price_text"] = price_elem.get_text().strip()
                        
                    products.append(product)
                    
            # If structured methods found few products, try generic link extraction
            if len(products) < 3:
                # Look for links that might be products
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    
                    # Skip empty, fragment, or javascript links
                    if not href or href.startswith('#') or href.startswith('javascript:'):
                        continue
                            
                    # Skip "previous" links
                    if 'prev' in href.lower() or 'previous' in href.lower():
                        continue
                            
                    # Normalize URL
                    full_url = urljoin(current_url, href)
                        
                    # Skip if same as current page
                    if full_url == current_url:
                        continue
                            
                    # Always extract, filter later        
                    # Create product entry
                    product = {
                        "name": link.get_text().strip(),
                        "slug": slugify(link.get_text().strip()),
                        "direct_buy_url": full_url,
                        "discovery_method": "crawl4ai_links"
                    }
                    
                    # Try to find image
                    img = link.find('img')
                    if img and img.get('src'):
                        img_src = img['src']
                        
                        # Normalize image URL
                        if not img_src.startswith(('http://', 'https://')):
                            img_src = urljoin(current_url, img_src)
                            
                        product["image_url"] = img_src
                        
                    products.append(product)
                    
            return products
            
        except Exception as e:
            logger.error(f"Error extracting from HTML: {str(e)}")
            return []
            
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
            '/product/', '/products/', '/shop/', '/store/', '/ols/'
            '/coffee/', '/bean/', '/item/', '/p/', '/collection/', '/collections/',
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