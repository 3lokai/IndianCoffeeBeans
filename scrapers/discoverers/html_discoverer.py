# scrapers/discoverers/html_discoverer.py
import logging
import asyncio
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse
import re
import hashlib

import aiohttp
from bs4 import BeautifulSoup, Tag

from common.product_classifier import is_likely_coffee_product
from common.utils import slugify, load_from_cache, save_to_cache
from common.product_classifier import is_likely_coffee_product
from config import USER_AGENT, REQUEST_TIMEOUT, CRAWL_DELAY, CACHE_ENABLED, CACHE_EXPIRY

logger = logging.getLogger(__name__)

class HtmlDiscoverer:
    """
    Discovers product URLs by crawling HTML pages and looking for product links
    within common listing structures.

    This is typically a fallback method when sitemap and API methods fail.
    """

    def __init__(self):
        """Initialize the HTML discoverer"""
        self.session = None
        self.crawl_depth = 3  # Max depth for crawling
        self.max_pages = 50   # Max pages to crawl
        # Common selectors for product containers/cards on listing pages
        self.product_container_selectors = [
            '.product-item', '.product-card', '.product', 'li.product',
            '.grid-item', '.collection-item', '.product-block', '.product-tile'
        ]
        # Common selectors for product links within containers
        self.product_link_selectors = [
            'a.product-item-link', 'a.product-card-link', 'a.product-loop-title',
            'a.woocommerce-LoopProduct-link', 'h2 a', 'h3 a', 'a.product-title', 'a' # Fallback: any link
        ]
        # Common selectors for product names within containers
        self.product_name_selectors = [
            '.product-item-name', '.product-card-title', '.product-title',
            'h2.woocommerce-loop-product__title', 'h2', 'h3', '.name'
        ]


    async def _init_session(self):
        """Initialize aiohttp session if needed"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )

    async def discover(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Discover product URLs by crawling HTML pages and extracting from listings.

        Args:
            base_url: Base URL of the website

        Returns:
            List of discovered product data (dictionary with 'name' and 'url')
        """
        await self._init_session()

        if not base_url.startswith(('http://', 'https://')):
            base_url = 'https://' + base_url

        base_url = base_url.rstrip('/')
        base_domain = urlparse(base_url).netloc

        catalog_paths = [
            '/shop', '/products', '/collections/coffee', '/collections/all',
            '/product-category/coffee', '/coffee', '/store', '/beans'
        ]

        seen_urls = set()
        visited: Set[str] = set()
        # Store discovered product data {url: name} to avoid duplicates
        discovered_products_dict: Dict[str, str] = {}
        to_visit: List[str] = [base_url] + [urljoin(base_url, path) for path in catalog_paths]
        # Ensure uniqueness in initial list
        to_visit = list(dict.fromkeys(to_visit))
        url_depth: Dict[str, int] = {url: 0 for url in to_visit}

        while to_visit and len(visited) < self.max_pages:
            current_url = to_visit.pop(0)
            if current_url in seen_urls:
                continue
            seen_urls.add(current_url)
            current_depth = url_depth.get(current_url, 0) # Use get for safety

            if current_url in visited:
                continue

            # Check domain again before processing
            if urlparse(current_url).netloc != base_domain:
                logger.debug(f"Skipping external or invalid domain link: {current_url}")
                continue

            visited.add(current_url)
            logger.debug(f"Crawling (Depth {current_depth}): {current_url}")

            # --- Caching logic for HTML content ---
            cache_key = f"html_{hashlib.md5(current_url.encode()).hexdigest()}"
            html = None
            if CACHE_ENABLED:
                html = load_from_cache(cache_key, "htmlpages")
                if html:
                    logger.info(f"Loaded HTML content from cache for {current_url}")
            if not html:
                try:
                    async with self.session.get(current_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch {current_url}: Status {response.status}")
                            continue
                        # Check content type - only process HTML
                        content_type = response.headers.get('Content-Type', '').lower()
                        if 'html' not in content_type:
                            logger.debug(f"Skipping non-HTML content at {current_url}: {content_type}")
                            continue
                        html = await response.text()
                        # Save to cache
                        if CACHE_ENABLED:
                            save_to_cache(cache_key, html, "htmlpages")
                except Exception as e:
                    logger.warning(f"Unexpected error processing {current_url}: {str(e)}", exc_info=True)
                    continue

            try:
                # Try the faster lxml parser first
                soup = BeautifulSoup(html, 'lxml')
            except ImportError:
                # Fall back to the built-in parser if lxml is not installed
                logger.debug("lxml parser not found, falling back to html.parser.")
                soup = BeautifulSoup(html, 'html.parser')

            # Extract product info directly from this page
            await self._extract_products_from_page(soup, current_url, base_url, discovered_products_dict)

            if current_depth < self.crawl_depth and len(visited) < self.max_pages:
                await self._queue_additional_pages(
                    soup, current_url, base_url, base_domain,
                    to_visit, visited, url_depth, current_depth, seen_urls
                )

            await asyncio.sleep(CRAWL_DELAY)

        # Convert dict to list of dicts format
        final_products = [{"name": name, "url": url} for url, name in discovered_products_dict.items()]

        logger.info(f"Discovered {len(final_products)} potential coffee products via HTML crawling from {len(visited)} pages.")
        return final_products


    async def _extract_products_from_page(self, soup: BeautifulSoup, current_url: str,
                                          base_url: str, discovered_products_dict: Dict[str, str]):
        """
        Extracts product names and URLs from product listing elements on a page.

        Args:
            soup: BeautifulSoup parsed HTML of the current page.
            current_url: The URL of the page being parsed.
            base_url: The base URL of the target website.
            discovered_products_dict: Dictionary to store {url: name} of discovered products.
        """
        base_domain = urlparse(base_url).netloc
        count_before = len(discovered_products_dict)

        # Find potential product containers
        product_elements = []
        for selector in self.product_container_selectors:
             try:
                elements = soup.select(selector)
                if elements:
                    product_elements.extend(elements)
                    logger.debug(f"Found {len(elements)} elements with selector '{selector}' on {current_url}")
             except Exception as e:
                 logger.warning(f"Error using selector '{selector}' on {current_url}: {e}")
                 
        # If specific containers found, process them. Otherwise, maybe fall back?
        # For now, focus on processing identified containers
        if not product_elements:
             logger.debug(f"No specific product containers found on {current_url}. Might need broader selectors or fallback.")
             # Potential Fallback: Look for *all* links and apply classifier? Risky.
             # For now, do nothing if no containers found.
             return

        logger.debug(f"Processing {len(product_elements)} potential product elements on {current_url}")
        processed_in_container = set() # Track URLs found within containers on this page

        for element in product_elements:
            product_url: Optional[str] = None
            product_name: Optional[str] = None

            # Find the link within the container
            link_tag: Optional[Tag] = None
            for selector in self.product_link_selectors:
                try:
                    link_tag = element.select_one(selector)
                    if link_tag and link_tag.has_attr('href'):
                        href = link_tag['href']
                        # Basic sanity check on href
                        if href and not href.startswith('#') and not href.startswith('javascript:'):
                            full_url = urljoin(current_url, href)
                            # Check domain
                            if urlparse(full_url).netloc == base_domain:
                                product_url = full_url
                                break # Found a suitable link
                            else:
                                logger.debug(f"Skipping link to external domain: {full_url}")
                                link_tag = None # Reset link_tag if domain doesn't match
                        else:
                             link_tag = None # Reset link_tag if invalid href
                except Exception as e:
                    logger.debug(f"Error finding link with selector '{selector}' in element: {e}")
                    link_tag = None # Ensure link_tag is None on error


            if not product_url:
                # logger.debug(f"Could not find a valid product link within element: {element.prettify()[:200]}...")
                continue # Skip this element if no valid link found

            # Avoid processing the same URL multiple times if it appears in different containers on the same page
            if product_url in processed_in_container:
                continue
            processed_in_container.add(product_url)


            # Find the name within the container
            name_tag: Optional[Tag] = None
            for selector in self.product_name_selectors:
                 try:
                    name_tag = element.select_one(selector)
                    if name_tag:
                        name_text = name_tag.get_text(strip=True)
                        if name_text:
                            product_name = name_text
                            break # Found a name
                 except Exception as e:
                     logger.debug(f"Error finding name with selector '{selector}' in element: {e}")

            # If no specific name found, try the link text itself
            if not product_name and link_tag:
                name_text = link_tag.get_text(strip=True)
                if name_text:
                    product_name = name_text
                # As a last resort for name, generate from URL slug
                elif product_url:
                     try:
                         path_parts = urlparse(product_url).path.strip('/').split('/')
                         if path_parts:
                             product_name = slugify(path_parts[-1]).replace('-', ' ').title()
                     except Exception:
                         pass # Ignore errors in slug generation

            if not product_name:
                 logger.debug(f"Could not determine product name for URL: {product_url}")
                 continue # Skip if we have URL but no name


            # Now, classify based on the extracted name and URL
            try:
                if is_likely_coffee_product(product_name, product_url):
                    # Check if URL already discovered (from other pages)
                    if product_url not in discovered_products_dict:
                         logger.info(f"Found potential coffee product: '{product_name}' -> {product_url}")
                         discovered_products_dict[product_url] = product_name
                    # else:
                    #     logger.debug(f"Product already found: {product_url}")
                # else:
                #      logger.debug(f"Skipping non-coffee item: '{product_name}' ({product_url})")
            except Exception as e:
                logger.error(f"Error classifying product '{product_name}' ({product_url}): {e}")

        count_after = len(discovered_products_dict)
        if count_after > count_before:
            logger.debug(f"Added {count_after - count_before} new products from {current_url}")


    async def _queue_additional_pages(self, soup: BeautifulSoup, current_url: str,
                                     base_url: str, base_domain: str,
                                     to_visit: List[str], visited: Set[str],
                                     url_depth: Dict[str, int], current_depth: int, seen_urls: Set[str]):
        """
        Queue additional pages to visit (pagination, categories).
        Implementation remains largely the same, but ensures uniqueness and domain check.
        """
        next_depth = current_depth + 1
        # Use a set to avoid adding duplicate links found by different methods
        links_to_queue: Set[str] = set()

        # Find pagination links
        try:
            pagination_links = self._find_pagination_links(soup, current_url)
            for link in pagination_links:
                 # Basic validation and domain check
                 parsed_link = urlparse(link)
                 if parsed_link.scheme in ('http', 'https') and parsed_link.netloc == base_domain:
                     links_to_queue.add(link)
        except Exception as e:
             logger.warning(f"Error finding pagination links on {current_url}: {e}")


        # Find category links
        try:
            category_links = self._find_category_links(soup, current_url)
            for link in category_links:
                # Basic validation and domain check
                parsed_link = urlparse(link)
                if parsed_link.scheme in ('http', 'https') and parsed_link.netloc == base_domain:
                    links_to_queue.add(link)
        except Exception as e:
             logger.warning(f"Error finding category links on {current_url}: {e}")

        # Add valid, new links to the main queue
        added_count = 0
        # Create a set of URLs currently in to_visit for faster lookups
        to_visit_set = set(to_visit)
        
        for link in links_to_queue:
            # Check if not visited and not already in the queue
            if link not in visited and link not in to_visit_set and link not in seen_urls:
                 # Double-check domain just in case
                 if urlparse(link).netloc == base_domain:
                    # Check queue size limit indirectly via visited count?
                    # Or add a direct limit on to_visit size if needed.
                    if len(visited) + len(to_visit) < self.max_pages * 1.5: # Allow queue to grow slightly larger than visited limit
                        to_visit.append(link)
                        to_visit_set.add(link) # Keep set updated
                        url_depth[link] = next_depth
                        added_count += 1
                    else:
                         logger.debug("Skipping queueing due to page limits.")
                         break # Stop adding if limits reached

        if added_count > 0:
            logger.debug(f"Queued {added_count} new links from {current_url}")


    # _find_pagination_links and _find_category_links remain the same as before
    # We keep them as separate methods for clarity.

    def _find_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Find pagination links in a page. (Implementation unchanged)
        """
        pagination_links = []
        pagination_selectors = [
            '.pagination a', '.pager a', '.pages a', 'nav.woocommerce-pagination a',
            'a.page-numbers', '.next-page', '.paginator a', '.pagination-next',
            '[rel="next"]' # Added common rel=next attribute
        ]

        processed_pagination_links = set()

        for selector in pagination_selectors:
             try:
                links = soup.select(selector)
                for link in links:
                    if link.has_attr('href'):
                        href = link['href']
                        if not href or href.startswith('#') or href.startswith('javascript:'):
                            continue
                        full_url = urljoin(current_url, href)
                        # Avoid adding the same URL multiple times
                        if full_url not in processed_pagination_links:
                             pagination_links.append(full_url)
                             processed_pagination_links.add(full_url)
             except Exception as e:
                 logger.debug(f"Error processing pagination selector '{selector}': {e}")

        # Simple heuristic: filter out links that look like product URLs themselves
        # (e.g. if a product link accidentally matches a pagination selector)
        final_pagination_links = [
            url for url in pagination_links
            if not self._looks_like_product_path(urlparse(url).path)
        ]

        return final_pagination_links


    def _find_category_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Find category/collection links in a page. (Implementation mostly unchanged, added path filtering)
        """
        category_links = []
        processed_category_links = set()

        # Prefer specific paths/classes first
        category_paths = [
            '/collections/', '/product-category/', '/category/', '/categories/', '/shop/'
            # Removed '/coffee/' as it's too broad and might match product URLs
        ]
        category_classes = [
            'category', 'collection', 'product-category', 'nav-link', 'menu-item' # Added common nav/menu classes
        ]

        # Check links based on path patterns
        for link in soup.find_all('a', href=True):
             try:
                href = link['href']
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue

                # Check if path STARTS WITH a category path (more specific)
                parsed_href = urlparse(href)
                path_lower = parsed_href.path.lower()
                if any(path_lower.startswith(path) for path in category_paths):
                    full_url = urljoin(current_url, href)
                    if full_url not in processed_category_links:
                        # Avoid adding links that look like products
                         if not self._looks_like_product_path(urlparse(full_url).path):
                            category_links.append(full_url)
                            processed_category_links.add(full_url)
             except Exception as e:
                 logger.debug(f"Error processing link href '{href}' for category path: {e}")


        # Check links based on class names
        for class_name in category_classes:
            try:
                 # Use more robust class checking if possible (e.g., regex for exact match or space-separated)
                 # Simple substring check for now: class_=lambda c: c and class_name in c.split()
                for link in soup.find_all('a', class_=lambda c: c and class_name in c.split(), href=True):
                    href = link['href']
                    if not href or href.startswith('#') or href.startswith('javascript:'):
                        continue
                    full_url = urljoin(current_url, href)
                    if full_url not in processed_category_links:
                         # Avoid adding links that look like products
                         if not self._looks_like_product_path(urlparse(full_url).path):
                            category_links.append(full_url)
                            processed_category_links.add(full_url)
            except Exception as e:
                 logger.debug(f"Error processing link class '{class_name}': {e}")

        # Simple heuristic: filter out links that look like product URLs themselves
        # (e.g. if a product link accidentally matches a category selector)
        # This filtering is now done inside the loops

        return category_links

    def _looks_like_product_path(self, path: str) -> bool:
        """
        A simple heuristic to guess if a URL path looks more like a product
        detail page than a category or pagination page. Used to filter
        category/pagination links. More restrictive than the old _is_product_url.
        """
        path_lower = path.lower().strip('/')
        if not path_lower: # Handle empty path after stripping
             return False

        # Indicators that strongly suggest a product page rather than listing/nav
        # Check these FIRST
        product_indicators = [
            '/product/', '/products/', '/p/' # Common explicit product paths
        ]
        # Use f'/.../' to ensure matching whole segments, not substrings within words
        if any(indicator in f'/{path_lower}/' for indicator in product_indicators):
            return True

        # Heuristic: path has multiple segments and last one doesn't suggest listing/pagination
        # And doesn't contain strong non-product indicators (checked next)
        path_parts = path_lower.split('/')
        has_multiple_parts = len(path_parts) > 1
        last_part_is_item_like = path_parts[-1] not in ['all', 'page', 'shop', 'products', 'collections', 'categories'] \
                                 and not path_parts[-1].isdigit() # Avoid simple page numbers
        # Heuristic: path ends with .html or similar (often specific items)
        ends_with_extension = any(path_lower.endswith(ext) for ext in ['.html', '.htm', '.php', '.aspx'])


        # Indicators that suggest it's NOT a product page (listing/nav)
        non_product_indicators = [
            '/collections/', '/category/', '/categories/', '/shop/', '/page/',
            '/search', 'sort_by=', 'filter.', '/tag/', '/blog/', '/news/', '/articles/'
        ]
        # If it contains a strong non-product indicator, assume it's not a product path
        # Check this AFTER the main product indicators
        # Use f'/.../' to ensure matching whole segments
        if any(indicator in f'/{path_lower}/' for indicator in non_product_indicators):
            return False

        # Apply secondary product heuristics only if no strong non-product indicators were found
        if ends_with_extension:
             return True
        if has_multiple_parts and last_part_is_item_like:
            # This is a weaker heuristic, could add false positives
            # Example: /about/team-member-name might trigger this
            # Let's make it more conservative: only trigger if path depth > 2?
            if len(path_parts) > 2:
                 logger.debug(f"Path '{path}' considered product-like due to depth/last part heuristic.")
                 return True


        # Default to false if unsure
        logger.debug(f"Path '{path}' considered non-product by default.")
        return False


    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def __aenter__(self):
        await self._init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

# Example usage (optional)
# async def main():
#     async with HtmlDiscoverer() as discoverer:
#         # products = await discoverer.discover("https://some-coffee-shop.com")
#         products = await discoverer.discover("example.com") # Replace with a real URL for testing
#         for product in products:
#             print(product)
#
# if __name__ == "__main__":
#      # Setup basic logging for testing
#      logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
#      # Run the example
#      # asyncio.run(main())
