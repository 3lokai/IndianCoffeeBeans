# scrapers/discoverers/structured_data_discoverer.py
import logging
import asyncio
import json
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from common.utils import slugify
from common.product_classifier import is_likely_coffee_product
from config import USER_AGENT, REQUEST_TIMEOUT, CRAWL_DELAY

logger = logging.getLogger(__name__)

class StructuredDataDiscoverer:
    """
    Discovers product URLs from structured data in HTML pages.
    
    This looks for schema.org Product data, JSON-LD, and other structured formats.
    """
    
    def __init__(self):
        """Initialize the structured data discoverer"""
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
        Discover product URLs from structured data.
        
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
        
        # Try common pages that might contain structured data
        catalog_paths = [
            '', # Homepage
            '/shop',
            '/products',
            '/collections/coffee',
            '/collections/all',
            '/product-category/coffee',
            '/coffee',
            '/store',
            '/beans'
        ]
        
        # Start with a list of pages to check
        pages_to_check = [base_url + path for path in catalog_paths]
        
        discovered_products = []
        
        for page_url in pages_to_check:
            try:
                # Fetch the page
                async with self.session.get(page_url) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch {page_url}: {response.status}")
                        continue
                        
                    html = await response.text()
                    
                    # Extract products from structured data
                    products = await self._extract_structured_data(html, page_url)
                    
                    if products:
                        discovered_products.extend(products)
                        logger.info(f"Found {len(products)} products from structured data on {page_url}")
                        
            except Exception as e:
                logger.warning(f"Error processing {page_url}: {str(e)}")
                continue
                
            # Be nice to the server
            await asyncio.sleep(CRAWL_DELAY)
            
        logger.info(f"Discovered {len(discovered_products)} products from structured data")
        return discovered_products
    
    async def _extract_structured_data(self, html: str, page_url: str) -> List[Dict[str, Any]]:
        """
        Extract product data from structured data in HTML.
        
        Args:
            html: HTML content
            page_url: URL of the page
            
        Returns:
            List of discovered products
        """
        products = []
        
        # Parse the HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # First, try JSON-LD format
        json_ld_products = self._extract_json_ld_products(soup, page_url)
        if json_ld_products:
            products.extend(json_ld_products)
            
        # Then, try microdata format
        microdata_products = self._extract_microdata_products(soup, page_url)
        if microdata_products:
            products.extend(microdata_products)
            
        # Check if we need to fetch additional product pages
        additional_urls = self._extract_product_urls_from_data(soup, page_url)
        
        # Fetch and process additional product pages
        if additional_urls:
            additional_products = await self._process_additional_urls(additional_urls)
            if additional_products:
                products.extend(additional_products)
                
        return products
    
    def _extract_json_ld_products(self, soup: BeautifulSoup, page_url: str) -> List[Dict[str, Any]]:
        """
        Extract product data from JSON-LD structured data.
        
        Args:
            soup: BeautifulSoup parsed HTML
            page_url: URL of the page
            
        Returns:
            List of discovered products
        """
        products = []
        
        # Find all script tags with type="application/ld+json"
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_ld_scripts:
            try:
                # Parse the JSON
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
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
                            # Get URL, try multiple properties and resolve relative URLs
                            item_url_prop = item.get('url') or item.get('offers', {}).get('url') # Offers might contain the specific variant URL
                            item_url = urljoin(page_url, item_url_prop) if item_url_prop else page_url # Fallback to page_url if no specific item URL
                            category = item.get('category') # Can be string or list
                            categories = [category] if isinstance(category, str) else category
                            
                            # REMOVED: Centralized filtering applied later in DiscoveryManager
                            # Use the centralized classifier
                            # if is_likely_coffee_product(name=name, url=item_url, categories=categories, description=description):
                            # Extract product data always, filter later
                            product = self._extract_product_from_json_ld(item, page_url)
                                
                            if product:
                                products.append(product)
            
            except Exception as e:
                logger.debug(f"Error parsing JSON-LD: {str(e)}")
                continue
                
        return products
    
    def _extract_product_from_json_ld(self, data: Dict[str, Any], page_url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from JSON-LD item.
        
        Args:
            data: JSON-LD data
            page_url: URL of the page
            
        Returns:
            Product data dict if valid, None otherwise
        """
        # Extract basic product info
        name = data.get('name')
        
        if not name:
            return None
            
        # Get product URL
        url = data.get('url')
        if url:
            # Normalize URL if it's relative
            if not url.startswith(('http://', 'https://')):
                url = urljoin(page_url, url)
        else:
            # Use page URL as fallback
            url = page_url
            
        # Create product entry
        product = {
            "name": name,
            "slug": slugify(name),
            "direct_buy_url": url,
            "discovery_method": "structured_data"
        }
        
        # Get description
        description = data.get('description')
        if description:
            product["description"] = description
            
        # Get image
        image = data.get('image')
        if image:
            # Handle different image formats
            if isinstance(image, list) and image:
                # Take first image
                image_item = image[0]
                if isinstance(image_item, dict):
                    image_url = image_item.get('url')
                else:
                    image_url = image_item
            elif isinstance(image, dict):
                image_url = image.get('url')
            else:
                image_url = image
                
            # Normalize image URL
            if image_url and not image_url.startswith(('http://', 'https://')):
                image_url = urljoin(page_url, image_url)
                
            if image_url:
                product["image_url"] = image_url
                
        # Get price
        offers = data.get('offers')
        if offers:
            # Handle different offers formats
            if isinstance(offers, list) and offers:
                offer = offers[0]
            else:
                offer = offers
                
            if isinstance(offer, dict):
                price = offer.get('price')
                if price:
                    try:
                        product["price"] = float(price)
                    except ValueError:
                        pass
                        
                availability = offer.get('availability')
                if availability:
                    product["is_available"] = 'InStock' in availability
        
        # Store raw data for later extraction
        product["structured_data"] = data
        
        return product
    
    def _extract_microdata_products(self, soup: BeautifulSoup, page_url: str) -> List[Dict[str, Any]]:
        """
        Extract product data from microdata structured data.
        
        Args:
            soup: BeautifulSoup parsed HTML
            page_url: URL of the page
            
        Returns:
            List of discovered products
        """
        products = []
        
        # Find all elements with itemtype="http://schema.org/Product"
        product_elements = soup.find_all(itemtype=lambda t: t and 'schema.org/Product' in t)
        
        for element in product_elements:
            try:
                # Extract product data
                product = self._extract_product_from_microdata(element, page_url)
                
                # REMOVED: Centralized filtering applied later in DiscoveryManager
                # if product and is_likely_coffee_product(name=product['name'], url=product['direct_buy_url'], categories=[], description=product.get('description')):
                # Add product always, filter later
                if product:
                    products.append(product)
                    
            except Exception as e:
                logger.debug(f"Error parsing microdata: {str(e)}")
                continue
                
        return products
    
    def _extract_product_from_microdata(self, element: BeautifulSoup, page_url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from microdata element.
        
        Args:
            element: BeautifulSoup element with microdata
            page_url: URL of the page
            
        Returns:
            Product data dict if valid, None otherwise
        """
        # Extract product name
        name_elem = element.find(itemprop='name')
        if name_elem:
            name = name_elem.get_text(strip=True)
        else:
            return None
            
        # Get product URL
        url_elem = element.find(itemprop='url')
        if url_elem and url_elem.has_attr('href'):
            url = url_elem['href']
            # Normalize URL if it's relative
            if not url.startswith(('http://', 'https://')):
                url = urljoin(page_url, url)
        else:
            # Use page URL as fallback
            url = page_url
            
        # Create product entry
        product = {
            "name": name,
            "slug": slugify(name),
            "direct_buy_url": url,
            "discovery_method": "structured_data"
        }
        
        # Get description
        desc_elem = element.find(itemprop='description')
        if desc_elem:
            product["description"] = desc_elem.get_text(strip=True)
            
        # Get image
        img_elem = element.find(itemprop='image')
        if img_elem:
            if img_elem.name == 'img' and img_elem.has_attr('src'):
                image_url = img_elem['src']
            elif img_elem.has_attr('content'):
                image_url = img_elem['content']
            else:
                image_url = None
                
            # Normalize image URL
            if image_url and not image_url.startswith(('http://', 'https://')):
                image_url = urljoin(page_url, image_url)
                
            if image_url:
                product["image_url"] = image_url
                
        # Get price
        price_elem = element.find(itemprop='price')
        if price_elem:
            price_text = price_elem.get_text(strip=True) or price_elem.get('content')
            if price_text:
                try:
                    # Remove currency symbols and parse
                    price_text = price_text.replace(',', '').replace('₹', '').replace('€', '')
                    product["price"] = float(price_text)
                except ValueError:
                    pass
                    
        # Check availability
        avail_elem = element.find(itemprop='availability')
        if avail_elem:
            avail_text = avail_elem.get_text(strip=True) or avail_elem.get('href') or avail_elem.get('content')
            if avail_text:
                product["is_available"] = 'InStock' in avail_text
                
        return product
    
    def _extract_product_urls_from_data(self, soup: BeautifulSoup, page_url: str) -> List[str]:
        """
        Extract product URLs from structured data that might need additional fetching.
        
        Args:
            soup: BeautifulSoup parsed HTML
            page_url: URL of the page
            
        Returns:
            List of product URLs to fetch
        """
        urls = []
        
        # Look for itemList in JSON-LD
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_ld_scripts:
            try:
                # Parse the JSON
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
                items_to_check = []
                
                if isinstance(data, list):
                    items_to_check.extend(data)
                elif isinstance(data, dict):
                    # Handle @graph structure
                    if '@graph' in data:
                        items_to_check.extend(data['@graph'])
                    else:
                        items_to_check.append(data)
                
                # Process each item
                for item in items_to_check:
                    # Check if it's an ItemList
                    if isinstance(item, dict) and item.get('@type') in ['ItemList', 'CollectionPage', 'Product']:
                        # Extract itemListElement
                        list_elements = item.get('itemListElement', [])
                        
                        if not isinstance(list_elements, list):
                            list_elements = [list_elements]
                            
                        for list_item in list_elements:
                            if isinstance(list_item, dict):
                                # Get item URL
                                item_url = None
                                
                                # Case 1: Direct URL
                                if 'url' in list_item:
                                    item_url = list_item['url']
                                # Case 2: Nested item
                                elif 'item' in list_item and isinstance(list_item['item'], dict):
                                    item_url = list_item['item'].get('url')
                                    
                                if item_url:
                                    # Normalize URL if it's relative
                                    if not item_url.startswith(('http://', 'https://')):
                                        item_url = urljoin(page_url, item_url)
                                        
                                    urls.append(item_url)
                
            except Exception as e:
                logger.debug(f"Error parsing JSON-LD for product URLs: {str(e)}")
                continue
                
        return urls
    
    async def _process_additional_urls(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch and process additional product URLs.
        
        Args:
            urls: List of URLs to fetch
            
        Returns:
            List of discovered products
        """
        products = []
        
        # Process URLs in batches to avoid overwhelming the server
        batch_size = 5
        
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            
            # Create tasks
            tasks = []
            for url in batch:
                tasks.append(self._fetch_and_extract_product(url))
                
            # Wait for all tasks to complete
            batch_results = await asyncio.gather(*tasks)
            
            # Add valid results to products
            for result in batch_results:
                if result:
                    products.append(result)
                    
            # Be nice to the server
            await asyncio.sleep(CRAWL_DELAY)
            
        return products
    
    async def _fetch_and_extract_product(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a product page and extract product data.
        
        Args:
            url: URL of the product page
            
        Returns:
            Product data dict if valid, None otherwise
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
                
                # Extract product data
                # First, try JSON-LD
                json_ld_products = self._extract_json_ld_products(soup, url)
                if json_ld_products:
                    return json_ld_products[0]
                    
                # Then, try microdata
                microdata_products = self._extract_microdata_products(soup, url)
                if microdata_products:
                    return microdata_products[0]
                    
                # If structured data extraction failed, try basic extraction
                # Extract product name
                name = None
                for selector in ['h1', '.product_title', '.product-title', '.title']:
                    heading = soup.select_one(selector)
                    if heading:
                        name = heading.get_text(strip=True)
                        break
                        
                if not name:
                    title = soup.find('title')
                    if title:
                        name = title.get_text(strip=True)
                        # Remove site name if present
                        if ' - ' in name:
                            name = name.split(' - ')[0]
                
                if not name:
                    return None
                    
                # Create product entry
                product = {
                    "name": name,
                    "slug": slugify(name),
                    "direct_buy_url": url,
                    "discovery_method": "structured_data_fallback"
                }
                
                # Extract meta description
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc and meta_desc.get('content'):
                    product["description"] = meta_desc.get('content')
                    
                # Extract image
                og_image = soup.find('meta', property='og:image')
                if og_image and og_image.get('content'):
                    product["image_url"] = og_image.get('content')
                
                # Check if it's a coffee product
                if not is_likely_coffee_product(name=name, url=url, categories=[], description=product.get('description')):
                    return None
                    
                return product
                
        except Exception as e:
            logger.warning(f"Error fetching product page {url}: {str(e)}")
            return None
    
    async def close(self):
        """Close resources"""
        if self.session and not self.session.closed:
            await self.session.close()