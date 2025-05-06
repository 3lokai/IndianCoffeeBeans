# coffee_scraper/scrapers/woocommerce.py
import aiohttp
import json
import re
from common.deepseek_extractor import enhance_with_deepseek
from common.utils import create_slug, fetch_with_retry, is_coffee_product, standardize_coffee_data
from common.cache import get_cached_products, cache_products
from datetime import datetime
from config import USE_DEEPSEEK, SKIP_CACHE

async def scrape_woocommerce(roaster):
    """Scrape products from a WooCommerce store using a two-pass approach."""
    base_url = roaster['website_url'].rstrip('/')
    
    # Check cache first
    cached_products = get_cached_products(roaster)
    if not SKIP_CACHE and cached_products:
        print(f"Using {len(cached_products)} cached products for {roaster['name']}")
        return cached_products
    
    # PASS 1: Identify coffee product URLs
    coffee_products = []
    product_urls = []
    
    # Try API methods first
    api_endpoints = [
        "/wp-json/wc/v3/products",
        "/wp-json/wc/v2/products",
        "/wp-json/wp/v2/product",
        "/products.json"
    ]
    
    async with aiohttp.ClientSession() as session:
        # Try each API endpoint
        for endpoint in api_endpoints:
            try:
                products_url = f"{base_url}{endpoint}?per_page=100"
                print(f"Trying endpoint: {products_url}")
                
                response = await fetch_with_retry(products_url, session)
                data = json.loads(response)
                
                if isinstance(data, list) and len(data) > 0:
                    # Found products via API
                    for product in data:
                        name = product.get('name', product.get('title', {}).get('rendered', ''))
                        if not name:
                            continue
                            
                        # Extract preliminary data for filtering
                        description = extract_description(product)
                        product_type = extract_product_type(product)
                        tags = extract_tags(product)
                        # Get product URL
                        product_url = product.get('permalink', 
                                        product.get('link', 
                                                f"{base_url}/product/{product.get('slug', create_slug(name))}"))
                        # Check if it's coffee
                        if is_coffee_product(name, description, product_type, tags, roaster['name'], product_url):
                            # Get product URL
                            product_url = product.get('permalink', 
                                              product.get('link', 
                                                      f"{base_url}/product/{product.get('slug', create_slug(name))}"))
                            
                            # Add to our list of URLs to visit
                            product_urls.append(product_url)
                            
                            # Store preliminary data
                            coffee_products.append({
                                "name": name,
                                "slug": create_slug(name),
                                "roaster_slug": roaster['slug'],
                                "direct_buy_url": product_url,
                                "preliminary_data": product  # Store for later reference
                            })
                    
                    # Found products, break out of API endpoint loop
                    if product_urls:
                        print(f"Found {len(product_urls)} coffee products via API")
                        break
            except Exception as e:
                print(f"API endpoint {endpoint} failed: {str(e)}")
        
        # If no products found via API, try catalog page scraping
        if not product_urls:
            print("No products found via API. Trying catalog pages...")
            catalog_urls = [
                f"{base_url}/shop",
                f"{base_url}/product-category/coffee",
                f"{base_url}/coffee",
                f"{base_url}/products",
                f"{base_url}/collections/coffee"
            ]
            
            for url in catalog_urls:
                try:
                    html = await fetch_with_retry(url, session)
                    
                    # Extract product links
                    product_blocks = re.findall(r'<li[^>]*class="[^"]*product[^"]*".*?</li>', html, re.DOTALL)
                    for block in product_blocks:
                        link_match = re.search(r'<a href="([^"]+)"[^>]*>.*?<h2[^>]*>([^<]+)</h2>', block, re.DOTALL)
                        if link_match:
                            link = link_match.group(1)
                            name = link_match.group(2).strip()
                            
                            # Basic filtering - could be enhanced
                            if is_coffee_product(name, "", "", [], roaster['name'], link):
                                product_urls.append(link)
                                coffee_products.append({
                                    "name": name,
                                    "slug": create_slug(name),
                                    "roaster_slug": roaster['slug'],
                                    "direct_buy_url": link
                                })
                    
                    if product_urls:
                        print(f"Found {len(product_urls)} coffee products via catalog page")
                        break
                except Exception as e:
                    print(f"Error scraping catalog {url}: {str(e)}")
        
        # PASS 2: Visit each product page to get detailed info
        scraped_coffees = []
        
        for i, product_data in enumerate(coffee_products):
            try:
                product_url = product_data["direct_buy_url"]
                print(f"Visiting product {i+1}/{len(coffee_products)}: {product_url}")
                
                # Get HTML of product page
                product_html = await fetch_with_retry(product_url, session)
                
                # Extract detailed product info
                coffee = {
                    "name": product_data["name"],
                    "slug": product_data["slug"],
                    "roaster_slug": product_data["roaster_slug"],
                    "direct_buy_url": product_url,
                    "is_available": True,  # Default to available
                    "last_scraped_at": datetime.now().isoformat(),
                    "scrape_status": "success"
                }
                
                # Extract description
                description = extract_description_from_html(product_html)
                coffee["description"] = clean_html(description)
                
                # Get product image
                image_url = extract_image_from_html(product_html)
                if image_url:
                    coffee["image_url"] = image_url
                
                # Check if it's a combo
                coffee["is_combo"] = is_combo_pack(coffee["name"], coffee["description"])
                
                # Extract price and variants
                extract_price_from_html(coffee, product_html)
                
                # Extract coffee attributes
                extract_coffee_attributes_from_html(coffee, product_html)
                
                # If we have preliminary data from API, use it to fill gaps
                if "preliminary_data" in product_data:
                    fill_from_api_data(coffee, product_data["preliminary_data"])
                
                # If missing critical attributes, enhance with LLM
                if USE_DEEPSEEK and needs_enhancement(coffee):
                    coffee = await enhance_with_deepseek(coffee, roaster['name'])
                else:
                    # Log missing attributes for analysis
                    missing = []
                    if "roast_level" not in coffee: missing.append("roast_level")
                    if "bean_type" not in coffee: missing.append("bean_type")
                    if "processing_method" not in coffee: missing.append("processing_method")
                    if "flavor_profiles" not in coffee: missing.append("flavor_profiles")
                    
                    if missing:
                        print(f"Missing attributes for {coffee['name']}: {', '.join(missing)}")
                
                scraped_coffees.append(standardize_coffee_data(coffee))
            except Exception as e:
                print(f"Error processing product {product_data['name']}: {str(e)}")
        
        # Cache the results
        cache_products(roaster, scraped_coffees)
        
        return scraped_coffees

# Add these functions to the WooCommerce scraper

def extract_description_from_html(html):
    """Extract product description from HTML."""
    # Try different common description containers
    desc_patterns = [
        r'<div[^>]*class="[^"]*woocommerce-product-details__short-description[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*product-description[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="tab-description"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*summary[^"]*"[^>]*>.*?<p>(.*?)</p>'
    ]
    
    for pattern in desc_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            return match.group(1)
    
    # If no description found, return empty string
    return ""

def extract_image_from_html(html):
    """Extract product image URL from HTML."""
    # Try different image patterns
    img_patterns = [
        r'<img[^>]*class="[^"]*wp-post-image[^"]*"[^>]*src="([^"]+)"',
        r'<div[^>]*class="[^"]*product-images[^"]*"[^>]*>.*?<img[^>]*src="([^"]+)"',
        r'<div[^>]*class="[^"]*woocommerce-product-gallery__image[^"]*"[^>]*>.*?<img[^>]*src="([^"]+)"',
        r'<meta property="og:image" content="([^"]+)"'
    ]
    
    for pattern in img_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            return match.group(1)
    
    return ""

def extract_price_from_html(coffee, html):
    """Extract price information from HTML."""
    # Try to find price
    price_patterns = [
        r'<span class="woocommerce-Price-amount amount">\s*<[^>]*>\s*[^<]*</[^>]*>\s*([0-9,.]+)',
        r'<p[^>]*class="[^"]*price[^"]*"[^>]*>\s*<span[^>]*>\s*<[^>]*>\s*[^<]*</[^>]*>\s*([0-9,.]+)',
        r'<span[^>]*id="price[^"]*"[^>]*>\s*<[^>]*>\s*([0-9,.]+)',
        r'data-product_price="([0-9,.]+)"'
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                price = float(match.group(1).replace(',', ''))
                # Default to 250g price
                coffee["price_250g"] = price
                break
            except ValueError:
                continue
    
    # Try to extract variants/weights
    variant_container_patterns = [
        r'<table[^>]*class="[^"]*variations[^"]*"[^>]*>(.*?)</table>',
        r'<form[^>]*class="[^"]*variations_form[^"]*"[^>]*>(.*?)</form>'
    ]
    
    for pattern in variant_container_patterns:
        container_match = re.search(pattern, html, re.DOTALL)
        if container_match:
            variants_html = container_match.group(1)
            
            # Look for weight options
            weight_options = re.findall(r'<option[^>]*value="([^"]+)"[^>]*>([^<]+)', variants_html)
            
            weight_prices = []
            for value, label in weight_options:
                # Extract weight from label
                weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', label.lower())
                if weight_match:
                    weight_value = float(weight_match.group(1))
                    weight_unit = weight_match.group(2).lower()
                    
                    # Convert to grams
                    if 'kg' in weight_unit:
                        weight_grams = int(weight_value * 1000)
                    else:
                        weight_grams = int(weight_value)
                    
                    # Try to find price for this variant
                    price_match = re.search(rf'data-value="{re.escape(value)}"[^>]*>.*?([0-9,.]+)', variants_html, re.DOTALL)
                    if price_match:
                        try:
                            price = float(price_match.group(1).replace(',', ''))
                            weight_prices.append((weight_grams, price))
                        except ValueError:
                            continue
            
            # Assign prices to standard categories
            if weight_prices:
                weight_prices.sort(key=lambda x: x[0])
                for weight_grams, price in weight_prices:
                    if weight_grams <= 100:
                        coffee["price_100g"] = price
                    elif weight_grams <= 250:
                        coffee["price_250g"] = price
                    elif weight_grams <= 500:
                        coffee["price_500g"] = price
                    else:
                        coffee["price_1kg"] = price

def extract_coffee_attributes_from_html(coffee, html):
    """Extract coffee attributes from HTML."""
    full_text = clean_html(html).lower()
    
    # Extract roast level
    roast_patterns = [
        (r'\b(light)\s+roast\b', 'light'),
        (r'\b(medium[\s-]*light)\s+roast\b', 'medium-light'),
        (r'\b(medium)\s+roast\b', 'medium'),
        (r'\b(medium[\s-]*dark)\s+roast\b', 'medium-dark'),
        (r'\b(dark)\s+roast\b', 'dark'),
    ]
    
    for pattern, roast in roast_patterns:
        if re.search(pattern, full_text, re.IGNORECASE):
            coffee["roast_level"] = roast
            break
    
    # Extract bean type
    if "arabica" in full_text and "robusta" in full_text:
        coffee["bean_type"] = "blend"
    elif "arabica" in full_text:
        coffee["bean_type"] = "arabica"
    elif "robusta" in full_text:
        coffee["bean_type"] = "robusta"
    
    # Check if it's a blend
    coffee["is_blend"] = "blend" in coffee.get('name', '').lower() or "blend" in full_text
    
    # Extract processing method
    process_patterns = [
        (r'\b(washed|wet processed)\b', 'washed'),
        (r'\b(natural|dry processed)\b', 'natural'),
        (r'\b(honey|pulped natural)\b', 'honey'),
        (r'\b(anaerobic)\b', 'anaerobic'),
    ]
    
    for pattern, process in process_patterns:
        if re.search(pattern, full_text, re.IGNORECASE):
            coffee["processing_method"] = process
            break
    
    # Extract flavor profiles
    flavor_profiles = []
    common_flavors = ["chocolate", "fruity", "nuts", "caramel", "citrus", "berry", "floral", "spice"]
    
    for flavor in common_flavors:
        if flavor in full_text:
            flavor_profiles.append(flavor)
    
    if flavor_profiles:
        coffee["flavor_profiles"] = flavor_profiles

def extract_description(product):
    """Extract description from API product data."""
    if 'description' in product:
        if isinstance(product['description'], str):
            return product['description']
        elif isinstance(product['description'], dict) and 'rendered' in product['description']:
            return product['description']['rendered']
    elif 'content' in product and 'rendered' in product['content']:
        return product['content']['rendered']
    return ""

def extract_product_type(product):
    """Extract product type from API product data."""
    if 'categories' in product:
        categories = [cat.get('name', '').lower() for cat in product['categories']]
        if any('coffee' in cat for cat in categories):
            return 'coffee'
    return ""

def extract_tags(product):
    """Extract tags from API product data."""
    tags = []
    if 'tags' in product:
        tags = [tag.get('name', '').lower() for tag in product['tags']]
    elif 'attributes' in product:
        for attr in product['attributes']:
            if 'options' in attr and isinstance(attr['options'], list):
                tags.extend([opt.lower() for opt in attr['options']])
    return tags

def fill_from_api_data(coffee, api_data):
    """Fill in missing data from API data if available."""
    # Fill in missing image
    if 'image_url' not in coffee and 'images' in api_data and len(api_data['images']) > 0:
        coffee["image_url"] = api_data['images'][0].get('src', api_data['images'][0].get('source_url', ''))
    
    # Fill in price if missing
    if 'price_250g' not in coffee and 'price' in api_data:
        try:
            coffee["price_250g"] = float(api_data['price'])
        except (ValueError, TypeError):
            pass
    
    # Fill in availability
    if 'in_stock' in api_data:
        coffee["is_available"] = api_data['in_stock']
    elif 'stock_status' in api_data:
        coffee["is_available"] = api_data['stock_status'] == 'instock'

async def process_woo_variations(coffee, product, base_url, session):
    """Process WooCommerce product variations."""
    if isinstance(product['variations'], list) and all(isinstance(x, str) for x in product['variations']):
        # Variations are URLs we need to fetch
        weight_prices = []
        
        for var_url in product['variations'][:5]:  # Limit to first 5 variations to avoid too many requests
            try:
                var_response = await fetch_with_retry(var_url, session)
                var_data = json.loads(var_response)
                
                var_name = var_data.get('name', '')
                var_price = float(var_data.get('price', 0))
                
                # Extract weight from variation name
                weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', var_name.lower())
                
                if weight_match:
                    weight_value = float(weight_match.group(1))
                    weight_unit = weight_match.group(2).lower()
                    
                    # Convert to grams
                    if 'kg' in weight_unit:
                        weight_grams = int(weight_value * 1000)
                    else:
                        weight_grams = int(weight_value)
                    
                    weight_prices.append((weight_grams, var_price))
            except Exception as e:
                print(f"Error fetching variation: {str(e)}")
                continue
        
        # Sort by weight ascending
        weight_prices.sort(key=lambda x: x[0])
        
        # Map to our standardized weight categories
        for weight_grams, price in weight_prices:
            if weight_grams <= 100:
                coffee["price_100g"] = price
            elif weight_grams <= 250:
                coffee["price_250g"] = price
            elif weight_grams <= 500:
                coffee["price_500g"] = price
            else:
                coffee["price_1kg"] = price

def extract_weight_from_name(coffee, name, description):
    """Extract weight from product name or description."""
    weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', (name + ' ' + description).lower())
    
    if weight_match:
        weight_value = float(weight_match.group(1))
        weight_unit = weight_match.group(2).lower()
        
        # Convert to grams
        if 'kg' in weight_unit:
            weight_grams = int(weight_value * 1000)
        else:
            weight_grams = int(weight_value)
        
        # Assign to appropriate weight category
        if weight_grams <= 100:
            coffee["price_100g"] = coffee.get("price_250g", 0)
        elif weight_grams <= 250:
            # Already assigned to price_250g
            pass
        elif weight_grams <= 500:
            coffee["price_500g"] = coffee.get("price_250g", 0)
            del coffee["price_250g"]
        else:
            coffee["price_1kg"] = coffee.get("price_250g", 0)
            del coffee["price_250g"]

def extract_coffee_attributes(coffee, product, description, categories, tags):
    """Extract coffee attributes from product data and description."""
    # Extract roast level
    roast_patterns = [
        (r'\b(light)\s+roast\b', 'light'),
        (r'\b(medium[\s-]*light)\s+roast\b', 'medium-light'),
        (r'\b(medium)\s+roast\b', 'medium'),
        (r'\b(medium[\s-]*dark)\s+roast\b', 'medium-dark'),
        (r'\b(dark)\s+roast\b', 'dark'),
    ]
    
    # First check in tags which are more reliable
    for tag in tags:
        for pattern, roast in roast_patterns:
            if re.search(pattern, tag, re.IGNORECASE):
                coffee["roast_level"] = roast
                break
    
    # Then try in description
    if "roast_level" not in coffee:
        for pattern, roast in roast_patterns:
            if re.search(pattern, description, re.IGNORECASE):
                coffee["roast_level"] = roast
                break
            
    # Extract bean type (check tags first, then description)
    bean_indicators = {
        'arabica': 'arabica',
        'robusta': 'robusta',
        'blend': 'blend'
    }
    
    for tag in tags:
        for indicator, bean_type in bean_indicators.items():
            if indicator in tag.lower():
                coffee["bean_type"] = bean_type
                break
    
    if "bean_type" not in coffee:
        if "arabica" in description.lower() and "robusta" in description.lower():
            coffee["bean_type"] = "blend"
        elif "arabica" in description.lower():
            coffee["bean_type"] = "arabica"
        elif "robusta" in description.lower():
            coffee["bean_type"] = "robusta"
        
    # Check if it's a blend
    coffee["is_blend"] = "blend" in coffee.get('name', '').lower() or "blend" in description.lower()
    
    # Try to extract processing method
    process_patterns = [
        (r'\b(washed|wet processed)\b', 'washed'),
        (r'\b(natural|dry processed)\b', 'natural'),
        (r'\b(honey|pulped natural)\b', 'honey'),
        (r'\b(anaerobic)\b', 'anaerobic'),
    ]
    
    for pattern, process in process_patterns:
        if re.search(pattern, description, re.IGNORECASE):
            coffee["processing_method"] = process
            break
            
    # Extract flavor profiles from tags or description
    flavor_profiles = []
    common_flavors = ["chocolate", "fruity", "nuts", "caramel", "citrus", "berry", "floral", "spice"]
    
    # Check tags first
    for tag in tags:
        tag = tag.lower()
        if tag in common_flavors:
            flavor_profiles.append(tag)
            
    # Then check description
    for flavor in common_flavors:
        if flavor in description.lower() and flavor not in flavor_profiles:
            flavor_profiles.append(flavor)
            
    if flavor_profiles:
        coffee["flavor_profiles"] = flavor_profiles
    
    return coffee

async def scrape_product_catalog_pages(base_url, session):
    """Fallback: Scrape product catalog pages for basic product data."""
    products = []
    catalog_urls = [
        f"{base_url}/shop",
        f"{base_url}/product-category/coffee",
        f"{base_url}/coffee",
        f"{base_url}/products",
        f"{base_url}/collections/coffee"
    ]
    
    for url in catalog_urls:
        try:
            html = await fetch_with_retry(url, session)
            
            # Extract product blocks using regex
            # This is a simplified approach - in real implementation, you'd want to use BeautifulSoup
            product_links = re.findall(r'<a href="([^"]+)"[^>]*class="[^"]*product[^"]*"', html)
            if not product_links:
                product_links = re.findall(r'<a href="([^"]+/product/[^"]+)"', html)
            
            print(f"Found {len(product_links)} product links on {url}")
            
            # Process first 10 products to keep it reasonable
            for link in product_links[:10]:
                # Clean the URL
                if not link.startswith('http'):
                    link = f"{base_url.rstrip('/')}/{link.lstrip('/')}"
                
                try:
                    product_html = await fetch_with_retry(link, session)
                    
                    # Extract basic product data
                    title_match = re.search(r'<h1[^>]*class="[^"]*product_title[^"]*"[^>]*>([^<]+)</h1>', product_html)
                    if not title_match:
                        title_match = re.search(r'<title>([^<|]+)', product_html)
                    
                    if title_match:
                        title = title_match.group(1).strip()
                        
                        # Get description
                        desc_match = re.search(r'<div[^>]*class="[^"]*product-description[^"]*"[^>]*>(.*?)</div>', product_html, re.DOTALL)
                        if not desc_match:
                            desc_match = re.search(r'<div[^>]*class="[^"]*woocommerce-product-details__short-description[^"]*"[^>]*>(.*?)</div>', product_html, re.DOTALL)
                        
                        description = desc_match.group(1) if desc_match else ""
                        
                        # Get price
                        price_match = re.search(r'<span class="woocommerce-Price-amount amount"><bdi><span class="woocommerce-Price-currencySymbol">[^<]+</span>([^<]+)</bdi></span>', product_html)
                        price = float(price_match.group(1).replace(',', '').strip()) if price_match else 0
                        
                        # Get image
                        img_match = re.search(r'<img[^>]*class="[^"]*wp-post-image[^"]*"[^>]*src="([^"]+)"', product_html)
                        if not img_match:
                            img_match = re.search(r'<div[^>]*class="[^"]*product-image[^"]*"[^>]*>.*?<img[^>]*src="([^"]+)"', product_html, re.DOTALL)
                        
                        image_url = img_match.group(1) if img_match else ""
                        
                        products.append({
                            'name': title,
                            'description': description,
                            'price': price,
                            'permalink': link,
                            'images': [{'src': image_url}] if image_url else [],
                            'categories': [],
                            'tags': []
                        })
                except Exception as e:
                    print(f"Error scraping product {link}: {str(e)}")
                    continue
            
            # If we found products, return them
            if products:
                return products
        except Exception as e:
            print(f"Error scraping catalog {url}: {str(e)}")
            continue
    
    return products

def clean_html(html_text):
    """Remove HTML tags from text."""
    if not html_text:
        return ""
    # Basic HTML cleaning - for production use a proper HTML parser
    text = re.sub(r'<[^>]+>', ' ', html_text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def is_combo_pack(name, description):
    """Detect if product is a combo/bundle of multiple coffees."""
    combo_indicators = [
        "pack", "bundle", "set", "collection", "sample", "trio", "duo", 
        "tasting", "gift box", "variety", "assortment"
    ]
    
    # Check title first (most reliable)
    if any(indicator in name.lower() for indicator in combo_indicators):
        # Validate with number indicators
        if any(num in name.lower() for num in ["2 x", "3 x", "two", "three", "multiple"]):
            return True
            
    # Check description as backup
    if description and "includes" in description.lower():
        if any(indicator in description.lower() for indicator in combo_indicators):
            return True
            
    return False

def needs_enhancement(coffee):
    """Check if coffee object needs LLM enhancement."""
    # Check if critical fields are missing
    missing_fields = []
    
    if "roast_level" not in coffee:
        missing_fields.append("roast_level")
        
    if "bean_type" not in coffee:
        missing_fields.append("bean_type")
        
    if "processing_method" not in coffee and not coffee.get("is_blend", False):
        missing_fields.append("processing_method")
        
    if "flavor_profiles" not in coffee:
        missing_fields.append("flavor_profiles")
        
    # Only enhance if multiple fields are missing - saves on API calls
    return len(missing_fields) >= 2