# coffee_scraper/scrapers/shopify.py
import aiohttp
import json
import re
from common.utils import create_slug, fetch_with_retry, is_coffee_product
from common.cache import get_cached_products, cache_products
from common.deepseek_extractor import enhance_with_deepseek
from datetime import datetime
from config import USE_DEEPSEEK, SKIP_CACHE
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from common.utils import standardize_coffee_data

def process_variants(coffee, product):
    """Process variants with improved weight standardization."""
    variants = product.get('variants', [])
    if not variants:
        return
    
    # Extract all weight/price pairs
    weight_prices = []
    
    for variant in variants:
        variant_title = variant.get('title', '').lower()
        price = float(variant.get('price', 0))
        
        # Try multiple patterns for weight extraction
        # Match patterns like: 250g, 250 gm, 250 grams, 250gram, 0.25kg
        weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', variant_title)
        
        if weight_match:
            weight_value = float(weight_match.group(1))
            weight_unit = weight_match.group(2).lower()
            
            # Convert to grams
            if 'kg' in weight_unit:
                weight_grams = int(weight_value * 1000)
            else:
                weight_grams = int(weight_value)
            
            weight_prices.append((weight_grams, price))
    
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
    
    # If no variant has detected weight, use first variant price as default 250g
    if not any(k in coffee for k in ["price_100g", "price_250g", "price_500g", "price_1kg"]):
        coffee["price_250g"] = float(variants[0].get('price', 0))

def contains_any(text, terms):
    return any(re.search(rf'\b{re.escape(term)}\b', text) for term in terms)

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

def extract_coffee_attributes(coffee, product, description):
    """Extract coffee attributes from product data and description."""
    # Extract roast level
    roast_patterns = [
        (r'\b(light)\s+roast\b', 'light'),
        (r'\b(medium[\s-]*light)\s+roast\b', 'medium-light'),
        (r'\b(medium)\s+roast\b', 'medium'),
        (r'\b(medium[\s-]*dark)\s+roast\b', 'medium-dark'),
        (r'\b(dark)\s+roast\b', 'dark'),
    ]
    
    # Try to find roast level in product data
    for pattern, roast in roast_patterns:
        if re.search(pattern, description, re.IGNORECASE):
            coffee["roast_level"] = roast
            break
            
    # Extract bean type
    if "arabica" in description.lower() and "robusta" in description.lower():
        coffee["bean_type"] = "blend"
    elif "arabica" in description.lower():
        coffee["bean_type"] = "arabica"
    elif "robusta" in description.lower():
        coffee["bean_type"] = "robusta"
        
    # Check if it's a blend
    coffee["is_blend"] = "blend" in product.get('title', '').lower() or "blend" in description.lower()
    
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
    for tag in product.get('tags', []):
        if tag.lower() in common_flavors:
            flavor_profiles.append(tag.lower())
            
    # Then check description
    for flavor in common_flavors:
        if flavor in description.lower() and flavor not in flavor_profiles:
            flavor_profiles.append(flavor)
            
    if flavor_profiles:
        coffee["flavor_profiles"] = flavor_profiles
    
    return coffee

def clean_html(html_text):
    """Remove HTML tags from text."""
    if not html_text:
        return ""
    # Basic HTML cleaning - for production use a proper HTML parser
    text = re.sub(r'<[^>]+>', ' ', html_text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

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

async def scrape_shopify(roaster):
    """Scrape products from a Shopify store with pagination support."""
    base_url = roaster['website_url'].rstrip('/')
    
    # Check cache first
    cached_products = get_cached_products(roaster)
    if not SKIP_CACHE and cached_products:
        print(f"Using {len(cached_products)} cached products for {roaster['name']}")
        return cached_products
    
    scraped_coffees = []
    page = 1
    has_more = True
    
    async with aiohttp.ClientSession() as session:
        while has_more:
            # Shopify pagination: /products.json?limit=250&page=X
            products_url = f"{base_url}/products.json?limit=250&page={page}"
            
            try:
                products_data = await fetch_with_retry(products_url, session)
                products_json = json.loads(products_data)
                
                products = products_json.get('products', [])
                
                # If we got fewer than 250 products, we've reached the end
                if len(products) < 250:
                    has_more = False
                
                if not products:
                    break
                
                for product in products:
                    # Extract core product data
                    name = product.get('title')
                    if not name:
                        continue
                        
                    description = product.get('body_html', '')
                    
                    # Skip non-coffee products based on product type and tags
                    product_type = product.get('product_type', '').lower()
                    tags = [tag.lower() for tag in product.get('tags', [])]
                    
                    # Detect if this is coffee or something else
                    
                    if not is_coffee_product(name, description, product_type, tags,  roaster['name'], product.get('handle', '')):
                        continue
                        
                    # Create basic product object
                    coffee = {
                        "name": name,
                        "slug": create_slug(name),
                        "roaster_slug": roaster['slug'],
                        "description": clean_html(description),
                        "direct_buy_url": f"{base_url}/products/{product.get('handle')}",
                        "is_available": product.get('available', True),
                        "is_combo": is_combo_pack(name, description),
                        "last_scraped_at": datetime.now().isoformat(),
                        "scrape_status": "success"
                    }
                    
                    # Get product image
                    if product.get('images') and len(product.get('images')) > 0:
                        coffee["image_url"] = product['images'][0].get('src')
                    
                    # Process pricing using improved variant handling
                    process_variants(coffee, product)
                    
                    # Extract as many attributes as possible
                    coffee = extract_coffee_attributes(coffee, product, description)
                    
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
                
                page += 1
                print(f"Processed page {page-1} with {len(products)} products for {roaster['name']}")
                
            except Exception as e:
                print(f"Error on page {page} for {roaster['name']}: {e}")
                has_more = False
    
    # Cache the results before returning
    cache_products(roaster, scraped_coffees)
    
    return scraped_coffees

async def scrape_shopify_from_sitemap(base_url, roaster):
    sitemap_url = f"{base_url}/sitemap_products_1.xml"
    coffee_products = []

    async with aiohttp.ClientSession() as session:
        try:
            sitemap_xml = await fetch_with_retry(sitemap_url, session)
            root = ET.fromstring(sitemap_xml)

            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            product_links = [loc.text for loc in root.findall('.//ns:loc', ns) if "/products/" in loc.text]
            print(f"🧭 Found {len(product_links)} product URLs via sitemap.")

            for link in product_links:
                try:
                    html = await fetch_with_retry(link, session)
                    soup = BeautifulSoup(html, 'html.parser')

                    name = soup.find("h1")
                    name = name.get_text(strip=True) if name else ""

                    desc_tag = soup.find("meta", attrs={"name": "description"})
                    desc = desc_tag["content"] if desc_tag else ""

                    if not is_coffee_product(name, desc, "", []):
                        continue

                    coffee = {
                        "name": name,
                        "slug": create_slug(name),
                        "roaster_slug": roaster['slug'],
                        "description": clean_html(desc),
                        "direct_buy_url": link,
                        "is_available": True,
                        "last_scraped_at": datetime.now().isoformat(),
                        "scrape_status": "success"
                    }

                    img_tag = soup.find("meta", property="og:image")
                    if img_tag:
                        coffee["image_url"] = img_tag.get("content")

                    if USE_DEEPSEEK and needs_enhancement(coffee):
                        coffee = await enhance_with_deepseek(coffee, roaster['name'])

                    coffee_products.append(coffee)

                except Exception as e:
                    print(f"❌ Error parsing {link}: {e}")
                    continue

        except Exception as e:
            print(f"❌ Failed to load sitemap: {e}")

    return coffee_products

