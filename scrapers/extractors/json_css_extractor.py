# scrapers/extractors/json_css_extractor.py
import logging
import json
import re
from typing import Dict, Any, Optional, List, Union

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from common.utils import slugify
from common.models import CoffeeModel, RoastLevel, BeanType, ProcessingMethod

logger = logging.getLogger(__name__)

class JsonCssExtractor:
    """
    Extracts detailed product information using Crawl4AI with JSON-CSS schema.
    
    This is the primary extractor that uses predefined selectors based on the
    platform type to extract structured data from product pages.
    """
    
    def __init__(self, browser_config: Optional[BrowserConfig] = None):
        """
        Initialize the extractor.
        
        Args:
            browser_config: Optional custom browser configuration for Crawl4AI
        """
        self.browser_config = browser_config or BrowserConfig(
            headless=True,
            ignore_https_errors=True,
            viewport_width=1280,
            viewport_height=800
        )
        
        # Initialize schema store
        self.schemas = {
            "shopify": self._create_shopify_schema(),
            "woocommerce": self._create_woocommerce_schema(),
            "generic": self._create_generic_schema()
        }
        
    async def extract(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract detailed product information.
        
        Args:
            product: Product dict with at minimum a direct_buy_url
            
        Returns:
            Enhanced product dict with extracted information
        """
        if not product.get("direct_buy_url"):
            logger.error("Product missing direct_buy_url")
            return product
            
        # Determine platform and schema
        platform = product.get("platform", "generic")
        schema = self.schemas.get(platform, self.schemas["generic"])
        
        try:
            # Create crawler instance
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                # Configure extraction
                extraction_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    extraction_strategy=JsonCssExtractionStrategy(schema),
                    wait_until="domcontentloaded"
                )
                
                # Run extraction
                logger.info(f"Extracting product from {product['direct_buy_url']}")
                result = await crawler.arun(url=product["direct_buy_url"], config=extraction_config)
                
                if not result.success:
                    logger.warning(f"Extraction failed for {product['direct_buy_url']}")
                    return product
                    
                # Process extracted content
                if result.extracted_content:
                    # Parse the JSON content
                    try:
                        extracted_data = json.loads(result.extracted_content)
                        enhanced_product = self._process_extracted_data(product, extracted_data, platform)
                        return enhanced_product
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse extracted content as JSON for {product['direct_buy_url']}")
                        return product
                else:
                    logger.warning(f"No content extracted from {product['direct_buy_url']}")
                    return product
                    
        except Exception as e:
            logger.error(f"Error extracting product from {product['direct_buy_url']}: {str(e)}")
            return product
            
    def _process_extracted_data(self, product: Dict[str, Any], 
                               extracted_data: Union[Dict[str, Any], List[Dict[str, Any]]], 
                               platform: str) -> Dict[str, Any]:
        """
        Process and merge extracted data with the product dict.
        
        Args:
            product: Original product dict
            extracted_data: Data extracted by Crawl4AI
            platform: Platform type (shopify, woocommerce, etc.)
            
        Returns:
            Enhanced product dict
        """
        # Handle case when extracted_data is a list
        if isinstance(extracted_data, list):
            if not extracted_data:
                return product
            extracted_data = extracted_data[0]
            
        # Start with original product
        enhanced = product.copy()
        
        # Extract and normalize basic fields
        if "name" in extracted_data and extracted_data["name"] and not enhanced.get("name"):
            enhanced["name"] = extracted_data["name"]
            enhanced["slug"] = slugify(extracted_data["name"])
            
        if "product_description" in extracted_data and extracted_data["product_description"]:
            enhanced["description"] = self._clean_html(extracted_data["product_description"])
            
        if "image_url" in extracted_data and extracted_data["image_url"] and not enhanced.get("image_url"):
            enhanced["image_url"] = extracted_data["image_url"]
            
        # Process prices
        self._process_prices(enhanced, extracted_data, platform)
        
        # Process roast level
        self._process_roast_level(enhanced, extracted_data)
        
        # Process processing method
        self._process_processing_method(enhanced, extracted_data)
        
        # Process bean type
        self._process_bean_type(enhanced, extracted_data)
        
        # Process origin/region
        self._process_origin(enhanced, extracted_data)
        
        # Process flavor profiles
        self._process_flavor_profiles(enhanced, extracted_data)
        
        # Process availability
        self._process_availability(enhanced, extracted_data)
        
        # Store raw extracted data for potential further processing
        enhanced["_extracted_data"] = extracted_data
        
        return enhanced
        
    def _process_prices(self, product: Dict[str, Any], extracted_data: Dict[str, Any], platform: str):
        """
        Process and normalize price information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
            platform: Platform type
        """
        # Handle different price formats across platforms
        if platform == "shopify":
            # Shopify typically has variants with prices
            if "variants" in extracted_data and isinstance(extracted_data["variants"], list):
                self._process_shopify_variants(product, extracted_data["variants"])
        elif platform == "woocommerce":
            # WooCommerce often has price and variations
            if "price" in extracted_data:
                self._process_woo_price(product, extracted_data)
        else:
            # Generic price processing
            if "price_text" in extracted_data:
                self._process_generic_price(product, extracted_data["price_text"])
                
    def _process_shopify_variants(self, product: Dict[str, Any], variants: List[Dict[str, Any]]):
        """
        Process Shopify variants to extract prices.
        
        Args:
            product: Product dict to update
            variants: List of variant dicts
        """
        weight_prices = []
        
        for variant in variants:
            if not isinstance(variant, dict):
                continue
                
            # Extract price
            price = None
            if "price" in variant:
                try:
                    price = float(variant["price"])
                except (ValueError, TypeError):
                    continue
                    
            # No price, skip
            if price is None:
                continue
                
            # Extract weight from title
            weight_grams = None
            title = variant.get("title", "").lower()
            
            # Try to extract weight in grams
            weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', title)
            if weight_match:
                weight_value = float(weight_match.group(1))
                weight_unit = weight_match.group(2).lower()
                
                # Convert to grams
                if 'kg' in weight_unit:
                    weight_grams = int(weight_value * 1000)
                else:
                    weight_grams = int(weight_value)
                    
            # If we extracted both weight and price, add to list
            if weight_grams and price:
                weight_prices.append((weight_grams, price))
                
        # Sort by weight and assign to standard categories
        if weight_prices:
            weight_prices.sort(key=lambda x: x[0])
            
            for weight_grams, price in weight_prices:
                if weight_grams <= 100:
                    product["price_100g"] = price
                elif weight_grams <= 250:
                    product["price_250g"] = price
                elif weight_grams <= 500:
                    product["price_500g"] = price
                else:
                    product["price_1kg"] = price
                    
        # If no weights extracted but we have variants with prices, use first price as default
        elif variants and "price" in variants[0]:
            try:
                product["price_250g"] = float(variants[0]["price"])
            except (ValueError, TypeError):
                pass
                
    def _process_woo_price(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process WooCommerce price information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Try regular price
        if "price" in extracted_data:
            try:
                price = float(extracted_data["price"])
                
                # Check product name/description for weight indicators
                found_weight = False
                text_to_check = f"{product.get('name', '')} {product.get('description', '')}"
                
                # Check for common weight patterns
                if re.search(r'\b100\s*g|\b100\s*gram', text_to_check, re.IGNORECASE):
                    product["price_100g"] = price
                    found_weight = True
                elif re.search(r'\b250\s*g|\b250\s*gram|\bquarter\s*kg', text_to_check, re.IGNORECASE):
                    product["price_250g"] = price
                    found_weight = True
                elif re.search(r'\b500\s*g|\b500\s*gram|\bhalf\s*kg', text_to_check, re.IGNORECASE):
                    product["price_500g"] = price
                    found_weight = True
                elif re.search(r'\b1\s*kg|\b1000\s*g|\b1000\s*gram', text_to_check, re.IGNORECASE):
                    product["price_1kg"] = price
                    found_weight = True
                    
                # If no specific weight found, default to 250g
                if not found_weight:
                    product["price_250g"] = price
                    
            except (ValueError, TypeError):
                pass
                
        # Process variations if available
        if "variations" in extracted_data and isinstance(extracted_data["variations"], list):
            weight_prices = []
            
            for variation in extracted_data["variations"]:
                if not isinstance(variation, dict):
                    continue
                    
                # Extract attributes looking for weight
                attributes = variation.get("attributes", [])
                weight_value = None
                weight_unit = "g"  # Default to grams
                
                for attr in attributes:
                    if not isinstance(attr, dict):
                        continue
                        
                    # Look for weight-related attributes
                    name = attr.get("name", "").lower()
                    value = attr.get("value", "").lower()
                    
                    if "weight" in name or "size" in name:
                        # Try to extract numeric weight
                        weight_match = re.search(r'(\d+\.?\d*)\s*(g|gram|gm|kg)', value)
                        if weight_match:
                            weight_value = float(weight_match.group(1))
                            weight_unit = weight_match.group(2).lower()
                            
                # Extract price
                if "price" in variation and weight_value:
                    try:
                        price = float(variation["price"])
                        
                        # Convert to grams
                        weight_grams = int(weight_value * 1000 if 'kg' in weight_unit else weight_value)
                        
                        weight_prices.append((weight_grams, price))
                        
                    except (ValueError, TypeError):
                        continue
                        
            # Sort by weight and assign to standard categories
            if weight_prices:
                weight_prices.sort(key=lambda x: x[0])
                
                for weight_grams, price in weight_prices:
                    if weight_grams <= 100:
                        product["price_100g"] = price
                    elif weight_grams <= 250:
                        product["price_250g"] = price
                    elif weight_grams <= 500:
                        product["price_500g"] = price
                    else:
                        product["price_1kg"] = price
                        
    def _process_generic_price(self, product: Dict[str, Any], price_text: str):
        """
        Process generic price text.
        
        Args:
            product: Product dict to update
            price_text: Price text to parse
        """
        if not price_text:
            return
            
        # Extract numeric price
        price_match = re.search(r'[\d,.]+', price_text)
        if price_match:
            try:
                # Remove thousands separators and convert to float
                price = float(price_match.group(0).replace(',', ''))
                
                # Check if there's weight information in the price text
                if '100g' in price_text or '100 g' in price_text or '100gm' in price_text:
                    product["price_100g"] = price
                elif '250g' in price_text or '250 g' in price_text or '250gm' in price_text:
                    product["price_250g"] = price
                elif '500g' in price_text or '500 g' in price_text or '500gm' in price_text:
                    product["price_500g"] = price
                elif '1kg' in price_text or '1 kg' in price_text or '1000g' in price_text:
                    product["price_1kg"] = price
                else:
                    # Default to 250g if no weight specified
                    product["price_250g"] = price
                    
            except (ValueError, TypeError):
                pass
                
    def _process_roast_level(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process and normalize roast level information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "roast_level" in product:
            return
            
        # Try specific roast level field
        if "roast_level" in extracted_data:
            roast_text = str(extracted_data["roast_level"]).lower()
            product["roast_level"] = self._normalize_roast_level(roast_text)
            return
            
        # Try roast info field
        if "roast_info" in extracted_data:
            roast_text = str(extracted_data["roast_info"]).lower()
            product["roast_level"] = self._normalize_roast_level(roast_text)
            return
            
        # If we have specifications, check there
        if "specifications" in extracted_data:
            specs_text = str(extracted_data["specifications"]).lower()
            
            # Look for roast level in specifications
            if "roast" in specs_text:
                product["roast_level"] = self._normalize_roast_level(specs_text)
                return
                
        # Check in description
        if "product_description" in extracted_data:
            desc_text = str(extracted_data["product_description"]).lower()
            
            # Look for specific roast mentions
            roast_level = None
            
            if "light roast" in desc_text:
                roast_level = "light"
            elif "medium-light roast" in desc_text or "medium light roast" in desc_text:
                roast_level = "medium-light"
            elif "medium roast" in desc_text:
                roast_level = "medium"
            elif "medium-dark roast" in desc_text or "medium dark roast" in desc_text:
                roast_level = "medium-dark"
            elif "dark roast" in desc_text:
                roast_level = "dark"
                
            if roast_level:
                product["roast_level"] = roast_level
                
    def _normalize_roast_level(self, text: str) -> str:
        """
        Normalize roast level from text.
        
        Args:
            text: Text containing roast level information
            
        Returns:
            Normalized roast level
        """
        text = text.lower()
        
        if "medium" in text and "light" in text:
            return RoastLevel.MEDIUM_LIGHT
        elif "medium" in text and "dark" in text:
            return RoastLevel.MEDIUM_DARK
        elif "medium" in text:
            return RoastLevel.MEDIUM
        elif "light" in text:
            return RoastLevel.LIGHT
        elif "dark" in text:
            return RoastLevel.DARK
        else:
            return RoastLevel.UNKNOWN
            
    def _process_processing_method(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process and normalize processing method information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "processing_method" in product:
            return
            
        # Try specific process info field
        if "process_info" in extracted_data:
            process_text = str(extracted_data["process_info"]).lower()
            product["processing_method"] = self._normalize_processing_method(process_text)
            return
            
        # If we have specifications, check there
        if "specifications" in extracted_data:
            specs_text = str(extracted_data["specifications"]).lower()
            
            # Look for processing method in specifications
            if "process" in specs_text:
                product["processing_method"] = self._normalize_processing_method(specs_text)
                return
                
        # Check in description
        if "product_description" in extracted_data:
            desc_text = str(extracted_data["product_description"]).lower()
            product["processing_method"] = self._normalize_processing_method(desc_text)
            
    def _normalize_processing_method(self, text: str) -> str:
        """
        Normalize processing method from text.
        
        Args:
            text: Text containing processing method information
            
        Returns:
            Normalized processing method
        """
        text = text.lower()
        
        if "washed" in text or "wet process" in text:
            return ProcessingMethod.WASHED
        elif "natural" in text or "dry process" in text:
            return ProcessingMethod.NATURAL
        elif "honey" in text:
            return ProcessingMethod.HONEY
        elif "pulped natural" in text:
            return ProcessingMethod.PULPED_NATURAL
        elif "anaerobic" in text:
            return ProcessingMethod.ANAEROBIC
        else:
            return ProcessingMethod.UNKNOWN
            
    def _process_bean_type(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process and normalize bean type information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "bean_type" in product:
            return
            
        # Try to extract from various fields
        text_to_check = ""
        
        # Check specifications
        if "specifications" in extracted_data:
            text_to_check += str(extracted_data["specifications"]).lower() + " "
            
        # Check description
        if "product_description" in extracted_data:
            text_to_check += str(extracted_data["product_description"]).lower() + " "
            
        # Check product name
        if "name" in extracted_data:
            text_to_check += str(extracted_data["name"]).lower() + " "
            
        # Determine bean type
        if "arabica" in text_to_check and "robusta" in text_to_check:
            product["bean_type"] = BeanType.BLEND
        elif "arabica" in text_to_check:
            product["bean_type"] = BeanType.ARABICA
        elif "robusta" in text_to_check:
            product["bean_type"] = BeanType.ROBUSTA
        elif "blend" in text_to_check:
            product["bean_type"] = BeanType.BLEND
        else:
            product["bean_type"] = BeanType.UNKNOWN
            
    def _process_origin(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process and extract origin/region information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "region_name" in product:
            return
            
        # Try specific origin info field
        if "origin_info" in extracted_data:
            product["region_name"] = extracted_data["origin_info"]
            return
            
        # If we have specifications, check there
        if "specifications" in extracted_data:
            specs_text = str(extracted_data["specifications"]).lower()
            
            # Look for common origin indicators
            origin_patterns = [
                r'origin:\s*([^,<.]+)',
                r'region:\s*([^,<.]+)',
                r'from\s+([^,<.]+)',
                r'grown in\s+([^,<.]+)'
            ]
            
            for pattern in origin_patterns:
                match = re.search(pattern, specs_text)
                if match:
                    product["region_name"] = match.group(1).strip().title()
                    return
                    
        # Check in description
        if "product_description" in extracted_data:
            desc_text = str(extracted_data["product_description"]).lower()
            
            # Look for common origin indicators
            origin_patterns = [
                r'from\s+([^,<.]+)',
                r'grown in\s+([^,<.]+)',
                r'sourced from\s+([^,<.]+)',
                r'origin:\s*([^,<.]+)',
                r'region:\s*([^,<.]+)'
            ]
            
            for pattern in origin_patterns:
                match = re.search(pattern, desc_text)
                if match:
                    product["region_name"] = match.group(1).strip().title()
                    return
                    
    def _process_flavor_profiles(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process and extract flavor profile information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "flavor_profiles" in product:
            return
            
        # Common flavor profiles
        common_flavors = [
            "chocolate", "cocoa", "nutty", "nuts", "caramel", "fruity", 
            "citrus", "berry", "floral", "spice", "earthy", "woody",
            "honey", "sweet", "vanilla", "tropical", "smoky", "tobacco"
        ]
        
        flavor_profiles = []
        
        # Check in description
        if "product_description" in extracted_data:
            desc_text = str(extracted_data["product_description"]).lower()
            
            # Look for flavor profile section
            flavor_section_match = re.search(r'flavou?r\s+profiles?:?\s*([^<]+)', desc_text)
            if flavor_section_match:
                flavor_text = flavor_section_match.group(1).lower()
                
                # Check for common flavors
                for flavor in common_flavors:
                    if flavor in flavor_text:
                        # Normalize flavor name
                        if flavor == "cocoa":
                            normalized = "chocolate"
                        elif flavor == "nuts":
                            normalized = "nutty"
                        else:
                            normalized = flavor
                            
                        if normalized not in flavor_profiles:
                            flavor_profiles.append(normalized)
            else:
                # If no specific section, check whole description
                for flavor in common_flavors:
                    if flavor in desc_text:
                        # Normalize flavor name
                        if flavor == "cocoa":
                            normalized = "chocolate"
                        elif flavor == "nuts":
                            normalized = "nutty"
                        else:
                            normalized = flavor
                            
                        if normalized not in flavor_profiles:
                            flavor_profiles.append(normalized)
                            
        # If we found flavors, update product
        if flavor_profiles:
            product["flavor_profiles"] = flavor_profiles
            
    def _process_availability(self, product: Dict[str, Any], extracted_data: Dict[str, Any]):
        """
        Process availability information.
        
        Args:
            product: Product dict to update
            extracted_data: Extracted data
        """
        # Skip if already set
        if "is_available" in product:
            return
            
        # Check stock status field
        if "stock_status" in extracted_data:
            status = str(extracted_data["stock_status"]).lower()
            product["is_available"] = "out of stock" not in status and "sold out" not in status
            
    def _clean_html(self, html: str) -> str:
        """
        Clean HTML text.
        
        Args:
            html: HTML text
            
        Returns:
            Cleaned text
        """
        if not html:
            return ""
            
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)
        
        # Fix whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def _create_shopify_schema(self) -> Dict[str, Any]:
        """
        Create extraction schema for Shopify stores.
        
        Returns:
            Schema dict
        """
        return {
            "name": "Shopify Product",
            "baseSelector": "body",
            "fields": [
                # Basic info
                {"name": "name", "selector": "h1.product-title, .product__title h1, .product-single__title", "type": "text"},
                {"name": "product_description", "selector": ".product-description, .product__description, .product-single__description", "type": "html"},
                
                # Images
                {"name": "image_url", "selector": ".product__media img, .product-featured-img", "type": "attribute", "attribute": "src"},
                
                # Price
                {"name": "price_text", "selector": ".price .money, .product__price .money, span.price", "type": "text"},
                
                # Availability
                {"name": "stock_status", "selector": ".product-form__inventory, .product-inventory, [data-store-availability-container]", "type": "text"},
                
                # Product attributes
                {"name": "roast_level", "selector": "[data-option-index='0'] .selected-text, .single-option-selector[data-option='roast'], .product-form__input:contains('Roast') .selected-value", "type": "text"},
                {"name": "process_info", "selector": "[data-option-index='1'] .selected-text, .single-option-selector[data-option='process'], .product-form__input:contains('Process') .selected-value", "type": "text"},
                {"name": "origin_info", "selector": ".product-single__meta-list:contains('Origin'), .product__info-container:contains('Origin'), .product-info__item:contains('Origin')", "type": "text"},
                
                # Extract variants as JSON
                {"name": "variants", "selector": "script[type='application/json']:contains('variants')", "type": "text"},
                
                # Additional specifications
                {"name": "specifications", "selector": "#product-specifications, .product-specifications, .product-description table", "type": "html"}
            ]
        }
        
    def _create_woocommerce_schema(self) -> Dict[str, Any]:
        """
        Create extraction schema for WooCommerce stores.
        
        Returns:
            Schema dict
        """
        return {
            "name": "WooCommerce Product",
            "baseSelector": "body",
            "fields": [
                # Basic info
                {"name": "name", "selector": ".product_title, .entry-title", "type": "text"},
                {"name": "product_description", "selector": ".woocommerce-product-details__short-description, #tab-description, .product-description", "type": "html"},
                
                # Images
                {"name": "image_url", "selector": ".woocommerce-product-gallery__image img", "type": "attribute", "attribute": "src"},
                
                # Price
                {"name": "price_text", "selector": ".price, .woocommerce-Price-amount", "type": "text"},
                
                # Availability
                {"name": "stock_status", "selector": ".stock, .availability", "type": "text"},
                
                # Product attributes from the attributes table
                {"name": "attributes_table", "selector": ".woocommerce-product-attributes, .shop_attributes", "type": "html"},
                
                # Try to find specific attributes
                {"name": "roast_level", "selector": ".woocommerce-product-attributes-item--attribute_roast-level .woocommerce-product-attributes-item__value, tr:contains('Roast') td", "type": "text"},
                {"name": "process_info", "selector": ".woocommerce-product-attributes-item--attribute_processing-method .woocommerce-product-attributes-item__value, tr:contains('Process') td", "type": "text"},
                {"name": "origin_info", "selector": ".woocommerce-product-attributes-item--attribute_origin .woocommerce-product-attributes-item__value, tr:contains('Origin') td", "type": "text"},
                
                # Additional specifications
                {"name": "specifications", "selector": ".product-info__description, .product-description, .woocommerce-Tabs-panel", "type": "html"}
            ]
        }
        
    def _create_generic_schema(self) -> Dict[str, Any]:
        """
        Create generic extraction schema that works across different platforms.
        
        Returns:
            Schema dict
        """
        return {
            "name": "Coffee Product",
            "baseSelector": "body",
            "fields": [
                # Basic info - try multiple common selectors
                {"name": "name", "selector": "h1, .product-title, .product-name, .product_title, .entry-title", "type": "text"},
                {"name": "product_description", "selector": ".product-description, .description, .product-details, .product-info, article p", "type": "html"},
                
                # Images - try multiple approaches
                {"name": "image_url", "selector": ".product-image img, .product-gallery img, .product_image img, .woocommerce-product-gallery__image img", "type": "attribute", "attribute": "src"},
                
                # Price
                {"name": "price_text", "selector": ".price, .product-price, span.amount, .price-container", "type": "text"},
                
                # Availability
                {"name": "stock_status", "selector": ".stock-status, .availability, .product-availability, .in-stock, .out-of-stock", "type": "text"},
                
                # Try to find specific product attributes in various formats
                {"name": "roast_info", "selector": ".roast-level, .product-roast, [data-option='roast'], div:contains('Roast Level'), span:contains('Roast:')", "type": "text"},
                {"name": "process_info", "selector": ".processing-method, .product-process, [data-option='process'], div:contains('Processing Method'), span:contains('Process:')", "type": "text"},
                {"name": "origin_info", "selector": ".origin, .product-origin, div:contains('Origin'), span:contains('Origin:')", "type": "text"},
                
                # Additional specifications from various containers
                {"name": "specifications", "selector": ".product-specs, .specifications, .product-attributes, .product-details table, .details", "type": "html"}
            ]
        }