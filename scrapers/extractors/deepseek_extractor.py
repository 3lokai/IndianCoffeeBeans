# scrapers/extractors/deepseek_extractor.py
import logging
import json
from typing import Dict, Any, Optional, List

from openai import OpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

from common.models import RoastLevel, BeanType, ProcessingMethod
from config import DEEPSEEK_API_KEY

logger = logging.getLogger(__name__)

class DeepseekExtractor:
    """
    Extracts product details using DeepSeek API as a fallback when structured extraction fails.
    
    This extractor:
    1. Uses Crawl4AI to convert the product page to clean markdown
    2. Sends the markdown to DeepSeek API for attribute extraction
    3. Normalizes and merges the extracted attributes with the product data
    """
    
    def __init__(self, browser_config: Optional[BrowserConfig] = None):
        """
        Initialize the extractor.
        
        Args:
            browser_config: Optional browser configuration for Crawl4AI
        """
        if not DEEPSEEK_API_KEY:
            logger.warning("DeepSeek API key not configured. DeepseekExtractor will not function.")
            
        self.api_key = DEEPSEEK_API_KEY
        self.browser_config = browser_config or BrowserConfig(
            headless=True,
            ignore_https_errors=True,
            viewport_width=1280,
            viewport_height=800
        )
        
    async def extract(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract product details using DeepSeek.
        
        Args:
            product: Product dict with at least a direct_buy_url
            
        Returns:
            Enhanced product dict with extracted attributes
        """
        if not self.api_key:
            logger.warning("DeepSeek API key not configured. Skipping extraction.")
            return product
            
        if not product.get("direct_buy_url"):
            logger.error("Product missing direct_buy_url")
            return product
            
        # Check if we need enhancement
        if not self._needs_enhancement(product):
            logger.info(f"Product {product.get('name')} doesn't need enhancement")
            return product
            
        try:
            # Step 1: Convert product page to markdown
            markdown = await self._get_page_markdown(product["direct_buy_url"])
            
            # If markdown extraction failed, try to use product description if available
            if not markdown:
                logger.warning(f"Failed to get markdown for {product['direct_buy_url']}")
                if product.get("description") and len(product.get("description", "")) > 50:
                    logger.info(f"Using product description as fallback for DeepSeek extraction")
                    markdown = product["description"]
                else:
                    logger.error(f"No suitable fallback content found for {product['direct_buy_url']}")
                    return product
                
            # Step 2: Extract attributes using DeepSeek
            extracted_attributes = await self._extract_attributes_with_deepseek(
                product.get("name", "Unknown Coffee"), 
                product.get("description", ""), 
                markdown
            )
            
            if not extracted_attributes:
                logger.warning(f"Failed to extract attributes for {product['direct_buy_url']}")
                return product
                
            # Step 3: Merge extracted attributes with product
            enhanced_product = self._merge_attributes(product, extracted_attributes)
            
            # Mark as enriched with DeepSeek
            enhanced_product["deepseek_enriched"] = True
            enhanced_product["extracted_by"] = "deepseek"

            return enhanced_product
            
        except Exception as e:
            logger.error(f"Error extracting product from {product['direct_buy_url']}: {str(e)}")
            return product
            
    def _needs_enhancement(self, product: Dict[str, Any]) -> bool:
        """
        Determine if a product needs attribute enhancement.
        
        Args:
            product: Product dict
            
        Returns:
            True if enhancement is needed, False otherwise
        """
        # Count missing critical attributes
        missing_attributes = 0
        
        critical_attributes = [
            "roast_level", 
            "bean_type", 
            "processing_method", 
            "flavor_profiles"
        ]
        
        for attr in critical_attributes:
            if attr not in product or not product[attr] or product[attr] == "unknown":
                missing_attributes += 1
                
        # Need enhancement if at least 2 critical attributes are missing
        return missing_attributes >= 2
            
    async def _get_page_markdown(self, url: str) -> Optional[str]:
        """
        Convert a product page to markdown using Crawl4AI.
        
        Args:
            url: URL of the product page
            
        Returns:
            Markdown text if successful, None otherwise
        """
        try:
            # Create crawler instance
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                # Configure markdown generation
                markdown_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    markdown_generator=DefaultMarkdownGenerator(
                        content_source="raw_html",  # Try raw_html instead of default
                        content_filter=PruningContentFilter(threshold=0.3, threshold_type="fixed"),  # Lower threshold
                        options={"body_width": 0, "ignore_links": False, "ignore_images": False}  # Keep all content
                    ),
                    wait_until="networkidle",  # Wait for network idle instead of domcontentloaded
                    page_timeout=45000  # Increase timeout to 45 seconds
                )
                
                # Run crawl
                logger.info(f"Getting markdown for {url}")
                result = await crawler.arun(url=url, config=markdown_config)
                
                if not result.success:
                    logger.warning(f"Failed to get markdown for {url}: Crawl unsuccessful")
                    return None
                    
                # Extract markdown
                markdown_content = None
                
                # Try different attributes based on Crawl4AI version
                if hasattr(result.markdown, 'fit_markdown'):
                    markdown_content = result.markdown.fit_markdown
                    logger.debug(f"Using fit_markdown for {url}")
                elif hasattr(result.markdown, 'raw_markdown'):
                    markdown_content = result.markdown.raw_markdown
                    logger.debug(f"Using raw_markdown for {url}")
                else:
                    markdown_content = str(result.markdown)
                    logger.debug(f"Using string representation of markdown for {url}")
                
                # Check for empty markdown
                if not markdown_content or len(markdown_content.strip()) < 50:
                    logger.warning(f"Empty or very short markdown retrieved for {url}")
                    
                    # Try to use raw HTML as fallback
                    if result.html and len(str(result.html)) > 100:
                        logger.info(f"Using HTML content as fallback for {url}")
                        return str(result.html)[:6000]  # Limit length
                        
                    return None
                
                # Success!
                logger.debug(f"Successfully extracted markdown from {url} (length: {len(markdown_content)})")
                return markdown_content
                
        except Exception as e:
            logger.error(f"Error getting markdown for {url}: {str(e)}")
            return None
            
    async def _extract_attributes_with_deepseek(self, name: str, description: str, markdown: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured attributes from markdown using DeepSeek API.
        
        Args:
            name: Product name
            description: Product description
            markdown: Markdown content from product page
            
        Returns:
            Dict of extracted attributes if successful, None otherwise
        """
        try:
            if not markdown and description:
                markdown = description
                logger.info(f"Using product description as fallback for DeepSeek extraction")
            # Initialize DeepSeek client
            client = OpenAI(
                api_key=self.api_key,
                base_url="https://api.deepseek.com"
            )
            
            # Prepare context
            context = f"""
            Product Name: {name}
            
            Original Description: {description}
            
            Product Page Content:
            {markdown[:6000]}  # Trim to avoid token limits
            """
            
            # Prepare prompt
            prompt = f"""
            Based on the coffee product information provided, extract the following attributes:
            
            1. roast_level: (exactly one of: light, medium-light, medium, medium-dark, dark, or unknown if unclear)
            2. bean_type: (exactly one of: arabica, robusta, blend, or unknown if unclear)
            3. processing_method: (one of: washed, natural, honey, anaerobic, pulped-natural, or unknown if unclear)
            4. region_name: (geographic origin of the coffee beans, or null if not specified)
            5. tasting_notes: (comma-separated flavor notes found in the description, or null if not specified)
            6. flavor_profiles: (array of common flavor categories like: chocolate, fruity, nutty, caramel, berry, citrus, floral, spice, or empty array if not specified)
            7. brew_methods: (array of recommended brewing methods like: espresso, filter, pour-over, french-press, aeropress, moka-pot, cold-brew, or empty array if not specified)
            8. altitude_min: (lowest altitude in meters, integer only or null if not specified)
            9. altitude_max: (highest altitude in meters, integer only or null if not specified)
            10. varietal: (coffee varietal/cultivar like SL9, Catuai, Kent, etc., or null if not specified)
            11. is_blend: (boolean true if it's a blend of different beans, false otherwise)
            12. is_seasonal: (boolean true if it's described as a seasonal or limited release, false otherwise)
            
            DO NOT infer or guess any values for which there's no clear evidence in the text.
            If a field is not clearly stated in the text, return unknown or null for that field.
            Return ONLY a valid JSON object with these fields and nothing else.
            """
            
            # Call DeepSeek API
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a coffee expert who extracts structured attributes from product descriptions."},
                    {"role": "user", "content": context + "\n\n" + prompt}
                ],
                max_tokens=800,
                temperature=0.1
            )
            
            # Extract response content
            ai_response = response.choices[0].message.content
            
            # Parse JSON from response
            try:
                # Find JSON in the response
                json_start = ai_response.find('{')
                json_end = ai_response.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = ai_response[json_start:json_end]
                    attributes = json.loads(json_str)
                    
                    # Clean up attributes
                    cleaned_attributes = self._clean_attributes(attributes)
                    
                    return cleaned_attributes
                else:
                    logger.warning(f"No JSON found in DeepSeek response: {ai_response[:100]}...")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse DeepSeek response as JSON: {str(e)}")
                logger.debug(f"Response content: {ai_response[:100]}...")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting attributes with DeepSeek: {str(e)}")
            return None
            
    def _clean_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize extracted attributes.
        
        Args:
            attributes: Raw attributes from DeepSeek
            
        Returns:
            Cleaned attributes dict
        """
        cleaned = {}
        
        # Normalize roast level
        if "roast_level" in attributes:
            roast = str(attributes["roast_level"]).lower()
            if roast in RoastLevel.__members__:
                cleaned["roast_level"] = roast
                
        # Normalize bean type
        if "bean_type" in attributes:
            bean = str(attributes["bean_type"]).lower()
            if bean in BeanType.__members__:
                cleaned["bean_type"] = bean
                
        # Normalize processing method
        if "processing_method" in attributes:
            process = str(attributes["processing_method"]).lower()
            # Handle special cases
            if process == "pulped natural":
                process = "pulped-natural"
                
            if process in ProcessingMethod.__members__:
                cleaned["processing_method"] = process
                
        # Handle region name
        if "region_name" in attributes and attributes["region_name"]:
            cleaned["region_name"] = attributes["region_name"]
            
        # Handle flavor notes
        if "tasting_notes" in attributes and attributes["tasting_notes"]:
            cleaned["tasting_notes"] = attributes["tasting_notes"]
            
        # Handle arrays
        for array_field in ["flavor_profiles", "brew_methods"]:
            if array_field in attributes and isinstance(attributes[array_field], list):
                cleaned[array_field] = attributes[array_field]
                
        # Handle altitude
        for altitude_field in ["altitude_min", "altitude_max"]:
            if altitude_field in attributes and attributes[altitude_field] is not None:
                try:
                    cleaned[altitude_field] = int(attributes[altitude_field])
                except (ValueError, TypeError):
                    pass
                    
        # Handle varietal
        if "varietal" in attributes and attributes["varietal"]:
            cleaned["varietal"] = attributes["varietal"]
            
        # Handle booleans
        for bool_field in ["is_blend", "is_seasonal"]:
            if bool_field in attributes:
                cleaned[bool_field] = bool(attributes[bool_field])
                
        return cleaned
        
    def _merge_attributes(self, product: Dict[str, Any], attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge extracted attributes with existing product data.
        
        Args:
            product: Original product dict
            attributes: Extracted attributes
            
        Returns:
            Enhanced product dict
        """
        # Start with a copy of the original product
        enhanced = product.copy()
        
        # Merge attributes, only adding if not already present
        for key, value in attributes.items():
            # Skip null/None values
            if value is None:
                continue
                
            # Skip empty arrays
            if isinstance(value, list) and not value:
                continue
                
            # Skip unknown values
            if value == "unknown":
                continue
                
             # Add value if not already present OR if existing value is "unknown"
            if key not in enhanced or not enhanced[key] or (
                key in ["roast_level", "bean_type", "processing_method"] and 
                enhanced[key] == "unknown"
            ):
                enhanced[key] = value
                
        return enhanced