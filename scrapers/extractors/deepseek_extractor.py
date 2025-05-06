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
            \nOriginal Description: {description}
            \nProduct Page Content:\n{markdown[:6000]}  # Trim to avoid token limits
            """

            # Prepare prompt (CoffeeModel fields only)
            prompt = (
                """
                Based on the coffee product information provided, extract the following attributes as JSON. Only include fields for which you find clear evidence. If a field is not clearly stated, use null or empty value as appropriate. Do not guess or infer beyond the text.

                1. roast_level: (one of: light, medium-light, medium, medium-dark, dark, or unknown)
                2. bean_type: (one of: arabica, robusta, blend, liberica, or unknown)
                3. processing_method: (one of: washed, natural, honey, anaerobic, pulped-natural, or unknown)
                4. region_name: (string, or null)
                5. flavor_profiles: (array of common flavor categories, e.g. chocolate, fruity, nutty, caramel, berry, citrus, floral, spice, or empty array)
                6. brew_methods: (array of brewing methods, e.g. espresso, filter, pour-over, french-press, aeropress, moka-pot, cold-brew, or empty array)
                7. prices: (dictionary mapping weight in grams to price, e.g. {\"250\": 450, \"500\": 800}, or empty if not found)
                8. image_url: (URL of main product image, or null)
                9. direct_buy_url: (URL to buy the product, or null)
                10. is_seasonal: (boolean, true if described as seasonal or limited release, else false)
                11. is_featured: (boolean, true if described as featured, bestseller, or recommended, else false)
                12. is_single_origin: (boolean, true if described as single origin, else false)
                13. is_available: (boolean, true if the product is in stock or available, else false)
                14. tags: (array of tags or keywords found, or empty array)
                15. external_links: (array of any external URLs found, or empty array)

                Return ONLY a valid JSON object with these fields and nothing else.
                """
            )

            # Call DeepSeek API
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a coffee expert who extracts structured attributes from product descriptions."},
                    {"role": "user", "content": context + "\n\n" + prompt}
                ],
                max_tokens=900,
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
                    logger.warning(f"No JSON found in DeepSeek response: {ai_response[:300]}...")
                    return None

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse DeepSeek response as JSON: {str(e)}")
                logger.debug(f"Response content: {ai_response[:500]}...")
                return None

        except Exception as e:
            logger.error(f"Error extracting attributes with DeepSeek: {str(e)}")
            logger.debug(f"Prompt: {prompt[:500]}\nMarkdown: {markdown[:500]}...")
            return None

    def _clean_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize extracted attributes to match CoffeeModel.
        """
        cleaned = {}
        # Normalize roast level
        if "roast_level" in attributes:
            roast = str(attributes["roast_level"]).lower()
            if roast in [v.value for v in RoastLevel]:
                cleaned["roast_level"] = roast
        # Normalize bean type
        if "bean_type" in attributes:
            bean = str(attributes["bean_type"]).lower()
            if bean in [v.value for v in BeanType]:
                cleaned["bean_type"] = bean
        # Normalize processing method
        if "processing_method" in attributes:
            process = str(attributes["processing_method"]).lower()
            if process == "pulped natural":
                process = "pulped-natural"
            if process in [v.value for v in ProcessingMethod]:
                cleaned["processing_method"] = process
        # Handle region name
        if "region_name" in attributes and attributes["region_name"]:
            cleaned["region_name"] = attributes["region_name"]
        # Flavor profiles
        if "flavor_profiles" in attributes and isinstance(attributes["flavor_profiles"], list):
            cleaned["flavor_profiles"] = attributes["flavor_profiles"]
        # Brew methods
        if "brew_methods" in attributes and isinstance(attributes["brew_methods"], list):
            cleaned["brew_methods"] = attributes["brew_methods"]
        # Prices
        if "prices" in attributes and isinstance(attributes["prices"], dict):
            cleaned["prices"] = attributes["prices"]
        # Image URL
        if "image_url" in attributes and attributes["image_url"]:
            cleaned["image_url"] = attributes["image_url"]
        # Direct buy URL
        if "direct_buy_url" in attributes and attributes["direct_buy_url"]:
            cleaned["direct_buy_url"] = attributes["direct_buy_url"]
        # is_seasonal, is_featured, is_single_origin, is_available
        for bool_field in ["is_seasonal", "is_featured", "is_single_origin", "is_available"]:
            if bool_field in attributes:
                cleaned[bool_field] = bool(attributes[bool_field])
        # Tags
        if "tags" in attributes and isinstance(attributes["tags"], list):
            cleaned["tags"] = attributes["tags"]
        # External links
        if "external_links" in attributes and isinstance(attributes["external_links"], list):
            cleaned["external_links"] = attributes["external_links"]
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