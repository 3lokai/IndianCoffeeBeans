# enhanced_coffee_enricher.py
# A more robust coffee data enricher using Crawl4AI

import asyncio
import json
from typing import List, Dict, Any, Optional
import pandas as pd
from pathlib import Path

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

from openai import OpenAI
from config import DEEPSEEK_API_KEY, ENRICHED_OUTPUT_CSV, OUTPUT_CSV

async def load_products_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Load products from CSV file."""
    df = pd.read_csv(csv_path)
    products = df.to_dict('records')
    print(f"Loaded {len(products)} products from CSV")
    return products

async def save_enriched_products(products: List[Dict[str, Any]], output_path: str):
    """Save enriched products to CSV file."""
    df = pd.DataFrame(products)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(products)} enriched products to {output_path}")

def create_coffee_extraction_schema():
    """Create a schema for structured extraction of coffee product data."""
    return {
        "name": "Coffee Product",
        "baseSelector": "body",  # Start with the whole page
        "fields": [
            # Basic product info
            {"name": "product_name", "selector": "h1", "type": "text"},
            
            # Price information - try multiple selectors for different site structures
            {"name": "price_text", "selector": ".price, .product-price, .woocommerce-Price-amount", "type": "text"},
            
            # Description - try multiple possible containers
            {"name": "product_description", "selector": ".product-description, .woocommerce-product-details__short-description, .description, #tab-description", "type": "html"},
            
            # Specifications - often in tables or lists
            {"name": "specifications", "selector": ".product-specs, .specifications, .product-attributes, table.shop_attributes", "type": "html"},
            
            # Roast level - might be in multiple places
            {"name": "roast_info", "selector": ".roast-level, .roast, [data-attribute='roast-level']", "type": "text"},
            
            # Origin information
            {"name": "origin_info", "selector": ".origin, .coffee-origin, .product-origin", "type": "text"},
            
            # Process method
            {"name": "process_info", "selector": ".process, .processing-method, .coffee-process", "type": "text"},
            
            # Additional attributes that might be present
            {"name": "attributes", "selector": ".product-attributes, .coffee-attributes", "type": "html"}
        ]
    }

async def enrich_products_with_crawl4ai(products: List[Dict[str, Any]], batch_size: int = 5) -> List[Dict[str, Any]]:
    """Process products in batches using Crawl4AI for extraction."""
    results = []
    
    # Create a browser config
    browser_config = BrowserConfig(
        headless=True,
        ignore_https_errors=True,
        viewport_width=1280,
        viewport_height=800
    )
    
    # Create a single crawler instance for the entire process
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Process in batches
        for i in range(0, len(products), batch_size):
            batch = products[i:i+batch_size]
            urls = [p.get('direct_buy_url') for p in batch if p.get('direct_buy_url')]
            
            if not urls:
                continue
                
            print(f"Processing batch {i//batch_size + 1}/{(len(products) + batch_size - 1)//batch_size} ({len(urls)} URLs)")
            
            # Process the batch
            url_to_results = {}
            
            # Process each URL individually
            for url in urls:
                # 1. First config: Extract structured data using CSS selectors
                css_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    extraction_strategy=JsonCssExtractionStrategy(create_coffee_extraction_schema()),
                    wait_until="domcontentloaded"
                )
                
                # 2. Second config: Generate clean markdown with content filtering
                markdown_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    markdown_generator=DefaultMarkdownGenerator(
                        content_filter=PruningContentFilter(threshold=0.5),
                        options={"body_width": 0}  # No wrapping
                    ),
                    wait_until="domcontentloaded"
                )
                
                try:
                    # Process with CSS extraction
                    css_result = await crawler.arun(url=url, config=css_config)
                    
                    # Process with markdown generation
                    markdown_result = await crawler.arun(url=url, config=markdown_config)
                    
                    # Store results
                    if url not in url_to_results:
                        url_to_results[url] = {'url': url}
                    
                    if css_result.success and css_result.extracted_content:
                        url_to_results[url]['structured_data'] = css_result.extracted_content
                        
                    if markdown_result.success and markdown_result.markdown:
                        md_content = None
                        if hasattr(markdown_result.markdown, 'fit_markdown'):
                            md_content = markdown_result.markdown.fit_markdown
                        if not md_content and hasattr(markdown_result.markdown, 'raw_markdown'):
                            md_content = markdown_result.markdown.raw_markdown
                        url_to_results[url]['markdown'] = md_content
                        
                except Exception as e:
                    print(f"Error processing {url}: {e}")
            
            # Now enrich products with the crawled data
            batch_results = []
            for product in batch:
                url = product.get('direct_buy_url')
                if url and url in url_to_results:
                    # Combine CSS extraction and markdown into a richer product
                    enriched_product = await enhance_product(
                        product, 
                        url_to_results[url].get('structured_data'),
                        url_to_results[url].get('markdown')
                    )
                    batch_results.append(enriched_product)
                else:
                    # Keep original product if we couldn't crawl it
                    batch_results.append(product)
            
            results.extend(batch_results)
            
            # Be nice to servers
            await asyncio.sleep(1)
    
    return results

async def enhance_product(product: Dict[str, Any], 
                          structured_data: Optional[str], 
                          markdown: Optional[str]) -> Dict[str, Any]:
    """Enhance a product with crawled data and LLM extraction."""
    try:
        # Start with the original product
        enriched = product.copy()
        
        # Parse structured data if available
        extracted_fields = {}
        if structured_data:
            try:
                data = json.loads(structured_data)
                
                # Handle data as list - typically the first item is what we want
                if isinstance(data, list):
                    if data:  # Check if the list is not empty
                        data = data[0]  # Take the first item
                    else:
                        data = {}  # Empty dict if list is empty
                
                # Now data should be a dictionary, so we can access with .get()
                # Map extracted fields to our schema
                if isinstance(data, dict):  # Double-check it's a dict
                    if data.get('product_name'):
                        extracted_fields['name'] = data.get('product_name')
                    
                    if data.get('price_text'):
                        # Try to parse price from text
                        import re
                        price_match = re.search(r'[\d,.]+', data.get('price_text', ''))
                        if price_match:
                            try:
                                price = float(price_match.group(0).replace(',', ''))
                                if 'price_250g' not in enriched:  # Only set if not already present
                                    enriched['price_250g'] = price
                            except (ValueError, TypeError):
                                pass
                    
                    if data.get('product_description'):
                        extracted_fields['description'] = clean_html(data.get('product_description'))
                    
                    # Extract more specific attributes
                    if data.get('roast_info'):
                        roast_text = data.get('roast_info', '').lower()
                        if 'light' in roast_text and 'medium' in roast_text:
                            extracted_fields['roast_level'] = 'medium-light'
                        elif 'medium' in roast_text and 'dark' in roast_text:
                            extracted_fields['roast_level'] = 'medium-dark'
                        elif 'medium' in roast_text:
                            extracted_fields['roast_level'] = 'medium'
                        elif 'light' in roast_text:
                            extracted_fields['roast_level'] = 'light'
                        elif 'dark' in roast_text:
                            extracted_fields['roast_level'] = 'dark'
                    
                    if data.get('process_info'):
                        process_text = data.get('process_info', '').lower()
                        if 'washed' in process_text:
                            extracted_fields['processing_method'] = 'washed'
                        elif 'natural' in process_text:
                            extracted_fields['processing_method'] = 'natural'
                        elif 'honey' in process_text:
                            extracted_fields['processing_method'] = 'honey'
                        elif 'anaerobic' in process_text:
                            extracted_fields['processing_method'] = 'anaerobic'
                    
                    if data.get('origin_info'):
                        extracted_fields['region'] = data.get('origin_info')
            except Exception as e:
                print(f"Error parsing structured data: {e}")
        
        # Update enriched product with extracted fields
        for key, value in extracted_fields.items():
            if value and (key not in enriched or not enriched[key]):
                enriched[key] = value
        
        # Use DeepSeek for any remaining fields
        if needs_enhancement(enriched) and markdown:
            enriched = await extract_attributes_with_deepseek(enriched, markdown)
        
        return enriched
    except Exception as e:
        print(f"Error enhancing product {product.get('name')}: {e}")
        return product

def clean_html(html_text):
    """Remove HTML tags from text."""
    if not html_text:
        return ""
    import re
    text = re.sub(r'<[^>]+>', ' ', html_text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def needs_enhancement(coffee):
    """Check if coffee object needs LLM enhancement."""
    missing_fields = []
    
    if "roast_level" not in coffee:
        missing_fields.append("roast_level")
        
    if "bean_type" not in coffee:
        missing_fields.append("bean_type")
        
    if "processing_method" not in coffee and not coffee.get("is_blend", False):
        missing_fields.append("processing_method")
        
    if "flavor_profiles" not in coffee:
        missing_fields.append("flavor_profiles")
        
    # Only enhance if multiple fields are missing
    return len(missing_fields) >= 2

async def extract_attributes_with_deepseek(product: Dict[str, Any], markdown: str) -> Dict[str, Any]:
    """Extract structured attributes from markdown using DeepSeek."""
    try:
        # Create context from both description and markdown
        combined_text = f"""
        Product Name: {product.get('name', 'Unknown')}
        Original Description: {product.get('description', '')}
        
        Product Page Content:
        {markdown}"""
        
        prompt = f"""
        Based on the coffee product information provided, extract the following attributes:
        
        1. roast_level: (exactly one of: light, medium-light, medium, medium-dark, dark)
        2. bean_type: (exactly one of: arabica, robusta, blend)
        3. processing_method: (one of: washed, natural, honey, anaerobic, or null if unclear)
        4. region: (geographic origin of the coffee beans)
        5. tasting_notes: (comma-separated flavor notes found in the description)
        6. flavor_profiles: (array of common flavor categories like: chocolate, fruity, nutty, caramel, berry, citrus, floral, spice)
        7. brew_methods: (array of recommended brewing methods like: espresso, filter, pour-over, french-press, aeropress, moka-pot, cold-brew)
        8. altitude_min: (lowest altitude in meters, integer only or null)
        9. altitude_max: (highest altitude in meters, integer only or null)
        10. varietal: (coffee varietal/cultivar like SL9, Catuai, Kent, etc.)
        11. is_blend: (boolean true if it's a blend of different beans)
        12. is_seasonal: (boolean true if it's described as a seasonal or limited release)
        
        DO NOT infer or guess any values. If a field is not clearly stated in the text, return null for that field.
        Return ONLY a valid JSON object with these fields and nothing else.
        """
        
        client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com"
        )
        
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a coffee expert who extracts structured attributes from product descriptions."},
                {"role": "user", "content": combined_text + "\n\n" + prompt}
            ],
            max_tokens=800,
            temperature=0.1
        )
        
        ai_response = response.choices[0].message.content
        
        # Extract JSON from response
        try:
            # Find JSON in the response
            json_start = ai_response.find('{')
            json_end = ai_response.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = ai_response[json_start:json_end]
                attributes = json.loads(json_str)
                
                # Update product with extracted attributes (only if not already present)
                for key, value in attributes.items():
                    if value is not None and (key not in product or not product[key]):
                        # Special handling for altitude values to ensure they're integers
                        if key in ['altitude_min', 'altitude_max'] and value:
                            try:
                                product[key] = int(value)
                            except (ValueError, TypeError):
                                pass
                        else:
                            product[key] = value
        except Exception as e:
            print(f"Error parsing DeepSeek JSON: {e}")
    
        return product
    except Exception as e:
        print(f"Error with DeepSeek extraction: {e}")
        return product

async def main():
    input_path = OUTPUT_CSV  # Path to your original scraped data
    output_path = ENRICHED_OUTPUT_CSV  # Where to save the enriched data
    
    # Load products with direct_buy_url
    products = await load_products_from_csv(input_path)
    
    # Filter for products with URLs
    products_to_enrich = [
        p for p in products 
        if p.get('direct_buy_url')
    ]
    
    print(f"Found {len(products_to_enrich)} products to enrich")
    
    # Save a sample of the markdown for inspection (optional)
    if products_to_enrich:
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
        from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
        from crawl4ai.content_filter_strategy import PruningContentFilter
        
        crawler = AsyncWebCrawler()
        sample_product = products_to_enrich[0]
        
        print(f"Generating sample markdown for: {sample_product.get('name')}")
        
        markdown_result = await crawler.arun(
            url=sample_product["direct_buy_url"],
            config=CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                markdown_generator=DefaultMarkdownGenerator(
                    content_filter=PruningContentFilter(threshold=0.5)
                )
            )
        )
        
        # Get the filtered markdown if available, otherwise use raw markdown
        markdown = markdown_result.markdown.fit_markdown or markdown_result.markdown.raw_markdown

        sample_path = "sample_markdown.md"
        with open(sample_path, 'w', encoding='utf-8') as f:
            f.write(markdown)
        
        print(f"Saved sample markdown to {sample_path}")
    
    # Enrich products with Crawl4AI and DeepSeek
    enriched_products = await enrich_products_with_crawl4ai(products_to_enrich, batch_size=5)
    
    # Merge enriched products back with original data
    enriched_dict = {p['direct_buy_url']: p for p in enriched_products if p.get('direct_buy_url')}
    
    for product in products:
        url = product.get('direct_buy_url')
        if url and url in enriched_dict:
            product.update(enriched_dict[url])
    
    # Save enriched products
    await save_enriched_products(products, output_path)
    
    print("Enrichment process completed!")

if __name__ == "__main__":
    asyncio.run(main())