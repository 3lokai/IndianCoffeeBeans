# markdown_enricher.py - Module for enriching products using markdown and LLM

import asyncio
from typing import Dict, List, Any

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from openai import OpenAI
import json
from config import DEEPSEEK_API_KEY

async def enrich_products_with_markdown(products: List[Dict[str, Any]], batch_size: int = 5) -> List[Dict[str, Any]]:
    """Process products in batches to avoid overwhelming resources"""
    results = []
    for i in range(0, len(products), batch_size):
        batch = products[i:i+batch_size]
        batch_results = await asyncio.gather(
            *[enrich_single_product(product) for product in batch]
        )
        results.extend(batch_results)
        print(f"Processed batch {i//batch_size + 1}/{(len(products) + batch_size - 1)//batch_size}")
        await asyncio.sleep(1)  # Be nice to servers
    return results

async def enrich_single_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a single product using markdown conversion and LLM extraction"""
    if not product.get('direct_buy_url'):
        return product
        
    try:
        # Initialize crawler
        crawler = AsyncWebCrawler()
        
        # Fetch and convert to markdown
        markdown = await crawler.arun(
            url=product["direct_buy_url"],
            config=CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                markdown_generator=DefaultMarkdownGenerator()
            )
        )
        
        # Skip if we couldn't get markdown
        if not markdown:
            print(f"Failed to get markdown for {product.get('name')}")
            return product
            
        # Enrich using DeepSeek
        return await extract_attributes_from_markdown(product, markdown)
    
    except Exception as e:
        print(f"Error enriching {product.get('name')}: {e}")
        return product

async def extract_attributes_from_markdown(product: Dict[str, Any], markdown: str) -> Dict[str, Any]:
    """Extract structured attributes from markdown using DeepSeek"""
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