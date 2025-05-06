import asyncio
import json
import csv
import re
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# Setup logging
import logging

import config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("roaster_scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Function to generate slug from name
def create_slug(name):
    return name.lower().replace(' ', '-').replace("'", '').replace('"', '')

async def enhance_roaster_extraction(roaster, crawler, run_config):
    """Enhanced version of roaster data extraction"""
    logging.info(f"Processing {roaster['roaster']}...")
    
    # State lookup mapping for standardization
    state_mapping = {
        'karnataka': 'Karnataka',
        'bangalore': 'Karnataka',
        'bengaluru': 'Karnataka',
        'kerala': 'Kerala',
        'tamil nadu': 'Tamil Nadu',
        'chennai': 'Tamil Nadu',
        'maharashtra': 'Maharashtra',
        'mumbai': 'Maharashtra',
        'delhi': 'Delhi',
        'new delhi': 'Delhi',
        'telangana': 'Telangana',
        'hyderabad': 'Telangana',
        'andhra pradesh': 'Andhra Pradesh',
    }
    
    try:
        # Extract basic data from CSV
        name = roaster['roaster']
        url = roaster['website']
        
        # Create roaster data object with initial values
        roaster_data = {
            "name": name,
            "slug": create_slug(name),
            "website_url": url,
            "location": None,
            "city": roaster.get('city', ''),
            "state": '',
            "instagram_handle": None,
            "description": None,
            "founded_year": None,
            "logo_url": None,
            "has_subscription": False,
            "has_physical_store": False,
            "is_featured": False,
        }
        
        # Try to infer state from city if available
        if roaster_data["city"]:
            city_lower = roaster_data["city"].lower()
            for key, value in state_mapping.items():
                if key in city_lower:
                    roaster_data["state"] = value
                    break
        
        # Set the location field based on city and state
        if roaster_data["city"] and roaster_data["state"]:
            roaster_data["location"] = f"{roaster_data['city']}, {roaster_data['state']}"
        elif roaster_data["city"]:
            roaster_data["location"] = roaster_data["city"]
        elif roaster_data["state"]:
            roaster_data["location"] = roaster_data["state"]
        
        # Extract Instagram from CSV if present
        if 'instagram' in roaster and roaster['instagram']:
            instagram = roaster['instagram']
            if 'instagram.com/' in instagram:
                handle = instagram.split('instagram.com/')[-1].split('/')[0].strip()
                # Normalize handle - remove trailing slashes, query params
                if '?' in handle:
                    handle = handle.split('?')[0]
                roaster_data["instagram_handle"] = handle
        
        # Fetch homepage with deeper content extraction
        homepage_config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED,
            markdown_generator=DefaultMarkdownGenerator(),
        )
        
        try:
            result = await crawler.arun(url=url, config=homepage_config)
        except Exception as e:
            if "ERR_SSL_VERSION_OR_CIPHER_MISMATCH" in str(e):
                logging.warning(f"SSL error on {url}, retrying with HTTP fallback...")
                alt_url = url.replace("https://", "http://")
                try:
                    result = await crawler.arun(url=alt_url, config=homepage_config)
                except Exception as e2:
                    logging.error(f"HTTP fallback also failed for {url}: {str(e2)}")
                    return None
            else:
                logging.error(f"Error accessing {url}: {str(e)}")
                return None
        
        if result and result.success:
            # Get both HTML and markdown for better extraction
            html_content = str(result.html)
            markdown_text = result.markdown.fit_markdown if hasattr(result.markdown, 'fit_markdown') else str(result.markdown)
            
            # Store limited markdown for potential GPT summarization later
            roaster_data["source_markdown"] = markdown_text[:3000]  # Limit to avoid overload
            
            # Use BeautifulSoup for better HTML parsing
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 1. Better logo extraction
            logo_url = None
            # Check for logo in image filenames
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if 'logo' in src.lower():
                    logo_url = src
                    break
            
            # If no logo found, try header image
            if not logo_url:
                header = soup.find('header')
                if header:
                    header_img = header.find('img')
                    if header_img and header_img.get('src'):
                        logo_url = header_img['src']
            
            # Favicon fallback
            if not logo_url:
                favicon_link = soup.find('link', rel=lambda r: r and ('icon' in r.lower()))
                if favicon_link and favicon_link.get('href'):
                    logo_url = favicon_link['href']
                else:
                    # Try default favicon.ico
                    logo_url = url.rstrip('/') + '/favicon.ico'
            
            # Fix relative URLs
            if logo_url and not (logo_url.startswith('http') or logo_url.startswith('//')):
                logo_url = url.rstrip('/') + ('/' if not logo_url.startswith('/') else '') + logo_url
            
            if logo_url:
                roaster_data["logo_url"] = logo_url
            
            # 2. Extract Instagram handle if not already found
            if not roaster_data["instagram_handle"]:
                # Look for Instagram links in HTML
                instagram_links = soup.select('a[href*="instagram.com"]')
                if instagram_links:
                    instagram_url = instagram_links[0].get('href', '')
                    insta_match = re.search(r'instagram\.com/([a-zA-Z0-9_\.]+)', instagram_url)
                    if insta_match:
                        handle = insta_match.group(1)
                        # Normalize handle
                        if '?' in handle:
                            handle = handle.split('?')[0]
                        roaster_data["instagram_handle"] = handle
            
            # Instagram OG tag fallback
            if not roaster_data["instagram_handle"]:
                og_site_name = soup.find('meta', property='og:site_name')
                if og_site_name and 'instagram' in og_site_name.get('content', '').lower():
                    roaster_data["instagram_handle"] = og_site_name.get('content', '').replace('Instagram: @', '').strip()
            
            # 3. Better description extraction
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                roaster_data["description"] = meta_desc.get('content')
            
            # If no meta description, look for about section
            if not roaster_data["description"] or len(roaster_data["description"]) < 100:
                about_section = soup.find(lambda tag: tag.name and tag.text and 'about' in tag.text.lower() and len(tag.text) > 100)
                if about_section:
                    roaster_data["description"] = about_section.text.strip()
            
            # If still no good description, use first substantial paragraph
            if not roaster_data["description"] or len(roaster_data["description"]) < 100:
                paragraphs = [p.text.strip() for p in soup.find_all('p') if len(p.text.strip()) > 100]
                if paragraphs:
                    coffee_related = [p for p in paragraphs if any(word in p.lower() for word in ['coffee', 'roast', 'bean', 'brew'])]
                    if coffee_related:
                        roaster_data["description"] = coffee_related[0]
                    else:
                        roaster_data["description"] = paragraphs[0]
            
            # 4. Check for subscription and physical store
            sub_indicators = ['subscription', 'subscribe', 'recurring', 'monthly delivery']
            roaster_data["has_subscription"] = any(indicator in html_content.lower() for indicator in sub_indicators)
            
            store_indicators = ['visit us', 'our store', 'physical location', 'cafe', 'visit our', 
                               'directions', 'opening hours', 'open from', 'coffee shop']
            roaster_data["has_physical_store"] = any(indicator in html_content.lower() for indicator in store_indicators)
            
            # 5. Look for founded year
            founded_patterns = [
                r'founded in (\d{4})',
                r'established in (\d{4})',
                r'since (\d{4})',
                r'est\. (\d{4})',
                r'started in (\d{4})',
                r'began in (\d{4})',
                r'founded.*?(\d{4})',
                r'established.*?(\d{4})'
            ]
            
            for pattern in founded_patterns:
                matches = re.search(pattern, html_content, re.IGNORECASE)
                if matches:
                    try:
                        year = int(matches.group(1))
                        if 1900 < year < 2025:  # Sanity check for valid years
                            roaster_data["founded_year"] = year
                            break
                    except ValueError:
                        continue
                    
        # 6. Detect the platform
        platform = await detect_platform(url, crawler)
        roaster_data["platform"] = platform

        # Try to visit about page if we're missing key information
        if not roaster_data.get("description") or not roaster_data.get("founded_year"):
            about_data = await crawl_about_pages(crawler, url, run_config)
            
            # Update any missing information from about pages
            for key, value in about_data.items():
                if (not roaster_data.get(key) or key == "description" and len(roaster_data[key]) < 100) and value:
                    roaster_data[key] = value
        
        # Final cleanup
        # Remove source_markdown before returning (unless you want to save it separately)
        if "source_markdown" in roaster_data:
            del roaster_data["source_markdown"]
        
        return roaster_data
        
    except Exception as e:
        logging.error(f"Error processing {roaster['roaster']}: {str(e)}")
        return None

async def crawl_about_pages(crawler, base_url, run_config):
    """Crawl about pages to extract missing information with confidence scoring"""
    about_suffixes = ['/about', '/about-us', '/our-story', '/story', '/about-our-company', '/who-we-are']
    about_data = {}
    confidence_score = 0  # Track how confident we are in the data
    
    for suffix in about_suffixes:
        try:
            about_url = base_url.rstrip('/') + suffix
            logging.info(f"Trying about page: {about_url}")
            
            about_result = await crawler.arun(
                url=about_url,
                config=run_config
            )
            
            if about_result and about_result.success:
                html_content = str(about_result.html)
                about_text = about_result.markdown.fit_markdown if hasattr(about_result.markdown, 'fit_markdown') else str(about_result.markdown)
                
                # Use BeautifulSoup for better parsing
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract description if not already found
                if not about_data.get("description"):
                    # First try to find sections with "about" or "story" in the heading
                    about_sections = []
                    for heading in soup.find_all(['h1', 'h2', 'h3']):
                        if any(term in heading.text.lower() for term in ['about', 'story', 'journey', 'who we are']):
                            # Get next element which might be a paragraph
                            next_elem = heading.find_next(['p', 'div'])
                            if next_elem and len(next_elem.text.strip()) > 100:
                                about_sections.append(next_elem.text.strip())
                                confidence_score += 5  # Higher confidence as it's under a relevant heading
                    
                    if about_sections:
                        about_data["description"] = about_sections[0]
                    else:
                        # Fall back to any substantial paragraph
                        paragraphs = [p.text.strip() for p in soup.find_all('p') if len(p.text.strip()) > 100]
                        if paragraphs:
                            about_data["description"] = paragraphs[0]
                            confidence_score += 2  # Lower confidence as it's just a paragraph
                
                # Look for founded year
                founded_patterns = [
                    r'founded in (\d{4})',
                    r'established in (\d{4})',
                    r'since (\d{4})',
                    r'est\. (\d{4})',
                    r'started in (\d{4})',
                    r'began in (\d{4})',
                    r'founded.*?(\d{4})',
                    r'established.*?(\d{4})'
                ]
                
                for pattern in founded_patterns:
                    matches = re.search(pattern, html_content, re.IGNORECASE)
                    if matches:
                        try:
                            year = int(matches.group(1))
                            if 1900 < year < 2025:
                                about_data["founded_year"] = year
                                
                                # Check if "founded" text is near "about" or "story" for higher confidence
                                context = html_content[max(0, matches.start() - 100):min(len(html_content), matches.end() + 100)]
                                if any(term in context.lower() for term in ['about', 'story', 'history']):
                                    confidence_score += 5
                                else:
                                    confidence_score += 3
                                    
                                break
                        except ValueError:
                            continue
                
                # If we got both description and founded year with high confidence, we can stop
                if about_data.get("description") and about_data.get("founded_year") and confidence_score >= 8:
                    break
        
        except Exception as e:
            logging.warning(f"Error accessing about page {suffix}: {str(e)}")
    
    return about_data

async def scrape_roasters():
    """Process all roasters with a single browser instance"""
    # Load roasters from CSV
    roasters = []
    try:
        with open(config.INPUT_CSV, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                roasters.append(row)
    except Exception as e:
        logging.error(f"Error loading CSV: {str(e)}")
        return [], []
    
    # Decide whether to run for all roasters or just test with a few
    # Uncomment the line below to process all roasters
    test_roasters = roasters
    
    # Or keep the test limit for initial testing
    #test_count = 10
    #test_roasters = roasters[:test_count]
    
    results = []
    errors = []
    
    # Configure browser
    browser_config = BrowserConfig(
        headless=True,
        verbose=True
    )
    
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        markdown_generator=DefaultMarkdownGenerator()
    )
    
    # Use a single browser context for all roasters
    async with AsyncWebCrawler(config=browser_config) as crawler:
        for roaster in test_roasters:
            try:
                # Use the enhanced extraction method
                roaster_data = await enhance_roaster_extraction(roaster, crawler, run_config)
                
                if roaster_data:
                    results.append(roaster_data)
                    logging.info(f"Successfully processed {roaster['roaster']}")
                else:
                    errors.append({
                        "roaster": roaster['roaster'],
                        "website": roaster['website'],
                        "error": "Failed to extract data"
                    })
                    
            except Exception as e:
                logging.error(f"Error processing {roaster['roaster']}: {str(e)}")
                errors.append({
                    "roaster": roaster['roaster'],
                    "website": roaster['website'],
                    "error": str(e)
                })
    
    return results, errors

async def summarize_descriptions_with_deepseek(roaster_data_list):
    """Use Deepseek LLM to create concise, meaningful descriptions"""
    import aiohttp
    
    DEEPSEEK_API_KEY = "sk-43eb2a22bce542c89076ea850bb46d27"  # Replace with your key
    DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"  # Check this endpoint in their docs
    
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    async with aiohttp.ClientSession() as session:
        for roaster in roaster_data_list:
            if not roaster.get("source_markdown") and not roaster.get("description"):
                continue
                
            source_text = roaster.get("source_markdown", roaster.get("description", ""))
            
            try:
                # Create a prompt specifically for coffee roaster descriptions
                prompt = f"""Summarize this coffee roaster description in 2-3 concise sentences. 
                Focus on their origin, coffee types, values, and what makes them unique.
                
                Text to summarize: {source_text[:4000]}
                
                Coffee roaster name: {roaster['name']}"""
                
                payload = {
                    "model": "deepseek-chat",  # Use appropriate model name from Deepseek
                    "messages": [
                        {"role": "system", "content": "You are a coffee expert who writes concise, engaging descriptions."},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 150,
                    "temperature": 0.7
                }
                
                async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        if "choices" in result and result["choices"] and "message" in result["choices"][0]:
                            roaster["description"] = result["choices"][0]["message"]["content"].strip().replace('\n', ' ')
                            logging.info(f"Generated description for {roaster['name']}")
                    else:
                        error_text = await response.text()
                        logging.error(f"Deepseek API error: {response.status} - {error_text}")
                        # Fallback if API fails
                        if source_text:
                            roaster["description"] = source_text[:300].strip()
                
            except Exception as e:
                logging.error(f"Error generating description for {roaster['name']}: {str(e)}")
                # Fallback if exception occurs
                if source_text:
                    roaster["description"] = source_text[:300].strip()
                
            # Add a small delay to avoid rate limiting
            await asyncio.sleep(0.5)
    
    return roaster_data_list

async def detect_platform(url, crawler=None):
    """Detect the e-commerce platform used by a website"""
    try:
        # If a crawler is provided, use it (to benefit from caching)
        if crawler:
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    markdown_generator=DefaultMarkdownGenerator()
                )
            )
            if result and result.success:
                html_content = str(result.html)
            else:
                return "unknown"
        else:
            # Otherwise use aiohttp directly
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    html_content = await resp.text()
                    
        # Use BeautifulSoup to parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Shopify
        if soup.find('script', src=lambda x: x and 'cdn.shopify.com' in x):
            return "shopify"
        if soup.find(attrs={"data-shopify": True}):
            return "shopify"
        
        # WooCommerce
        if soup.find('body', class_=lambda c: c and 'woocommerce' in c):
            return "woocommerce"
        if soup.find('link', href=lambda x: x and 'woocommerce' in x):
            return "woocommerce"
        
        # Magento
        if soup.find('meta', attrs={"name": "generator", "content": lambda x: x and 'Magento' in x}):
            return "magento"
        if 'mage-' in html_content:
            return "magento"
        
        # WordPress
        if soup.find('meta', attrs={"name": "generator", "content": lambda x: x and 'WordPress' in x}):
            return "wordpress"
        if soup.find(class_=lambda x: x and 'wp-' in x):
            return "wordpress"
            
        # Framer
        if 'framer.com' in html_content or 'framerusercontent.com' in html_content:
            return "framer"
            
        # Webflow
        if 'webflow.com' in html_content:
            return "webflow"
            
        # Squarespace
        if 'squarespace.com' in html_content:
            return "squarespace"
            
        # Wix
        if 'wix.com' in html_content or 'wixsite.com' in html_content:
            return "wix"

        return "static"

    except Exception as e:
        logging.warning(f"Platform detection error for {url}: {str(e)}")
        return "unknown"
    
async def main():
    """Main function to orchestrate the entire scraping flow"""
    print("Starting roaster scraper...")
    
    # Process roasters
    results, errors = await scrape_roasters()
    print(f"Scraped {len(results)} roasters, encountered {len(errors)} errors")
    
    # Process descriptions with LLM
    if results:
        print("Enhancing descriptions with DeepSeek...")
        results = await summarize_descriptions_with_deepseek(results)
        print(f"Enhanced {len(results)} descriptions")
    
    # Save results to CSV
    if results:
        try:
            csv_file = Path('enriched_roasters.csv')
            print(f"Attempting to write CSV to {csv_file.absolute()}")
            
            with csv_file.open('w', newline='', encoding='utf-8') as file:
                fieldnames = [
                    'name', 'slug', 'description', 'website_url', 'instagram_handle', 'location',
                    'city', 'state', 'founded_year', 'logo_url',
                    'has_subscription', 'has_physical_store', 'is_featured', 'platform'
                ]
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                
                for i, result in enumerate(results):
                    # Only write fields that exist in our schema
                    filtered_result = {k: v for k, v in result.items() if k in fieldnames}
                    try:
                        writer.writerow(filtered_result)
                    except Exception as row_err:
                        print(f"Error writing row {i} to CSV: {str(row_err)}")
                        print(f"Problematic data: {filtered_result}")
            
            logging.info(f"Results saved to {csv_file}")
            print(f"CSV export completed successfully")
        
        except Exception as e:
            print(f"ERROR during CSV export: {str(e)}")
            logging.error(f"Failed to save CSV: {str(e)}")
            
            # Also save as JSON for easier importing to Supabase
            json_file = Path('enriched_roasters.json')
            with json_file.open('w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            
            logging.info(f"Results also saved to {json_file}")
            print(f"Results also saved to {json_file}")
        
    # Save errors to CSV
    if errors:
        error_file = Path('errors.csv')
        with error_file.open('w', newline='', encoding='utf-8') as file:
            fieldnames = ['roaster', 'website', 'error']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for error in errors:
                writer.writerow(error)
        
        logging.info(f"Errors saved to {error_file}")
        print(f"Errors saved to {error_file}")
    
    # Print results for verification
    for result in results:
        print("\n" + "="*40)
        print(f"ROASTER: {result['name']}")
        print("="*40)
        for key, value in result.items():
            print(f"{key}: {value}")
    
    logging.info(f"Successfully processed {len(results)} of {len(errors) + len(results)} roasters")
    print(f"Successfully processed {len(results)} of {len(errors) + len(results)} roasters")
    print("Scraping complete!")

if __name__ == "__main__":
    asyncio.run(main())