# coffee_scraper/scrapers/static.py

import re
import aiohttp
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime
from common.utils import create_slug, fetch_with_retry, clean_html, is_coffee_product, record_skipped_product, standardize_coffee_data
from common.deepseek_extractor import enhance_with_deepseek
from config import USE_DEEPSEEK

def needs_enhancement(coffee):
    missing = []
    if "roast_level" not in coffee: missing.append("roast_level")
    if "bean_type" not in coffee: missing.append("bean_type")
    if "processing_method" not in coffee and not coffee.get("is_blend", False): missing.append("processing_method")
    if "flavor_profiles" not in coffee: missing.append("flavor_profiles")
    return len(missing) >= 2

async def scrape_static_site(roaster):
    base_url = roaster['website_url'].rstrip('/')
    sitemap_url = f"{base_url}/sitemap.xml"
    candidate_products = []
    scraped_coffees = []

    async with aiohttp.ClientSession() as session:
        try:
            # Step 1: Load root sitemap
            root_xml = await fetch_with_retry(sitemap_url, session)
            root = ET.fromstring(root_xml)
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            child_sitemaps = [loc.text for loc in root.findall('.//ns:loc', ns)]

            for child_url in child_sitemaps:
                if not any(kw in child_url.lower() for kw in ["product", "store", "shop", "ols", "catalog"]):
                    continue
                try:
                    print(f"📦 Checking sitemap: {child_url}")
                    child_xml = await fetch_with_retry(child_url, session)
                    child_root = ET.fromstring(child_xml)
                    for entry in child_root.findall('.//ns:url', ns):
                        loc = entry.find('ns:loc', ns)
                        if loc is None:
                            continue
                        url = loc.text
                        title = ""
                        img_tag = entry.find('.//{http://www.google.com/schemas/sitemap-image/1.1}title')
                        if img_tag is not None:
                            title = img_tag.text.strip()
                        else:
                            title = url.split("/")[-1].replace("-", " ")
                        if any(kw in title.lower() for kw in ["coffee", "brew", "kaapi", "beans", "roast", "arabica", "blend"]):
                            candidate_products.append((url, title))
                        else:
                            record_skipped_product(title, "not matched in light filter", roaster['name'], url)
                except Exception as e:
                    print(f"⚠️ Failed to parse {child_url}: {e}")

            print(f"✅ Found {len(candidate_products)} candidates via sitemap.")

            # Step 2: Scrape details
            for i, (url, name) in enumerate(candidate_products):
                try:
                    html = await fetch_with_retry(url, session)
                    soup = BeautifulSoup(html, 'html.parser')
                    desc = soup.find("meta", attrs={"name": "description"})
                    description = desc["content"] if desc else ""

                    coffee = {
                        "name": name,
                        "slug": create_slug(name),
                        "roaster_slug": roaster['slug'],
                        "description": clean_html(description),
                        "direct_buy_url": url,
                        "is_available": True,
                        "last_scraped_at": datetime.now().isoformat(),
                        "scrape_status": "success"
                    }

                    img = soup.find("meta", property="og:image")
                    if img:
                        coffee["image_url"] = img.get("content")

                    if USE_DEEPSEEK and needs_enhancement(coffee):
                        coffee = await enhance_with_deepseek(coffee, roaster['name'])

                    scraped_coffees.append(standardize_coffee_data(coffee))
                except Exception as e:
                    print(f"❌ Error scraping {url}: {e}")

        except Exception as e:
            print(f"❌ Could not load sitemap for {base_url}: {e}")

    return scraped_coffees
