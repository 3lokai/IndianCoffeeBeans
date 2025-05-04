# scrapers/roaster_pipeline.py
import asyncio
from datetime import datetime
import logging
from typing import Dict, Any, Optional, List, Tuple
import aiohttp
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from crawl4ai import PruningContentFilter
from common.description_processor import DescriptionProcessor
from openai import OpenAI
from config import DEEPSEEK_API_KEY
from common.utils import slugify, load_from_cache, save_to_cache
from common.models import RoasterModel
from scrapers.platform_detector import PlatformDetector
from config import USER_AGENT, REQUEST_TIMEOUT, MAX_RETRIES, CRAWL_DELAY, DEEPSEEK_API_KEY

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
logger = logging.getLogger(__name__)
CURRENT_YEAR = datetime.now().year
# State lookup mapping for standardization
STATE_MAPPING = {
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

class RoasterPipeline:
    """Pipeline for extracting roaster metadata"""
    def _try_load_crawl4ai(self):
        try:
            from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
            from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

            self.AsyncWebCrawler = AsyncWebCrawler
            self.BrowserConfig = BrowserConfig
            self.CrawlerRunConfig = CrawlerRunConfig
            self.CacheMode = CacheMode
            self.DefaultMarkdownGenerator = DefaultMarkdownGenerator
            self.crawl4ai_loaded = True
            logger.info("Crawl4AI successfully loaded")
        except ImportError:
            self.crawl4ai_loaded = False
            logger.warning("Crawl4AI not available")
            
    def __init__(self, db_client=None, refresh_cache=False):
        self.db_client = db_client
        self.refresh_cache = refresh_cache
        self.platform_detector = PlatformDetector()
        self.session = None
        self.crawl4ai_loaded = False
        self._try_load_crawl4ai()
        
    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
                
    async def fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content with retries and fallback to HTTP"""
        await self._init_session()

        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    logger.warning(f"[{response.status}] Failed to fetch {url}")

            except Exception as e:
                logger.warning(f"[Attempt {attempt+1}] Error fetching {url}: {e}")

                # Retry with HTTP if HTTPS SSL fails
                if attempt == 0 and "SSL" in str(e) and url.startswith("https://"):
                    fallback_url = url.replace("https://", "http://")
                    logger.info(f"Trying HTTP fallback: {fallback_url}")
                    try:
                        async with self.session.get(fallback_url) as response:
                            if response.status == 200:
                                return await response.text()
                    except Exception as http_e:
                        logger.warning(f"HTTP fallback failed: {http_e}")

            # Exponential backoff with cap
            await asyncio.sleep(min(CRAWL_DELAY * (2 ** attempt), 10))

        logger.error(f"All attempts failed for {url}")
        return None
        
    async def extract_logo(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract logo URL from homepage or fallback"""
        selectors = [
        lambda s: any('logo' in (img.get('src') or '').lower() for img in s.find_all('img')),
        "header .logo img", "header img.logo", ".site-logo img", ".logo-img", 
        "#logo img", "img.logo", "[class*='logo'] img", ".navbar-brand img", 
        ".brand img", ".header-logo img", "a[class*='logo'] img", "h1.logo a img",
        ".site-header__logo img", ".header__heading-logo img", ".logo-bar__image img", 
        ".custom-logo-link img"  # Added Shopify/WooCommerce selectors
        ]

        logo_url = None

        # Method 1: Simple loop on inline lambda check
        for img in soup.find_all('img'):
            src = img.get('src', '')
            if 'logo' in src.lower():
                logo_url = src
                break

        # Method 2: CSS selectors
        if not logo_url:
            for selector in selectors[1:]:  # Skip lambda
                try:
                    logo_img = soup.select_one(selector)
                    if logo_img and logo_img.get('src'):
                        logo_url = logo_img['src']
                        break
                except Exception:
                    continue

        # Method 3: OpenGraph
        if not logo_url:
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                logo_url = og["content"]

        # Method 4: Favicon fallback
        if not logo_url:
            icon_link = soup.find('link', rel=lambda r: r and 'icon' in r.lower())
            if icon_link and icon_link.get('href'):
                logo_url = icon_link['href']
            else:
                logo_url = base_url.rstrip('/') + '/favicon.ico'

        # Normalize relative paths
        if logo_url and not logo_url.startswith(('http://', 'https://')):
            logo_url = urljoin(base_url, logo_url)

        return logo_url

        
    async def extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract description from meta, OG, or about section"""
        
        def clean(text):
            cleaned = re.sub(r'\s+', ' ', text).strip()
            return cleaned[:497] + "..." if len(cleaned) > 500 else cleaned

        # 1. Meta description
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return clean(meta["content"])

        # 2. OpenGraph description
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            return clean(og["content"])

        # 3. About sections
        about_selectors = [
            ".about-us", "#about", ".about-section", "section.about", "[class*='about']",
            ".page-width", ".entry-content"  # Added Shopify/WooCommerce selectors
        ]
        for selector in about_selectors:
            try:
                node = soup.select_one(selector)
                if node:
                    text = node.get_text(strip=True)
                    if text:
                        return clean(text)
            except Exception:
                continue

        # 4. First substantial coffee-related paragraph
        paragraphs = [p.text.strip() for p in soup.find_all('p') if len(p.text.strip()) > 100]
        for p in paragraphs:
            if any(term in p.lower() for term in ['coffee', 'roast', 'bean', 'brew']):
                return clean(p)

        return None

        
    async def extract_social_media(self, soup: BeautifulSoup) -> Tuple[List[str], Optional[str]]:
        platforms = ["instagram", "facebook", "twitter", "linkedin", "youtube"]
        
        # Add selector-based approach
        social_selectors = [
            ".social-icons a", ".site-footer__social-icons a", ".header__social-icons a", 
            ".footer-social a", ".header-social a"
        ]
        
        # Get links from selectors first
        social_links = []
        for selector in social_selectors:
            try:
                for link in soup.select(f"{selector}[href]"):
                    href = link.get("href", "")
                    if any(p in href.lower() for p in platforms):
                        social_links.append(href)
            except:
                continue
        
        # Fall back to old method if we didn't find anything
        if not social_links:
            links = [a["href"] for a in soup.find_all("a", href=True)]
            social_links = [url for url in links if any(p in url.lower() for p in platforms)]
        
        # Extract Instagram handle
        instagram = next((re.search(r'instagram\.com/([^/?]+)', url).group(1) 
                        for url in social_links if 'instagram.com' in url and re.search(r'instagram\.com/([^/?]+)', url)), None)

        return social_links, instagram

        
    def _extract_email(self, soup: BeautifulSoup) -> Optional[str]:
        """Try to extract email from links and text"""
        email_regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

        # From mailto links
        mailto = soup.find('a', href=lambda h: h and 'mailto:' in h)
        if mailto:
            return mailto['href'].replace('mailto:', '').strip()

        # From visible contact links
        links = soup.find_all("a", href=True)
        for link in links:
            match = re.search(email_regex, link.text)
            if match:
                return match.group(0)

        # From whole HTML
        matches = re.findall(email_regex, str(soup))
        return matches[0] if matches else None


    def _extract_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Try to extract phone from tel links or visible text"""
        phone_regex = r'(?:\+91[-\s]?)?[6-9]\d{9}'

        # From tel: links
        tel = soup.find('a', href=lambda h: h and 'tel:' in h.lower())
        if tel:
            return tel['href'].replace('tel:', '').strip()

        # From entire HTML
        matches = re.findall(phone_regex, str(soup))
        return matches[0] if matches else None


    async def extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """Extract email and phone"""
        return {
            "contact_email": self._extract_email(soup),
            "contact_phone": self._extract_phone(soup),
        }

        
    async def extract_location(self, soup: BeautifulSoup, city_from_input: Optional[str] = None) -> Dict[str, Optional[str]]:
        """Extract city and infer state"""
        result = {"city": city_from_input, "state": None}

        def match_state(value: str) -> Optional[str]:
            for k, v in STATE_MAPPING.items():
                if k in value.lower() or v.lower() == value.lower():
                    return v
            return None

        # Use existing city to infer state
        if city_from_input:
            result["state"] = match_state(city_from_input)

        # Check structured address blocks
        selectors = [
            ".address", ".location", ".contact-address", "footer address",
            ".site-footer__contact-info", ".footer-contact"  # Added Shopify/WooCommerce selectors
            ]
        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                text = node.get_text(strip=True)
                parts = re.split(r'[,\-]', text)
                if len(parts) >= 2:
                    result["city"] = result["city"] or parts[0].strip()
                    state_guess = parts[1].strip()
                    result["state"] = result["state"] or match_state(state_guess)

        return result

    async def _has_keywords(self, soup: BeautifulSoup, keywords: List[str]) -> bool:
        """Check presence of keywords in links and page text"""
        # In anchor text
        for link in soup.find_all("a"):
            if any(kw in (link.text or "").lower() for kw in keywords):
                return True

        # In visible text
        for tag in soup.find_all(["div", "section", "p"]):
            if any(kw in (tag.text or "").lower() for kw in keywords):
                return True

        return any(kw in str(soup).lower() for kw in keywords)


    async def has_subscription(self, soup: BeautifulSoup) -> bool:
        return await self._has_keywords(soup, ["subscription", "subscribe", "monthly", "recurring"])


    async def has_physical_store(self, soup: BeautifulSoup) -> bool:
        return await self._has_keywords(soup, ["visit us", "our location", "our cafe", "store hours", "physical store"])

        
    async def extract_founded_year(self, soup: BeautifulSoup) -> Optional[int]:
        html = str(soup)
        patterns = [
            r'founded\s+in\s+(\d{4})', r'established\s+in\s+(\d{4})', 
            r'since\s+(\d{4})', r'est\.\s*(\d{4})'
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1900 <= year <= CURRENT_YEAR:
                    return year

        return None

        
    async def get_platform_specific_about_paths(self, platform: str) -> List[str]:
        """Get most likely about page paths based on detected platform"""
        
        # Common paths across all platforms
        common_paths = ['/about', '/about-us', '/our-story', '/story']
        
        # Platform-specific paths
        platform_paths = {
            "shopify": [
                '/pages/about', 
                '/pages/about-us',
                '/pages/our-story',
                '/pages/story',
                '/pages/mission'
            ],
            "woocommerce": [
                '/about-page',
                '/our-company',
                '/our-mission',
                '/who-we-are'
            ],
            "wordpress": [
                '/index.php/about',
                '/index.php/about-us',
                '/about-page'
            ],
            "squarespace": [
                '/about-1',
                '/aboutus'
            ],
            "wix": [
                '/about-1',
                '/about-us-1'
            ]
        }
        
        # Get platform-specific paths or empty list if platform not found
        specific_paths = platform_paths.get(platform, [])
        
        # Combine platform-specific paths with common ones
        # Put platform-specific ones first for efficiency
        return specific_paths + common_paths

    async def crawl_about_pages(self, base_url: str, platform: str = "unknown") -> Dict[str, Any]:
        """Try crawling known about pages to extract missing fields with platform awareness"""
        
        # Get platform-specific paths first
        suffixes = await self.get_platform_specific_about_paths(platform)
        logger.info(f"Using platform-specific paths for {platform}: {suffixes[:5]}")
        # Add any other custom paths you want to check
        if platform not in ["shopify", "woocommerce", "wordpress"]:
            suffixes.extend([
                '/philosophy',
                '/values',
                '/mission'
            ])
        
        results = {}
        descriptions = []  # Collect all descriptions for scoring later
        
        # Track if we've found a valid about page
        found_valid_about_page = False
        required_fields = ["social_links", "contact_email", "city"]
        
        for suffix in suffixes:
             # If we've already found a good about page with most data, stop searching
            if found_valid_about_page and len([k for k in required_fields if k in results]) >= 2:
                logger.info(f"Found sufficient about page data, stopping search")
                break

            url = base_url.rstrip('/') + suffix
            logger.info(f"Trying about page: {url}")

            try:
                html = await self.fetch_page(url)
                if not html:
                    continue

                soup = BeautifulSoup(html, "html.parser")

                # Description - Store all candidates
                desc = await self.extract_description(soup)
                if desc:
                    descriptions.append(desc)
                    found_valid_about_page = True  # This is likely an about page if it has a description

                # Contact
                if not results.get("contact_email") or not results.get("contact_phone"):
                    contact = await self.extract_contact_info(soup)
                    if any(contact.values()):
                        found_valid_about_page = True
                    results.update({k: v for k, v in contact.items() if v})

                # Social
                if not results.get("social_links"):
                    links, insta = await self.extract_social_media(soup)
                    if links:
                        results["social_links"] = links
                        found_valid_about_page = True
                    if insta:
                        results["instagram_handle"] = insta

                # Location
                if not results.get("city") or not results.get("state"):
                    loc = await self.extract_location(soup)
                    if any(loc.values()):
                        found_valid_about_page = True
                    results.update({k: v for k, v in loc.items() if v})

                # Founded year
                if not results.get("founded_year"):
                    year = await self.extract_founded_year(soup)
                    if year:
                        results["founded_year"] = year
                        found_valid_about_page = True
                
            except Exception as e:
                logger.warning(f"Error scraping {url}: {str(e)}")

        # Store all descriptions for processing later
        if descriptions:
            results["about_page_descriptions"] = descriptions
        
        return results


    async def enhance_description_with_deepseek(self, markdown: str, roaster_name: str) -> Optional[str]:
        """Use DeepSeek (OpenAI client) to generate summary"""
        if not DEEPSEEK_API_KEY:
            logger.warning("DeepSeek key missing.")
            return None

        try:
            prompt = f"""
            Summarize this coffee roaster’s story in 5-6 sentences.
            Focus on their origin, coffee types, values, and uniqueness.

            Roaster Name: {roaster_name}
            Source Text: {markdown[:10000]}
            """

            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a coffee domain expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=150,
                stream=False
            )

            return response.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"DeepSeek (OpenAI) failed: {str(e)}")
            return None
        
    async def process_with_crawl4ai(self, roaster_data: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Process a roaster using Crawl4AI for enhanced extraction"""
        name = roaster_data.get("name")
        website = roaster_data.get("website_url")
        
        if not name or not website:
            logger.error(f"Missing required fields for roaster: {roaster_data}")
            return None
            
        # Normalize website URL
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
      
        logger.info(f"Processing roaster with Crawl4AI: {name} ({website})")
        
        # Check cache first
        cache_key = f"roaster_{slugify(name)}"
        if not self.refresh_cache:
            cached_data = load_from_cache(cache_key, "roasters")
        if cached_data:
            logger.info(f"Found cached data for {name}")
            try:
                # Convert the cached dictionary back to a RoasterModel
                return RoasterModel(**cached_data)
            except Exception as e:
                logger.error(f"Error converting cached data to model: {str(e)}")
                # Clear invalid cache
                return None
                    
        try:
            # Configure Crawl4AI
            browser_config = self.BrowserConfig(
                headless=True,
                verbose=True
            )
            
            run_config = self.CrawlerRunConfig(
            cache_mode=self.CacheMode.ENABLED,
            markdown_generator=self.DefaultMarkdownGenerator(
                content_filter=PruningContentFilter(threshold=0.5, threshold_type="fixed")
            )
            )
            
            # Create roaster data object with initial values
            roaster = {
                "name": name,
                "slug": roaster_data.get("slug") or slugify(name),
                "website_url": website,
                "description": None,
                "city": roaster_data.get("city", None),
                "state": None,
                "social_links": [],
                "contact_email": None,
                "contact_phone": None,
                "founded_year": None,
                "logo_url": None,
                "has_subscription": False,
                "has_physical_store": False,
                "is_featured": False,
                "instagram_handle": None,
            }
            
            # Create crawler and fetch page
            async with self.AsyncWebCrawler(config=browser_config) as crawler:
                try:
                    # First detect the platform directly
                    platform_info = await self.platform_detector.detect(website)
                    platform_type = platform_info.get("platform", "unknown")
                    result = await crawler.arun(url=website, config=run_config)
                except Exception as e:
                    if "ERR_SSL_VERSION_OR_CIPHER_MISMATCH" in str(e):
                        logger.warning(f"SSL error on {website}, retrying with HTTP fallback...")
                        alt_url = website.replace("https://", "http://")
                        try:
                            result = await crawler.arun(url=alt_url, config=run_config)
                        except Exception as e2:
                            logger.error(f"HTTP fallback also failed for {website}: {str(e2)}")
                            return None
                    else:
                        logger.error(f"Error accessing {website}: {str(e)}")
                        return None
                
                if result and result.success:
                    # Get both HTML and markdown for better extraction
                    html_content = str(result.html)
                    markdown_text = result.markdown.fit_markdown if hasattr(result.markdown, 'fit_markdown') else str(result.markdown)
                    
                    # Store limited markdown for potential DeepSeek summarization later
                    source_markdown = markdown_text[:3000]  # Limit to avoid overload
                    
                    # Use BeautifulSoup for HTML parsing
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Extract all metadata from soup
                    logo_url = await self.extract_logo(soup, website)
                    homepage_description = await self.extract_description(soup)
                    contact_info = await self.extract_contact_info(soup)
                    social_links, instagram_handle = await self.extract_social_media(soup)
                    location_info = await self.extract_location(soup, roaster.get("city"))
                    has_subscription = await self.has_subscription(soup)
                    has_physical_store = await self.has_physical_store(soup)
                    founded_year = await self.extract_founded_year(soup)

                    # Try schema.org extraction first
                    schema_description = DescriptionProcessor.extract_from_schema(html_content)

                    # Collect description candidates
                    description_sources = {
                        'homepage': homepage_description,
                        'schema': schema_description,
                        'about_pages': [],
                        'markdown': source_markdown
                    }

                    # Update basic roaster data
                    roaster.update({
                        "logo_url": logo_url,
                        "contact_email": contact_info.get("contact_email"),
                        "contact_phone": contact_info.get("contact_phone"),
                        "city": location_info.get("city"),
                        "state": location_info.get("state"),
                        "social_links": social_links,
                        "instagram_handle": instagram_handle,
                        "has_subscription": has_subscription,
                        "has_physical_store": has_physical_store,
                        "founded_year": founded_year,
                        "_platform": platform_info
                    })

                    # Try to visit about page for missing information, using platform-aware paths
                    logger.info(f"Platform type before crawling about pages: '{platform_type}'")
                    about_data = await self.crawl_about_pages(website, platform_type)

                    # Add about page descriptions to candidates
                    if about_data.get("about_page_descriptions"):
                        description_sources["about_pages"] = about_data.get("about_page_descriptions")

                    # Update any missing non-description information from about pages
                    for key, value in about_data.items():
                        if key != "about_page_descriptions" and (not roaster.get(key)) and value:
                            roaster[key] = value

                    # Process all descriptions through central processor
                    final_description = await DescriptionProcessor.compile_description(
                        description_sources, 
                        name, 
                        deepseek_client=client if DEEPSEEK_API_KEY else None
                    )

                    # Set the final description
                    roaster["description"] = final_description
                
                # Pydantic validation before Save to cache
                try:
                    roaster = RoasterModel(**roaster).dict()
                except Exception as e:
                    logger.error(f"Validation failed for {name}: {str(e)}")
                    return None
                save_to_cache(cache_key, roaster, "roasters")
                
                # Insert into database if client provided
                if self.db_client:
                    try:
                        # Create a copy without non-DB fields
                        db_roaster = {k: v for k, v in roaster.items() if k not in ["_platform"]}
                        
                        # Use the upsert_roaster method from SupabaseClient
                        roaster_id = self.db_client.upsert_roaster(db_roaster)
                        logger.info(f"Saved roaster {name} to database with ID: {roaster_id}")
                        
                    except Exception as e:
                        logger.error(f"Error saving roaster {name} to database: {str(e)}")
                
                return roaster
                
        except Exception as e:
            logger.error(f"Error processing roaster {name} with Crawl4AI: {str(e)}")
            return None
        
    async def process_roaster(self, roaster_data: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Process a single roaster, extract metadata, and store in database
        
        Args:
            roaster_data (Dict[str, str]): Basic roaster info with name and website
            
        Returns:
            Optional[Dict[str, Any]]: Enriched roaster data
        """
        # Try using Crawl4AI if available
        if self.crawl4ai_loaded:
            try:
                crawl4ai_result = await self.process_with_crawl4ai(roaster_data)
                if crawl4ai_result:
                    return crawl4ai_result
            except Exception as e:
                logger.error(f"Error using Crawl4AI for {roaster_data.get('name')}: {str(e)}")
                logger.info("Falling back to basic extraction method")
        
        # Fall back to basic method if Crawl4AI fails or is not available
        name = roaster_data.get("name")
        website = roaster_data.get("website_url")
        
        if not name or not website:
            logger.error(f"Missing required fields for roaster: {roaster_data}")
            return None
            
        # Normalize website URL
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
            
        logger.info(f"Processing roaster with basic method: {name} ({website})")
        
        # Check cache first
        cache_key = f"roaster_{slugify(name)}"
        if not self.refresh_cache:
            cached_data = load_from_cache(cache_key, "roasters")
            if cached_data:
                logger.info(f"Found cached data for {name}")
                return cached_data
        
        # Fetch homepage
        html_content = await self.fetch_page(website)
        if not html_content:
            logger.error(f"Failed to fetch homepage for {name}")
            return None
            
        # Parse HTML
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Detect platform
        platform_info = await self.platform_detector.detect(website, html_content)
        platform_type = platform_info.get("platform", "unknown")

        # Extract metadata
        logo_url = await self.extract_logo(soup, website)
        homepage_description = await self.extract_description(soup)
        contact_info = await self.extract_contact_info(soup)
        social_links, instagram_handle = await self.extract_social_media(soup)
        location_info = await self.extract_location(soup, roaster_data.get("city"))
        has_subscription = await self.has_subscription(soup)
        has_physical_store = await self.has_physical_store(soup)
        founded_year = await self.extract_founded_year(soup)

        # Try schema.org extraction
        schema_description = DescriptionProcessor.extract_from_schema(html_content)

        # Collect description candidates
        description_sources = {
            'homepage': homepage_description,
            'schema': schema_description,
            'about_pages': [],
            'markdown': None  # No markdown in basic mode
        }

        # Create roaster model
        roaster = {
            "name": name,
            "slug": roaster_data.get("slug") or slugify(name),
            "website_url": website,
            "social_links": social_links,
            "contact_email": contact_info.get("contact_email"),
            "contact_phone": contact_info.get("contact_phone"),
            "city": location_info.get("city"),
            "state": location_info.get("state"),
            "founded_year": founded_year,
            "logo_url": logo_url,
            "has_subscription": has_subscription,
            "has_physical_store": has_physical_store,
            "is_featured": False,  # Default value
            # Non-DB fields
            "instagram_handle": instagram_handle,
            "location": None,
            "_platform": platform_info
        }

        # Try to visit about page using platform-aware paths
        about_data = await self.crawl_about_pages(website, platform_type)

        # Add about page descriptions to candidates
        if about_data.get("about_page_descriptions"):
            description_sources["about_pages"] = about_data.get("about_page_descriptions")

        # Update any missing non-description information from about pages
        for key, value in about_data.items():
            if key != "about_page_descriptions" and (not roaster.get(key)) and value:
                roaster[key] = value

        # Process all descriptions through central processor
        final_description = await DescriptionProcessor.compile_description(
            description_sources, 
            name, 
            deepseek_client=client if DEEPSEEK_API_KEY else None
        )

        # Set the final description
        roaster["description"] = final_description
        
        # Pydantic validation before Save to cache
        try:
            roaster_model = RoasterModel(**roaster)
            save_to_cache(cache_key, roaster_model.dict(), "roasters")

            # Insert into database if client provided
            if self.db_client:
                try:
                    # Create a copy without non-DB fields
                    db_roaster = {k: v for k, v in roaster_model.dict().items() if k not in ["_platform"]}
                    
                    # Use the upsert_roaster method from SupabaseClient
                    roaster_id = self.db_client.upsert_roaster(db_roaster)
                    logger.info(f"Saved roaster {name} to database with ID: {roaster_id}")
                except Exception as e:
                    logger.error(f"Error saving roaster {name} to database: {str(e)}")
        
            # Return the model object, not dict
            return roaster_model
        except Exception as e:
            logger.error(f"Validation failed for {name}: {str(e)}")
            return None

         
    async def process_roasters(self, roasters_data: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Process multiple roasters with rate limiting
        
        Args:
            roasters_data (List[Dict[str, str]]): List of roaster info dicts
            
        Returns:
            List[Dict[str, Any]]: List of enriched roaster data
        """
        results = []
        errors = []
        
        semaphore = asyncio.Semaphore(3)  # limit 3 at a time (tweak as needed)

        async def limited_process(roaster):
            async with semaphore:
                try:
                    logger.info(f"Processing {roaster.get('name', 'Unknown Roaster')}...")
                    result = await self.process_roaster(roaster)
                    if result:
                        results.append(result)
                    else:
                        errors.append({"roaster": roaster.get("name"), "error": "Failed to extract"})
                except Exception as e:
                    errors.append({"roaster": roaster.get("name"), "error": str(e)})

        await asyncio.gather(*(limited_process(r) for r in roasters_data))
                
        # Log summary
        logger.info(f"Successfully processed {len(results)} of {len(roasters_data)} roasters")
        if errors:
            logger.warning(f"Encountered {len(errors)} errors during processing")
            
        return results
        
    async def close(self):
        """Close resources"""
        await self.platform_detector.close()
        
        if self.session and not self.session.closed:
            await self.session.close()