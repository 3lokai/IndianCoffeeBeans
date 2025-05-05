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
# Add as a class constant in RoasterPipeline
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
    'goa': 'Goa',
    'west bengal': 'West Bengal',
    'kolkata': 'West Bengal',
    'calcutta': 'West Bengal',
    'punjab': 'Punjab',
    'chandigarh': 'Chandigarh',
    'uttar pradesh': 'Uttar Pradesh',
    'lucknow': 'Uttar Pradesh',
    'rajasthan': 'Rajasthan',
    'jaipur': 'Rajasthan',
    'gujarat': 'Gujarat',
    'ahmedabad': 'Gujarat',
    'assam': 'Assam',
    'guwahati': 'Assam',
    'odisha': 'Odisha',
    'bhubaneswar': 'Odisha'
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
        self.crawl4ai_loaded = False
        self.session = None  # Properly initialize session
        self._try_load_crawl4ai()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _init_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )

    async def fetch_page(self, url: str) -> Optional[str]:
        await self._init_session()
        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    if attempt == 0:
                        logger.warning(f"[{response.status}] Failed to fetch {url}")
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"[Attempt {attempt+1}] Error fetching {url}: {e}")
                if attempt == 0 and "SSL" in str(e) and url.startswith("https://"):
                    fallback_url = url.replace("https://", "http://")
                    try:
                        async with self.session.get(fallback_url) as response:
                            if response.status == 200:
                                return await response.text()
                    except Exception:
                        pass
            await asyncio.sleep(min(CRAWL_DELAY * (2 ** attempt), 10))
        logger.error(f"All attempts failed for {url}")
        return None

    async def extract_logo(self, soup: BeautifulSoup, base_url: str, platform: str) -> Optional[str]:
        """Extract logo URL from homepage with improved detection"""
        logo_url = None

        # Method 1: Direct image filename check
        for img in soup.find_all('img'):
            src = img.get('src', '')
            if 'logo' in src.lower():
                logo_url = src
                break

        # Method 2: Header image fallback
        if not logo_url:
            header = soup.find('header')
            if header:
                header_img = header.find('img')
                if header_img and header_img.get('src'):
                    logo_url = header_img['src']

        # Method 3: Platform-specific selectors
        if not logo_url:
            if platform == "shopify":
                selectors = [
                    ".site-header__logo img", ".header__logo img", ".header-logo img",
                    ".header__heading-logo img", ".logo-image", ".header-item--logo img",
                    ".site-header__logo-image", ".header-wrapper .header-item img"
                ]
            elif platform == "woocommerce":
                selectors = [
                    ".custom-logo", ".site-logo img", ".wp-block-site-logo img",
                    "#logo img", ".logo img", ".storefront-site-branding img"
                ]
            else:
                selectors = []
            for selector in selectors:
                try:
                    logo_img = soup.select_one(selector)
                    if logo_img and logo_img.get('src'):
                        logo_url = logo_img['src']
                        break
                except Exception:
                    continue

        # Method 4: Generic selectors (fallback)
        if not logo_url:
            generic_selectors = [
                ".site-logo img", ".logo-img", "#logo img", "img.logo", 
                "[class*='logo'] img", ".navbar-brand img", ".brand img", 
                ".header-logo img", "a[class*='logo'] img", "h1.logo a img",
                ".site-header__logo img", ".header__heading-logo img"
            ]
            for selector in generic_selectors:
                try:
                    logo_img = soup.select_one(selector)
                    if logo_img and logo_img.get('src'):
                        logo_url = logo_img['src']
                        break
                except Exception:
                    continue

        # Method 5: OpenGraph image
        if not logo_url:
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                logo_url = og["content"]

        # Method 6: Favicon fallback
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

    async def extract_all_metadata(self, soup: BeautifulSoup, base_url: str, city_from_input: Optional[str] = None, platform: str = "unknown") -> Dict[str, Any]:
        """Extract all metadata from a BeautifulSoup object in a single pass"""
        results = {}
        results["logo_url"] = await self.extract_logo(soup, base_url, platform)
        results["description"] = self._extract_description(soup)
        results.update(self._extract_contact_info(soup, platform))
        results.update(self._extract_location(soup, city_from_input))
        results.update(self._extract_social_media(soup, platform))
        results["has_subscription"] = self._check_subscription(soup)
        results["has_physical_store"] = self._check_physical_store(soup)
        results["founded_year"] = self._extract_founded_year(soup)
        return results

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"]
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            return og["content"]
        return None

    def _extract_contact_info(self, soup: BeautifulSoup, platform: str) -> Dict[str, Optional[str]]:
        email_regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        phone_regex = r'(?:\+91[-\s]?)?[6-9]\d{9}'
        contact_email = None
        contact_phone = None
        # Platform-specific selectors
        contact_selectors = []
        if platform == "shopify":
            contact_selectors = [
                ".footer__contact-info", ".site-footer__newsletter-block",
                ".address", ".contact-information", ".footer-contact", ".contact-info-area"
            ]
        elif platform == "woocommerce":
            contact_selectors = [
                ".footer__contact-info", ".site-footer__newsletter-block",
                ".address", ".contact-information", ".footer-contact", ".contact-info-area"
            ]
        # Try platform-specific contact sections
        for selector in contact_selectors:
            contact_section = soup.select_one(selector)
            if contact_section:
                mailto = contact_section.find('a', href=lambda h: h and 'mailto:' in h)
                if mailto:
                    contact_email = mailto['href'].replace('mailto:', '').strip()
                tel = contact_section.find('a', href=lambda h: h and 'tel:' in h.lower())
                if tel:
                    contact_phone = tel['href'].replace('tel:', '').strip()
        # Fallback: footer
        if not contact_email or not contact_phone:
            footer = soup.find("footer") or soup.select_one(".site-footer, #footer, .footer")
            if footer:
                if not contact_email:
                    mailto = footer.find('a', href=lambda h: h and 'mailto:' in h)
                    if mailto:
                        contact_email = mailto['href'].replace('mailto:', '').strip()
                    else:
                        for link in footer.find_all("a", href=True):
                            match = re.search(email_regex, link.text)
                            if match:
                                contact_email = match.group(0)
                                break
                if not contact_phone:
                    tel = footer.find('a', href=lambda h: h and 'tel:' in h.lower())
                    if tel:
                        contact_phone = tel['href'].replace('tel:', '').strip()
                    else:
                        matches = re.findall(phone_regex, str(footer))
                        if matches:
                            contact_phone = matches[0]
        # Fallback: anywhere in page
        if not contact_email:
            mailto = soup.find('a', href=lambda h: h and 'mailto:' in h)
            if mailto:
                contact_email = mailto['href'].replace('mailto:', '').strip()
            else:
                links = soup.find_all("a", href=True)
                for link in links:
                    match = re.search(email_regex, link.text)
                    if match:
                        contact_email = match.group(0)
                        break
        if not contact_phone:
            tel = soup.find('a', href=lambda h: h and 'tel:' in h.lower())
            if tel:
                contact_phone = tel['href'].replace('tel:', '').strip()
            else:
                matches = re.findall(phone_regex, str(soup))
                if matches:
                    contact_phone = matches[0]
        return {"contact_email": contact_email, "contact_phone": contact_phone}

    def _extract_location(self, soup: BeautifulSoup, city_from_input: Optional[str]) -> Dict[str, Optional[str]]:
        city = city_from_input
        state = None
        selectors = [".address", ".location", ".contact-address", "footer address", ".site-footer__contact-info", ".footer-contact"]
        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                text = node.get_text(strip=True)
                parts = re.split(r'[,-]', text)
                if len(parts) >= 2:
                    if not city:
                        city = parts[0].strip()
                    if not state:
                        state = STATE_MAPPING.get(parts[1].strip().lower(), None)
        return {"city": city, "state": state}

    def _extract_social_media(self, soup: BeautifulSoup, platform: str) -> Dict[str, Any]:
        platforms = ["instagram", "facebook", "twitter", "linkedin", "youtube"]
        social_links = []
        # Platform-specific selectors
        shopify_social_selectors = [
            ".footer-social a", ".site-footer__social-icons a", ".header__social-icons a",
            ".social-links a", ".social-sharing a", "[data-social-icons-wrapper] a"
        ]
        woo_social_selectors = [
            ".social-icons a", ".social-navigation a", ".site-social-links a",
            ".social-media-icons a", ".widget_social_widget a"
        ]
        all_selectors = []
        if platform == "shopify":
            all_selectors = shopify_social_selectors
        elif platform == "woocommerce":
            all_selectors = woo_social_selectors
        else:
            all_selectors = shopify_social_selectors + woo_social_selectors
        # First check footer
        footer = soup.find("footer") or soup.select_one(".site-footer, #footer, .footer")
        if footer:
            for selector in all_selectors:
                links = footer.select(selector)
                for link in links:
                    href = link.get("href", "")
                    if any(p in href.lower() for p in platforms):
                        social_links.append(href)
        # Fallback: try selectors in whole page
        if not social_links:
            for selector in all_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get("href", "")
                    if any(p in href.lower() for p in platforms):
                        social_links.append(href)
        # Last resort: check all links
        if not social_links:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if any(p in href.lower() for p in platforms):
                    social_links.append(href)
        insta = next((re.search(r'instagram\.com/([^/?]+)', url).group(1) for url in social_links if 'instagram.com' in url and re.search(r'instagram\.com/([^/?]+)', url)), None)
        return {"social_links": social_links, "instagram_handle": insta}

    def _check_subscription(self, soup: BeautifulSoup) -> bool:
        html_str = str(soup)
        return any(kw in html_str.lower() for kw in ["subscription", "subscribe", "monthly"])

    def _check_physical_store(self, soup: BeautifulSoup) -> bool:
        html_str = str(soup)
        return any(kw in html_str.lower() for kw in ["visit us", "our location", "our cafe"])

    def _extract_founded_year(self, soup: BeautifulSoup) -> Optional[int]:
        html_str = str(soup)
        patterns = [r'founded\s+in\s+(\d{4})', r'established\s+in\s+(\d{4})', r'since\s+(\d{4})', r'est\.\s*(\d{4})']
        for pattern in patterns:
            match = re.search(pattern, html_str, re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1900 <= year <= CURRENT_YEAR:
                    return year
        return None

    async def _get_crawl4ai_markdown(self, url: str) -> Optional[str]:
        """Get markdown content from Crawl4AI"""
        try:
            browser_config = self.BrowserConfig(headless=True, verbose=False)
            run_config = self.CrawlerRunConfig(
                cache_mode=self.CacheMode.ENABLED,
                markdown_generator=self.DefaultMarkdownGenerator(
                    content_filter=PruningContentFilter(threshold=0.5, threshold_type="fixed")
                )
            )
            async with self.AsyncWebCrawler(config=browser_config) as crawler:
                try:
                    result = await crawler.arun(url=url, config=run_config)
                except Exception as e:
                    if "ERR_SSL" in str(e) and url.startswith("https://"):
                        alt_url = url.replace("https://", "http://")
                        result = await crawler.arun(url=alt_url, config=run_config)
                    else:
                        raise
                if result and result.success:
                    return (result.markdown.fit_markdown 
                            if hasattr(result.markdown, 'fit_markdown') 
                            else str(result.markdown))
                return None
        except Exception as e:
            logger.warning(f"Failed to generate markdown: {str(e)}")
            return None

    async def _detect_platform(self, url: str, html_content: Optional[str] = None) -> dict:
        """Detect platform with proper context management"""
        async with PlatformDetector() as detector:
            if html_content:
                return await detector.detect(url, html_content)
            return await detector.detect(url)

    async def get_platform_specific_about_paths(self, platform: str) -> list:
        """Return a list of about-page suffixes for the given platform."""
        if platform == "shopify":
            return ["/pages/about", "/pages/our-story", "/about", "/about-us"]
        elif platform == "woocommerce":
            return ["/about", "/about-us", "/our-story"]
        elif platform == "wordpress":
            return ["/about", "/about-us", "/our-story"]
        else:
            return ["/about", "/about-us", "/our-story"]

    async def crawl_about_pages(self, base_url: str, platform: str = "unknown") -> dict:
        """Optimized: fetch and process about pages in parallel, merging results"""
        suffixes = await self.get_platform_specific_about_paths(platform)
        if platform not in ["shopify", "woocommerce", "wordpress"]:
            suffixes.extend(['/philosophy', '/values', '/mission'])
        suffixes = suffixes[:5]  # Limit to top 5
        results = {"about_page_descriptions": []}
        urls = [base_url.rstrip('/') + suffix for suffix in suffixes]
        htmls = await asyncio.gather(*(self.fetch_page(url) for url in urls))
        valid_pages = [(url, html) for url, html in zip(urls, htmls) if html]
        async def process_page(url, html):
            try:
                soup = BeautifulSoup(html, "html.parser")
                page_data = await self.extract_all_metadata(soup, base_url, platform=platform)
                if page_data.get("description"):
                    results["about_page_descriptions"].append(page_data["description"])
                return {k: v for k, v in page_data.items() if k != "description" and v is not None}
            except Exception as e:
                logger.warning(f"Error processing about page {url}: {e}")
                return {}
        page_results = await asyncio.gather(
            *(process_page(url, html) for url, html in valid_pages)
        )
        for page_data in page_results:
            for key, value in page_data.items():
                if key not in results or not results[key]:
                    results[key] = value
        return results

    async def unified_process_roaster(self, roaster_data: Dict[str, str]) -> Optional[RoasterModel]:
        name = roaster_data.get("name")
        website = roaster_data.get("website_url")
        if not name or not website:
            return None
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
        cache_key = f"roaster_{slugify(name)}"
        if not self.refresh_cache:
            cached_data = load_from_cache(cache_key, "roasters")
            if cached_data:
                return RoasterModel(**cached_data)
        homepage_html = await self.fetch_page(website)
        if not homepage_html:
            return None
        platform_info = await self._detect_platform(website, homepage_html)
        platform_type = platform_info.get("platform", "unknown")
        soup = BeautifulSoup(homepage_html, "html.parser")
        metadata = await self.extract_all_metadata(soup, website, roaster_data.get("city"), platform=platform_type)
        markdown_text = None
        if self.crawl4ai_loaded:
            try:
                markdown_result = await self._get_crawl4ai_markdown(website)
                if markdown_result:
                    markdown_text = markdown_result[:3000]
            except Exception as e:
                logger.warning(f"Crawl4AI markdown generation failed: {e}")
        roaster = {
            "name": name,
            "slug": roaster_data.get("slug") or slugify(name),
            "website_url": website,
            "description": None,
            **metadata,
            "_platform": platform_info
        }
        required_fields = ["description", "social_links", "contact_email"]
        if any(not roaster.get(field) for field in required_fields):
            about_data = await self.crawl_about_pages(website, platform_type)
            if about_data.get("about_page_descriptions"):
                about_descriptions = about_data.pop("about_page_descriptions")
            else:
                about_descriptions = []
            for key, value in about_data.items():
                if not roaster.get(key) and value:
                    roaster[key] = value
        else:
            about_descriptions = []
        description_sources = {
            'homepage': metadata.get("description"),
            'schema': DescriptionProcessor.extract_from_schema(homepage_html),
            'about_pages': about_descriptions,
            'markdown': markdown_text
        }
        final_description = await DescriptionProcessor.compile_description(
            description_sources,
            name,
            deepseek_client=client if DEEPSEEK_API_KEY else None
        )
        roaster["description"] = final_description
        try:
            roaster_model = RoasterModel(**roaster)
            save_to_cache(cache_key, roaster_model.dict(), "roasters")
            if self.db_client:
                db_roaster = {k: v for k, v in roaster_model.dict().items() if k not in ["_platform"]}
                roaster_id = self.db_client.upsert_roaster(db_roaster)
                if roaster_id:
                    roaster_model.id = roaster_id
            return roaster_model
        except Exception as e:
            logger.error(f"Validation error for {name}: {str(e)}")
            return None

    async def process_roasters(self, roasters_data: List[Dict[str, str]], concurrency: int = 3) -> List[RoasterModel]:
        results = []
        semaphore = asyncio.Semaphore(concurrency)
        async def process_with_semaphore(roaster):
            async with semaphore:
                return await self.unified_process_roaster(roaster)
        processed = await asyncio.gather(
            *(process_with_semaphore(r) for r in roasters_data),
            return_exceptions=True
        )
        for i, result in enumerate(processed):
            if isinstance(result, Exception):
                logger.error(f"Error processing roaster {roasters_data[i].get('name')}: {str(result)}")
            elif result:
                results.append(result)
        logger.info(f"Processed {len(results)}/{len(roasters_data)} roasters successfully")
        return results