# scrapers/pipeline.py
import logging
import asyncio
import json
from typing import List, Dict, Any, Optional, Tuple
import uuid
from datetime import datetime

from scrapers.discoverers.discovery_manager import DiscoveryManager
from scrapers.roaster_pipeline import RoasterPipeline
from scrapers.extractors.json_css_extractor import JsonCssExtractor
from scrapers.extractors.deepseek_extractor import DeepseekExtractor
from common.utils import load_from_cache, save_to_cache, slugify, setup_logging
from common.models import BeanType, CoffeeModel, ProcessingMethod, RoastLevel, RoasterModel
from db.supabase_client import SupabaseClient
from crawl4ai import BrowserConfig
from config import BATCH_SIZE, REFRESH_CACHE, MAX_CONCURRENT_EXTRACTORS, CRAWL_DELAY

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Unified pipeline that orchestrates the entire scraping process:
    1. Discover roaster metadata
    2. Discover product URLs
    3. Extract detailed product information
    4. Validate and transform data
    5. Upload to database
    """
    
    def __init__(self, 
                db_client: Optional[SupabaseClient] = None, 
                refresh_cache: bool = REFRESH_CACHE,
                max_concurrency: int = MAX_CONCURRENT_EXTRACTORS,
                browser_config: Optional[BrowserConfig] = None):
        """
        Initialize the pipeline.
        
        Args:
            db_client: Optional Supabase client for database operations
            refresh_cache: Whether to bypass cache and refresh data
            max_concurrency: Maximum number of concurrent extraction tasks
            browser_config: Optional browser configuration for crawling
        """
        self.db_client = db_client
        self.refresh_cache = refresh_cache
        self.max_concurrency = max_concurrency
        
        # Initialize browser config if not provided
        if not browser_config:
            self.browser_config = BrowserConfig(
                headless=True,
                ignore_https_errors=True,
                viewport_width=1280,
                viewport_height=800
            )
        else:
            self.browser_config = browser_config
            
        # Initialize components
        self.discovery_manager = DiscoveryManager(db_client=db_client, refresh_cache=refresh_cache)
        self.roaster_pipeline = RoasterPipeline(db_client=db_client, refresh_cache=refresh_cache)
        
        # Initialize extractors
        self.json_css_extractor = JsonCssExtractor(browser_config=self.browser_config)
        self.deepseek_extractor = DeepseekExtractor(browser_config=self.browser_config)
        
        # Track statistics
        self.stats = {
            "roasters_processed": 0,
            "products_discovered": 0,
            "products_extracted": 0,
            "products_enriched": 0,
            "products_uploaded": 0,
            "errors": 0
        }
    
    async def process_roaster_list(self, roaster_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a list of roasters.
        
        Args:
            roaster_list: List of roaster info dicts (at minimum, each dict needs 'name' and 'website_url')
            
        Returns:
            Statistics about the processing
        """
        logger.info(f"Starting to process {len(roaster_list)} roasters")
        
        # Reset statistics
        self.stats = {
            "roasters_processed": 0,
            "products_discovered": 0,
            "products_extracted": 0,
            "products_enriched": 0,
            "products_uploaded": 0,
            "errors": 0
        }
        
        # Process each roaster
        for roaster_info in roaster_list:
            try:
                await self.process_roaster(roaster_info)
                self.stats["roasters_processed"] += 1
                
                # Small delay between roasters to avoid overloading
                await asyncio.sleep(CRAWL_DELAY)
                
            except Exception as e:
                logger.error(f"Error processing roaster {roaster_info.get('name')}: {str(e)}")
                self.stats["errors"] += 1
                continue
        
        logger.info(f"Completed processing {self.stats['roasters_processed']} roasters")
        logger.info(f"Discovered {self.stats['products_discovered']} products")
        logger.info(f"Extracted {self.stats['products_extracted']} products")
        logger.info(f"Enriched {self.stats['products_enriched']} products with DeepSeek")
        logger.info(f"Uploaded {self.stats['products_uploaded']} products to database")
        logger.info(f"Encountered {self.stats['errors']} errors")
        
        return self.stats
    
    async def process_roaster(self, roaster_info: Dict[str, Any]) -> RoasterModel:
        """
        Process a single roaster.
        
        Args:
            roaster_info: Roaster info dict with at least 'name' and 'website_url'
            
        Returns:
            RoasterModel with complete roaster information
        """
        name = roaster_info.get('name')
        website = roaster_info.get('website_url')
        
        if not name or not website:
            raise ValueError(f"Roaster info missing required fields: {roaster_info}")
            
        logger.info(f"Processing roaster: {name} ({website})")
        
        # Step 1: Extract roaster metadata
        roaster = await self.roaster_pipeline.process_roaster(roaster_info)
        logger.info(f"Extracted metadata for {name}")
        
        # Add verification step
        if self.db_client:
            # Force upsert the roaster directly, right before product processing
            try:
                db_roaster = roaster.dict()
                # Remove any fields not in the database schema
                for field in ['_platform', 'location']:
                    if field in db_roaster:
                        db_roaster.pop(field)
                
                # Get the roaster ID
                roaster_id = await self.db_client.upsert_roaster(db_roaster)
                logger.info(f"Forcibly ensured roaster {name} exists in DB with ID: {roaster_id}")
                
                # Wait a moment to ensure database consistency
                await asyncio.sleep(1)
                
                if not roaster_id:
                    logger.error(f"Cannot get roaster ID for {name}, skipping product extraction")
                    return roaster
                
                # Update the roaster object with the confirmed ID
                roaster.id = roaster_id
            except Exception as e:
                logger.error(f"Error ensuring roaster {name} exists in DB: {str(e)}")
                return roaster
                
        # Step 2: Discover products
        products = await self.discovery_manager.discover_products(roaster.dict())
        logger.info(f"Discovered {len(products)} products for {name}")
        self.stats["products_discovered"] += len(products)
        
        # Step 3: Extract detailed product information
        if products:
            extracted_products = await self._extract_product_details(products, roaster)
            logger.info(f"Extracted details for {len(extracted_products)} products from {name}")
            
            # Step 4: Upload to database
            if self.db_client and extracted_products:
                uploaded_count = await self._upload_products(extracted_products, roaster)
                logger.info(f"Uploaded {uploaded_count} products from {name} to database")
                self.stats["products_uploaded"] += uploaded_count
        
        return roaster
    
    async def _extract_product_details(self, 
                                     products: List[Dict[str, Any]], 
                                     roaster: RoasterModel) -> List[CoffeeModel]:
        """
        Extract detailed information for discovered products.
        
        Args:
            products: List of discovered product dicts
            roaster: RoasterModel for the roaster
            
        Returns:
            List of CoffeeModel instances with complete product information
        """
        # Split products into batches to control concurrency
        batches = [products[i:i + BATCH_SIZE] for i in range(0, len(products), BATCH_SIZE)]
        extracted_products = []
        
        for batch in batches:
            # Create a semaphore to limit concurrency
            semaphore = asyncio.Semaphore(self.max_concurrency)
            
            # Create extraction tasks
            tasks = []
            for product in batch:
                task = self._extract_single_product(product, roaster, semaphore)
                tasks.append(task)
                
            # Execute tasks concurrently with controlled parallelism
            batch_results = await asyncio.gather(*tasks)
            
            # Filter out None results and add to extracted products
            batch_extracted = [p for p in batch_results if p]
            extracted_products.extend(batch_extracted)
            
            self.stats["products_extracted"] += len(batch_extracted)
            
            # Count DeepSeek enriched products
            self.stats["products_enriched"] += sum(1 for p in batch_extracted if p.deepseek_enriched)


            # Small delay between batches
            await asyncio.sleep(CRAWL_DELAY)
            
        return extracted_products
    
    async def _extract_single_product(self, 
                                    product: Dict[str, Any], 
                                    roaster: RoasterModel,
                                    semaphore: asyncio.Semaphore) -> Optional[CoffeeModel]:
        """
        Extract detailed information for a single product.
        
        Args:
            product: Product dict with basic information
            roaster: RoasterModel for the roaster
            semaphore: Semaphore to control concurrency
            
        Returns:
            CoffeeModel if extraction successful, None otherwise
        """
        async with semaphore:
            try:
                # Check if we have a cached version
                cache_key = f"product_{product.get('slug')}"
                if not self.refresh_cache:
                    cached_data = load_from_cache(cache_key, "extracted_products")
                    if cached_data:
                        logger.info(f"Using cached data for {product.get('name')}")
                        return CoffeeModel.parse_obj(cached_data)
                
                # Step 1: Extract with JSON-CSS extractor
                enhanced_product = await self.json_css_extractor.extract(product)
                
                # Make sure roaster info is included
                enhanced_product["roaster_id"] = str(roaster.id)
                
                # Step 2: Check if we need DeepSeek enrichment
                missing_attrs = 0
                for attr in ["roast_level", "bean_type", "processing_method", "flavor_profiles"]:
                    if attr not in enhanced_product or enhanced_product.get(attr) == "unknown":
                        missing_attrs += 1
                
                if missing_attrs >= 2:
                    logger.info(f"Product {product.get('name')} needs DeepSeek enrichment")
                    enhanced_product = await self.deepseek_extractor.extract(enhanced_product)
                
                # Step 3: Convert to CoffeeModel
                coffee_model = self._convert_to_coffee_model(enhanced_product, roaster)
                
                # Step 4: Cache the result
                save_to_cache(cache_key, coffee_model.dict(), "extracted_products")
                
                return coffee_model
                
            except Exception as e:
                logger.error(f"Error extracting product {product.get('name')}: {str(e)}")
                return None
            
            # Rate limiting
            finally:
                await asyncio.sleep(CRAWL_DELAY)
    
    def _convert_to_coffee_model(self, 
                               product: Dict[str, Any], 
                               roaster: RoasterModel) -> CoffeeModel:
        """
        Convert extracted product dict to CoffeeModel.
        
        Args:
            product: Extracted product dict
            roaster: RoasterModel for the roaster
            
        Returns:
            CoffeeModel instance
        """
        # Extract and normalize prices
        prices = {}
        for key, value in product.items():
            if key.startswith("price_") and isinstance(value, (int, float)):
                size_str = key.replace("price_", "")
                if size_str.endswith("g"):
                    size_str = size_str[:-1]  # Remove trailing 'g'
                
                try:
                    size_grams = int(size_str)
                    prices[size_grams] = float(value)
                except (ValueError, TypeError):
                    continue
        
        # Create CoffeeModel
        coffee_dict = {
             "id": product.get("id", str(uuid.uuid4())),
            "roaster_id": product.get("roaster_id", str(roaster.id)),
            "name": product.get("name", "Unknown Coffee"),
            "slug": product.get("slug", slugify(product.get("name", "unknown"))),
            "description": product.get("description", None),
            # Map to valid database values
            "roast_level": self.map_to_valid_roast_level(product.get("roast_level", RoastLevel.UNKNOWN)),
            "bean_type": self.map_to_valid_bean_type(product.get("bean_type", BeanType.UNKNOWN)),
            "processing_method": self.map_to_valid_processing_method(product.get("processing_method", ProcessingMethod.UNKNOWN)),
            "image_url": product.get("image_url", None),
            "direct_buy_url": product.get("direct_buy_url", None),
            "region_name": product.get("region_name", None),
            "is_seasonal": product.get("is_seasonal", False),
            "is_available": product.get("is_available", True),
            "is_featured": product.get("is_featured", False),
            "is_single_origin": not product.get("is_blend", False),
            "tags": product.get("tags", []),
            "deepseek_enriched": product.get("deepseek_enriched", False),
            "flavor_profiles": product.get("flavor_profiles", []),
            "brew_methods": product.get("brew_methods", []),
            "prices": prices,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        return CoffeeModel.parse_obj(coffee_dict)
    
    async def _upload_products(self, 
                         products: List[CoffeeModel], 
                         roaster: RoasterModel) -> int:
        """
        Upload products to database.
        
        Args:
            products: List of CoffeeModel instances
            roaster: RoasterModel for the roaster
            
        Returns:
            Number of successfully uploaded products
        """
        if not self.db_client:
            logger.warning("No database client provided, skipping upload")
            return 0
            
        if not products:
            logger.warning("No products to upload")
            return 0
            
        logger.info(f"Uploading {len(products)} products for {roaster.name}")
        
        # Split into batches
        batches = [products[i:i + BATCH_SIZE] for i in range(0, len(products), BATCH_SIZE)]
        uploaded_count = 0
        
        for batch in batches:
            try:
                # Convert each product to a dict
                batch_dicts = []
                for product in batch:
                    # Create a copy of the dict without the non-DB fields
                    product_dict = product.dict()
                    # Remove fields that are handled separately
                    for field in ['flavor_profiles', 'brew_methods', 'prices', 'external_links', 'region_name']:
                        if field in product_dict:
                            product_dict.pop(field)
                    batch_dicts.append(product_dict)
                
                # Upsert to database
                result = await self.db_client.upsert_coffees(batch_dicts)
                
                if result:  # If we have coffee IDs back
                    for i, coffee_id in enumerate(result):
                        # Process prices if available
                        coffee_model = batch[i]
                        if hasattr(coffee_model, 'prices') and coffee_model.prices:
                            await self.db_client.upsert_coffee_prices(coffee_id, coffee_model.prices)
                            
                        # Process flavor profiles if available
                        if hasattr(coffee_model, 'flavor_profiles') and coffee_model.flavor_profiles:
                            for flavor in coffee_model.flavor_profiles:
                                await self.db_client.link_flavor_profile(coffee_id, flavor)
                                
                        # Process brew methods if available
                        if hasattr(coffee_model, 'brew_methods') and coffee_model.brew_methods:
                            for method in coffee_model.brew_methods:
                                await self.db_client.link_brew_method(coffee_id, method)
                    
                    uploaded_count += len(batch)
                    logger.info(f"Successfully uploaded batch of {len(batch)} products")
                else:
                    logger.error(f"Failed to upload batch: No IDs returned")
                
            except Exception as e:
                logger.error(f"Error uploading batch: {str(e)}")
                
            # Small delay between batches
            await asyncio.sleep(CRAWL_DELAY)
            
        return uploaded_count
    
    async def close(self):
        """Close all resources"""
        await self.discovery_manager.close()
        await self.roaster_pipeline.close()
    
    def map_to_valid_roast_level(self, roast_level: str) -> str:
        """Map any roast level to a valid database value"""
        # Valid values from your updated enum
        valid_values = ['light', 'light-medium', 'medium', 'medium-dark', 'dark', 
                    'city', 'city-plus', 'full-city', 'french', 'italian', 
                    'cinnamon', 'filter', 'espresso', 'omniroast']
        
        if roast_level and roast_level.lower() in valid_values:
            return roast_level.lower()
            
        # Map similar values to valid ones
        mappings = {
            'unknown': 'unknown',  # No longer defaulting to medium
            'light roast': 'light',
            'medium roast': 'medium',
            'dark roast': 'dark',
            'blonde': 'light',
            'breakfast': 'light',
            'vienna': 'medium-dark',
            'continental': 'dark',
            'new orleans': 'dark',
            'spanish': 'dark',
            'black': 'dark',
            'half city': 'light-medium',
            'american': 'medium',
            'high': 'light',
            'medium high': 'light-medium',
            'medium low': 'medium-dark',
            'low': 'dark'
        }
        
        # Check if any word in the input matches our mappings (for multi-word descriptions)
        if roast_level:
            lower_roast = roast_level.lower()
            for key, value in mappings.items():
                if key in lower_roast:
                    return value
        
        return 'unknown'  # Default to unknown if no match

    def map_to_valid_bean_type(self, bean_type: str) -> str:
        """Map any bean type to a valid database value"""
        # Valid values from your updated enum
        valid_values = ['arabica', 'robusta', 'liberica', 'blend', 
                    'mixed-arabica', 'arabica-robusta']
        
        if bean_type and bean_type.lower() in valid_values:
            return bean_type.lower()
            
        # Map similar values to valid ones
        mappings = {
            'unknown': 'unknown',  # No longer defaulting to arabica
            '100% arabica': 'arabica',
            'arabica beans': 'arabica',
            'robusta beans': 'robusta',
            'espresso blend': 'blend',
            'mixed origin': 'blend',
            'coffee blend': 'blend',
            'specialty blend': 'blend',
            'single origin': 'arabica',  # Most single origins are arabica
            'heirloom': 'arabica',
            'bourbon': 'arabica',
            'typica': 'arabica',
            'canephora': 'robusta',
            'arabicas': 'arabica',
            'arabica/robusta': 'arabica-robusta',
            'arabica & robusta': 'arabica-robusta',
            'arabica and robusta': 'arabica-robusta'
        }
        
        # Check if any word in the input matches our mappings
        if bean_type:
            lower_bean = bean_type.lower()
            for key, value in mappings.items():
                if key in lower_bean:
                    return value
        
        return 'unknown'  # Default to unknown if no match

    def map_to_valid_processing_method(self, processing_method: str) -> str:
        """Map any processing method to a valid database value"""
        # Valid values from your updated enum
        valid_values = ['washed', 'natural', 'honey', 'pulped-natural', 
                    'anaerobic', 'monsooned', 'wet-hulled', 
                    'carbonic-maceration', 'double-fermented']
        
        if processing_method and processing_method.lower() in valid_values:
            return processing_method.lower()
            
        # Map similar values to valid ones
        mappings = {
            'unknown': 'unknown',  # No longer defaulting to washed
            'wet': 'washed',
            'fully washed': 'washed',
            'dry': 'natural',
            'unwashed': 'natural',
            'sun-dried': 'natural',
            'semi-washed': 'honey',
            'pulped': 'pulped-natural',
            'fermented': 'washed',
            'monsoon': 'monsooned',
            'monsoon malabar': 'monsooned',
            'monsooned malabar': 'monsooned',
            'malabar': 'monsooned',
            'giling basah': 'wet-hulled',
            'wet-process': 'washed',
            'dry-process': 'natural',
            'black honey': 'honey',
            'red honey': 'honey',
            'yellow honey': 'honey',
            'white honey': 'honey',
            'carbonic': 'carbonic-maceration',
            'double wash': 'double-fermented',
            'eco-pulped': 'pulped-natural'
        }
        
        # Check if any word in the input matches our mappings
        if processing_method:
            lower_process = processing_method.lower()
            for key, value in mappings.items():
                if key in lower_process:
                    return value
        
        return 'unknown'  # Default to unknown if no match