#!/usr/bin/env python3
"""
Test script to verify project setup is working correctly.
Tests importing all major components and runs a basic check.
"""

import os
import logging
import traceback
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Test project setup by importing and initializing components"""
    logger.info("Testing project setup...")
    
    # Check environment variables
    logger.info("Checking environment variables...")
    load_dotenv()
    required_vars = [
        "SUPABASE_URL",
        "SUPABASE_KEY"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.warning(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.warning("Please update your .env file with these variables.")
    else:
        logger.info("Environment variables check passed!")
    
    # Test importing modules
    try:
        logger.info("Testing imports...")
        
        # Config
        from config import SUPABASE_URL, SUPABASE_KEY, USER_AGENT
        logger.info(f"Config loaded. USER_AGENT: {USER_AGENT}")
        
        # Models
        from common.models import RoasterModel, CoffeeModel
        logger.info("Models imported successfully")
        
        # Utilities
        from common.utils import slugify, extract_price, extract_weight
        test_slug = slugify("Blue Tokai Coffee Roasters")
        logger.info(f"Utility functions working. Slug test: {test_slug}")
        
        # Supabase client
        try:
            from db.supabase_client import SupabaseClient
            if not missing_vars:
                db = SupabaseClient()
                logger.info("Supabase client initialized")
            else:
                logger.warning("Skipping Supabase client test due to missing environment variables")
        except Exception as e:
            logger.error(f"Error initializing Supabase client: {str(e)}")
            traceback.print_exc()
        
        # Platform detector
        from scrapers.platform_detector import PlatformDetector
        logger.info("Platform detector imported successfully")
        
        logger.info("All imports successful!")
        
    except ImportError as e:
        logger.error(f"Import error: {str(e)}")
        logger.error("Project setup may be incorrect. Check your folder structure and files.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("Setup test completed successfully! Project structure looks good.")
    else:
        logger.error("Setup test failed. Please fix the issues before continuing.")