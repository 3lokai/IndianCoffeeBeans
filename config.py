# config.py
from dotenv import load_dotenv
import os

# Construct the full path to the .env file
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# DeepSeek API configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# Crawler settings
USER_AGENT = "IndianCoffeeBeans.com Scraper/1.0"
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
CRAWL_DELAY = 2  # seconds

# Cache settings
CACHE_ENABLED = True
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "cache")
CACHE_EXPIRY = 86400  # 24 hours in seconds
REFRESH_CACHE = os.getenv("REFRESH_CACHE", "false").lower() == "true"

# Output settings
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "output")
CSV_EXPORT_PATH = OUTPUT_DIR  # For compatibility with our implementation

# Pipeline settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))  # Number of items to process in a batch
MAX_CONCURRENT_EXTRACTORS = int(os.getenv("MAX_CONCURRENT_EXTRACTORS", "3"))

# Platform-specific settings
PLATFORM_SPECIFIC = {
    "shopify": {
        "api_endpoints": [
            "/products.json",
            "/collections/all/products.json"
        ],
        "product_urls": [
            "/collections/all",
            "/collections/coffee",
            "/products"
        ]
    },
    "woocommerce": {
        "api_endpoints": [
            "/wp-json/wc/v3/products",
            "/wp-json/wc/v2/products"
        ],
        "product_urls": [
            "/shop",
            "/product-category/coffee",
            "/product"
        ]
    },
    "generic": {
        "product_urls": [
            "/products",
            "/coffee",
            "/shop",
            "/beans"
        ]
    }
}

# Ensure directories exist
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Create subdirectories for better organization
os.makedirs(os.path.join(CACHE_DIR, "products"), exist_ok=True)
os.makedirs(os.path.join(CACHE_DIR, "roasters"), exist_ok=True)
os.makedirs(os.path.join(CACHE_DIR, "extracted_products"), exist_ok=True)

# Check for required environment variables
def check_env_vars():
    """Check for required environment variables and print warnings"""
    required_vars = {
        "database": ["SUPABASE_URL", "SUPABASE_KEY"],
        "deepseek": ["DEEPSEEK_API_KEY"]
    }
    
    missing = {}
    
    for category, vars in required_vars.items():
        missing_in_category = [var for var in vars if not os.getenv(var)]
        if missing_in_category:
            missing[category] = missing_in_category
    
    if missing:
        print("\n[WARNING] Missing environment variables:")
        for category, vars in missing.items():
            print(f"  {category.upper()}: {', '.join(vars)}")
        print("Some functionality may be limited.\n")
    
    return missing

# Run environment variable check
missing_vars = check_env_vars()