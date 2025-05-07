# common/utils.py
from datetime import datetime
import re
import unicodedata
from typing import Optional, Dict, Any, Union
import json
import os
import logging
import time
from config import CACHE_DIR, CACHE_ENABLED, CACHE_EXPIRY

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        try:
            from yarl import URL
            if isinstance(obj, URL):
                return str(obj)
        except ImportError:
            pass
        return super().default(obj)

logger = logging.getLogger(__name__)

def setup_logging(level=logging.INFO, log_file=None):
    """Setup logging with proper formatting"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Get current timestamp for log filename if none provided
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'logs/test_run_{timestamp}.log'
    
    handlers = [logging.StreamHandler()]  # Always log to console
    
    # Add file handler if log_file is provided
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    return log_file

def slugify(s: str) -> str:
    """
    Convert a string to a URL-friendly slug.
    Example: "Blue Tokai Coffee Roasters" -> "blue-tokai-coffee-roasters"
    """
    # Convert to lowercase and replace spaces with hyphens
    s = s.lower().replace(' ', '-')
    
    # Remove accents
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    
    # Remove non-word characters (except hyphens)
    s = re.sub(r'[^\w\-]', '', s)
    
    # Remove consecutive hyphens
    s = re.sub(r'-+', '-', s)
    
    # Remove leading and trailing hyphens
    return s.strip('-')

def extract_price(text: str) -> Optional[float]:
    """
    Extract price from text string. Works with both ₹ and Rs formats.
    Examples: "₹550", "Rs. 550", "Rs.550/-", "550/-"
    """
    # Remove commas
    text = text.replace(',', '')
    
    # Try to find price in different formats
    patterns = [
        r'₹\s*(\d+\.?\d*)',           # ₹550 or ₹ 550
        r'Rs\.?\s*(\d+\.?\d*)',        # Rs.550 or Rs. 550
        r'(\d+\.?\d*)/-',              # 550/-
        r'INR\s*(\d+\.?\d*)',          # INR 550
        r'Price:\s*(?:₹|Rs\.?|INR)?\s*(\d+\.?\d*)' # Price: ₹550
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1))
    
    return None

def extract_weight(text: str) -> Optional[int]:
    """
    Extract weight in grams from text string.
    Examples: "250g", "250 grams", "250 g", "0.25kg"
    """
    # Check for grams (g, gram, grams)
    gram_patterns = [
        r'(\d+)\s*(?:g|gram|grams|gm)\b',  # 250g, 250 gram, 250 grams
    ]
    
    for pattern in gram_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    
    # Check for kilograms (kg, kgs)
    kg_patterns = [
        r'(\d+(?:\.\d+)?)\s*(?:kg|kgs|kilograms|kilogram)\b',  # 0.25kg, 0.25 kg
    ]
    
    for pattern in kg_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Convert to grams
            return int(float(match.group(1)) * 1000)
    
    return None

def get_cache_path(cache_key: str, subdir: Optional[str] = None) -> str:
    """Get the file path for a cache entry"""
    base_dir = CACHE_DIR
    if subdir:
        base_dir = os.path.join(base_dir, subdir)
        os.makedirs(base_dir, exist_ok=True)
    
    # Make sure the cache key is filesystem-safe
    safe_key = slugify(cache_key)
    return os.path.join(base_dir, f"{safe_key}.json")

def load_from_cache(cache_key: str, subdir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load data from cache if it exists and is not expired"""
    if not CACHE_ENABLED:
        return None
        
    cache_path = get_cache_path(cache_key, subdir)
    
    if not os.path.exists(cache_path):
        return None
        
    try:
        # Check if cache is 
        if time.time() - os.path.getmtime(cache_path) > CACHE_EXPIRY:
            logger.debug(f"Cache expired for {cache_key}")
            return None
            
        with open(cache_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading cache for {cache_key}: {str(e)}")
        return None

def save_to_cache(cache_key: str, data: Union[Dict[str, Any], list], subdir: Optional[str] = None) -> bool:
    """Save data to cache"""
    if not CACHE_ENABLED:
        return False
        
    cache_path = get_cache_path(cache_key, subdir)
    
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)  # Use custom encoder
        return True
    except Exception as e:
        logger.error(f"Error saving cache for {cache_key}: {str(e)}")
        return False
