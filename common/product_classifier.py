# common/product_classifier.py
import logging
import re
from typing import Optional, List

logger = logging.getLogger(__name__)

# --- Constants for Classification ---

# Keywords strongly indicating it IS a coffee product
# (No longer strictly needed for the new logic, but kept for reference/future use)
COFFEE_KEYWORDS = [
    'coffee', 'bean', 'roast', 'brew', 'espresso', 'arabica',
    'robusta', 'blend', 'single origin', 'estate', 'peaberry',
    'monsooned', 'chicory'
    # Add more specific coffee terms
]

# Keywords strongly indicating it's NOT a coffee product (e.g., equipment, merch)
NON_PRODUCT_KEYWORDS = [
    'mug', 'cup', 'filter', 'brewer', 'grinder', 'equipment', 'machine',
    'maker', 'merch', 'merchandise', 't-shirt', 'subscription', 'apparel',
    'accessories', 'gift card', 'e-gift', 'voucher', 'course', 'workshop',
    'event', 'membership', 'tumbler', 'french press', 'aeropress', 'kalita',
    'hario', 'v60', 'chemex', 'scale', 'kettle', 'frother', 'capsule', 'pod',
    'kit'
    # Add more non-coffee item terms
]

# URL patterns indicating it's likely a product page (less specific than keywords)
# (No longer strictly needed for the new logic, but kept for reference/future use)
GENERAL_PRODUCT_INDICATORS = [
    '/product/', '/products/', '/shop/', '/store/', '/item/', '/p/'
    # '/coffee/' - Can be ambiguous (category vs product)
    # '/bean/' - Can be ambiguous
]

# URL patterns indicating it's likely NOT a product page
NON_PRODUCT_URL_PATTERNS = [
    '/category/', '/categories/', '/collection/', '/collections/', # Handled by keyword check usually, but good fallback
    '/tag/', '/author/', '/page/', '/blog/', '/news/', '/article/',
    '/about', '/contact', '/faq', '/shipping', '/returns', '/policy',
    '/account', '/cart', '/checkout', '/login', '/register', '/gift-cards',
    '/learn', '/guide', '/recipes', '/events', '/workshops', '/wholesale'
]


def is_likely_coffee_product(name: Optional[str] = None,
                             url: Optional[str] = None,
                             product_type: Optional[str] = None, # Keep for potential future use
                             categories: Optional[List[str]] = None,
                             description: Optional[str] = None) -> bool:
    """
    Determine if a product should be excluded based on strong negative indicators.
    Errs on the side of including the product unless specific non-product keywords
    or URL patterns are found.

    Args:
        name: Product name.
        url: Product URL.
        product_type: Type specified in structured data (e.g., "Product"). (Currently unused)
        categories: List of product categories.
        description: Product description.

    Returns:
        False if strong negative indicators are found, True otherwise.
    """
    # Combine all relevant text fields for easier searching
    text_fields = [str(s).lower() for s in [name, description] if s]
    if categories:
        text_fields.extend([str(c).lower() for c in categories])
    combined_text = " ".join(text_fields)

    url_lower = str(url).lower() if url else ""

    # --- Exclusion Checks ---
    # 1. Check for keywords indicating it's NOT a coffee product (e.g., equipment, merch)
    # Use regex word boundaries (\b) to ensure whole word matches.
    negative_keyword_found = False
    matched_keywords = []
    for keyword in NON_PRODUCT_KEYWORDS:
        # Need to escape special regex characters if any are added to keywords later
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, combined_text, re.IGNORECASE):
            negative_keyword_found = True
            matched_keywords.append(keyword)
            # Optional: break early if one match is enough
            # break

    if negative_keyword_found:
        logger.debug(f"Classifier: Excluding '{name or url}' due to non-product keyword(s) in text: {matched_keywords}")
        return False

    # 2. Check for URL patterns indicating non-product pages (e.g., /blog/, /category/)
    if url_lower and any(pattern in url_lower for pattern in NON_PRODUCT_URL_PATTERNS):
        # This check is primarily for page *type* (blog, category page, policy page etc.)
        # The keyword check above should handle non-coffee *items* on product-like URLs.
        matched_patterns = [p for p in NON_PRODUCT_URL_PATTERNS if p in url_lower]
        logger.debug(f"Classifier: Excluding '{name or url}' due to non-product URL pattern(s): {matched_patterns}")
        return False

    # --- Default Inclusion ---
    # If none of the exclusion rules were met, assume it's a product we want to keep.
    logger.debug(f"Classifier: Including '{name or url}' as no exclusion rules matched.")
    return True
