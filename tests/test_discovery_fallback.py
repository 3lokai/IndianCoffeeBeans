import pytest
from unittest.mock import patch, MagicMock

# --- test_discovery_fallback_order ---
def test_discovery_fallback_order():
    from scrapers.discoverers.discovery_manager import DiscoveryManager
    dm = DiscoveryManager()
    order = []
    with patch.object(dm, '_discover_shopify_products', side_effect=lambda url: order.append('shopify') or []), \
         patch.object(dm, '_discover_woocommerce_products', side_effect=lambda url: order.append('woocommerce') or []), \
         patch.object(dm, '_discover_sitemap_products', side_effect=lambda url: order.append('sitemap') or []), \
         patch.object(dm, '_discover_html_products', side_effect=lambda url: order.append('html') or []), \
         patch.object(dm, '_discover_structured_data_products', side_effect=lambda url: order.append('structured') or []):
        dm.discover_products({'website_url': 'https://test.com'})
    assert order == ['shopify', 'woocommerce', 'sitemap', 'html', 'structured']
