import pytest
from unittest.mock import patch, MagicMock

# --- test_homepage_crawl_success ---
def test_homepage_crawl_success():
    from scrapers.roaster_pipeline import crawl_homepage_metadata
    html = """
        <html><head><title>Test Roaster</title></head>
        <body><img src='logo.png' id='logo'><p id='desc'>Best coffee!</p></body></html>
    """
    with patch('scrapers.roaster_pipeline.fetch_html', return_value=html):
        meta = crawl_homepage_metadata('https://test.com')
        assert meta['logo'] == 'logo.png'
        assert 'Best coffee' in meta['description']
