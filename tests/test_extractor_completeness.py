import pytest

# --- test_product_extraction_field_completeness ---
def test_product_extraction_field_completeness():
    from scrapers.extractors.json_css_extractor import JsonCssExtractor
    sample = {'name': 'Coffee', 'description': 'desc', 'image_url': 'img.jpg'}
    required_fields = ['name', 'description', 'image_url']
    extractor = JsonCssExtractor()
    product = extractor._postprocess(sample)
    missing = [f for f in required_fields if f not in product or not product[f]]
    assert not missing
