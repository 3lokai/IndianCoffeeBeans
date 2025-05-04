# tests/test_extractors.py
import pytest
import asyncio
from unittest.mock import patch, MagicMock
import json
import os
from pathlib import Path

# Import modules to test
from scrapers.extractors.json_css_extractor import JsonCssExtractor
from scrapers.extractors.deepseek_extractor import DeepseekExtractor
from common.models import RoastLevel, BeanType, ProcessingMethod

# Setup test directory for fixtures
TEST_DIR = Path(__file__).parent
FIXTURES_DIR = TEST_DIR / "fixtures"
os.makedirs(FIXTURES_DIR, exist_ok=True)


# Mock data fixtures
@pytest.fixture
def shopify_product():
    return {
        "name": "Ethiopia Yirgacheffe",
        "slug": "ethiopia-yirgacheffe",
        "direct_buy_url": "https://example.com/products/ethiopia-yirgacheffe",
        "platform": "shopify",
        "image_url": "https://example.com/test.jpg",
        "source_data": {
            "title": "Ethiopia Yirgacheffe",
            "body_html": "<p>A fruity, floral coffee from Ethiopia with bright citrus notes.</p>",
            "variants": [
                {"title": "250g", "price": "550.00"},
                {"title": "500g", "price": "1000.00"}
            ]
        }
    }


@pytest.fixture
def woocommerce_product():
    return {
        "name": "Colombia Supremo",
        "slug": "colombia-supremo",
        "direct_buy_url": "https://example.com/products/colombia-supremo",
        "platform": "woocommerce",
        "description": "A balanced, smooth coffee from Colombia with notes of caramel and nuts.",
        "source_data": {
            "name": "Colombia Supremo",
            "description": "A balanced, smooth coffee from Colombia with notes of caramel and nuts.",
            "price": "500.00",
            "attributes": [
                {"name": "Roast", "options": ["Medium"]},
                {"name": "Process", "options": ["Washed"]},
                {"name": "Origin", "options": ["Colombia"]}
            ]
        }
    }


@pytest.fixture
def generic_product():
    return {
        "name": "Brazil Santos",
        "slug": "brazil-santos",
        "direct_buy_url": "https://example.com/products/brazil-santos",
        "platform": "generic",
        "description": "A nutty, chocolatey coffee from Brazil.",
        "image_url": "https://example.com/brazil.jpg"
    }


@pytest.fixture
def extracted_data_shopify():
    return {
        "name": "Ethiopia Yirgacheffe",
        "product_description": "<p>A fruity, floral coffee from Ethiopia with bright citrus notes.</p><p>This coffee is grown at high altitudes in the Yirgacheffe region.</p>",
        "image_url": "https://example.com/test.jpg",
        "price_text": "₹550.00",
        "stock_status": "In Stock",
        "roast_level": "Light",
        "process_info": "Washed",
        "origin_info": "Ethiopia, Yirgacheffe Region",
        "variants": json.dumps([
            {"title": "250g", "price": "550.00"},
            {"title": "500g", "price": "1000.00"}
        ]),
        "specifications": "<table><tr><td>Altitude</td><td>1800-2200m</td></tr><tr><td>Varietals</td><td>Heirloom</td></tr></table>"
    }


@pytest.fixture
def extracted_data_woocommerce():
    return {
        "name": "Colombia Supremo",
        "product_description": "A balanced, smooth coffee from Colombia with notes of caramel and nuts.",
        "image_url": "https://example.com/colombia.jpg",
        "price_text": "₹500.00",
        "stock_status": "In Stock",
        "attributes_table": "<table><tr><th>Roast</th><td>Medium</td></tr><tr><th>Process</th><td>Washed</td></tr><tr><th>Origin</th><td>Colombia</td></tr></table>",
        "roast_level": "Medium",
        "process_info": "Washed",
        "origin_info": "Colombia",
        "specifications": "<p>This coffee is grown in the Huila region of Colombia at altitudes of 1600-1900m.</p>"
    }


@pytest.fixture
def extracted_data_generic():
    return {
        "name": "Brazil Santos",
        "product_description": "A nutty, chocolatey coffee from Brazil. Medium-dark roast with low acidity.",
        "image_url": "https://example.com/brazil.jpg",
        "price_text": "₹475 for 250g",
        "stock_status": "In Stock",
        "roast_info": "Medium-dark roast",
        "process_info": "Natural processed",
        "origin_info": "Brazil, Santos region",
        "specifications": "<p>Altitude: 1000-1200m<br>Varietals: Bourbon, Catuai</p>"
    }


@pytest.fixture
def deepseek_extracted_attributes():
    return {
        "roast_level": "medium-dark",
        "bean_type": "arabica",
        "processing_method": "natural",
        "region_name": "Brazil, Santos",
        "tasting_notes": "Chocolate, nuts, caramel",
        "flavor_profiles": ["chocolate", "nutty", "caramel"],
        "brew_methods": ["espresso", "french-press", "moka-pot"],
        "altitude_min": 1000,
        "altitude_max": 1200,
        "varietal": "Bourbon, Catuai",
        "is_blend": False,
        "is_seasonal": False
    }


@pytest.fixture
def mock_crawler():
    with patch('crawl4ai.AsyncWebCrawler') as mock_crawler_class:
        mock_crawler = MagicMock()
        mock_crawler_class.return_value.__aenter__.return_value = mock_crawler
        
        # Create mock result
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.markdown = MagicMock()
        mock_result.markdown.fit_markdown = "Mocked markdown content"
        mock_result.html = "<html>Mocked HTML content</html>"
        mock_result.extracted_content = json.dumps({"test": "data"})
        
        mock_crawler.arun.return_value = mock_result
        
        yield mock_crawler


@pytest.fixture
def mock_openai():
    with patch('openai.OpenAI') as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        
        # Mock chat completion
        mock_completion = MagicMock()
        mock_message = MagicMock()
        mock_message.content = json.dumps({
            "roast_level": "medium-dark",
            "bean_type": "arabica",
            "processing_method": "natural",
            "region_name": "Brazil, Santos",
            "tasting_notes": "Chocolate, nuts, caramel",
            "flavor_profiles": ["chocolate", "nutty", "caramel"],
            "brew_methods": ["espresso", "french-press", "moka-pot"],
            "altitude_min": 1000,
            "altitude_max": 1200,
            "varietal": "Bourbon, Catuai",
            "is_blend": False,
            "is_seasonal": False
        })
        mock_completion.choices = [MagicMock(message=mock_message)]
        
        mock_client.chat.completions.create.return_value = mock_completion
        
        yield mock_client


@pytest.mark.asyncio
async def test_json_css_extractor_shopify(mock_crawler, shopify_product, extracted_data_shopify):
    # Configure mock crawler to return the test data
    mock_crawler.arun.return_value.extracted_content = json.dumps(extracted_data_shopify)
    
    extractor = JsonCssExtractor()
    
    enhanced_product = await extractor.extract(shopify_product)
    
    # Check basic fields were extracted
    assert enhanced_product.get("name") == "Ethiopia Yirgacheffe"
    assert enhanced_product.get("description") is not None
    assert enhanced_product.get("image_url") == "https://example.com/test.jpg"
    
    # Check specific fields
    assert enhanced_product.get("roast_level") == RoastLevel.LIGHT
    assert enhanced_product.get("processing_method") == ProcessingMethod.WASHED
    assert enhanced_product.get("region_name") == "Ethiopia, Yirgacheffe Region"
    
    # Check price extraction
    assert "price_250g" in enhanced_product or "price_500g" in enhanced_product


@pytest.mark.asyncio
async def test_json_css_extractor_woocommerce(mock_crawler, woocommerce_product, extracted_data_woocommerce):
    # Configure mock crawler to return the test data
    mock_crawler.arun.return_value.extracted_content = json.dumps(extracted_data_woocommerce)
    
    extractor = JsonCssExtractor()
    
    enhanced_product = await extractor.extract(woocommerce_product)
    
    # Check basic fields were extracted
    assert enhanced_product.get("name") == "Colombia Supremo"
    assert enhanced_product.get("description") is not None
    assert enhanced_product.get("image_url") == "https://example.com/colombia.jpg"
    
    # Check specific fields
    assert enhanced_product.get("roast_level") == RoastLevel.MEDIUM
    assert enhanced_product.get("processing_method") == ProcessingMethod.WASHED
    assert enhanced_product.get("region_name") == "Colombia"
    
    # Check price extraction (WooCommerce often has a single price)
    assert "price_250g" in enhanced_product or "price" in enhanced_product


@pytest.mark.asyncio
async def test_json_css_extractor_generic(mock_crawler, generic_product, extracted_data_generic):
    # Configure mock crawler to return the test data
    mock_crawler.arun.return_value.extracted_content = json.dumps(extracted_data_generic)
    
    extractor = JsonCssExtractor()
    
    enhanced_product = await extractor.extract(generic_product)
    
    # Check basic fields were extracted
    assert enhanced_product.get("name") == "Brazil Santos"
    assert enhanced_product.get("description") is not None
    assert enhanced_product.get("image_url") == "https://example.com/brazil.jpg"
    
    # Check specific fields
    assert enhanced_product.get("roast_level") == RoastLevel.MEDIUM_DARK
    assert enhanced_product.get("processing_method") == ProcessingMethod.NATURAL
    assert enhanced_product.get("region_name") == "Brazil, Santos region"
    
    # Check price extraction from text
    assert "price_250g" in enhanced_product


@pytest.mark.asyncio
async def test_json_css_extractor_failure(mock_crawler, generic_product):
    # Simulate a failed crawl
    mock_crawler.arun.return_value.success = False
    
    extractor = JsonCssExtractor()
    
    # Even on failure, the original product should be returned unmodified
    result = await extractor.extract(generic_product)
    assert result == generic_product


@pytest.mark.asyncio
async def test_deepseek_extractor_success(mock_crawler, mock_openai, generic_product, deepseek_extracted_attributes):
    # Configure the mock crawler
    mock_crawler.arun.return_value.success = True
    mock_crawler.arun.return_value.markdown.fit_markdown = """
    # Brazil Santos

    A nutty, chocolatey coffee from Brazil. Medium-dark roast with low acidity.

    ## Details
    - **Origin:** Brazil, Santos region
    - **Altitude:** 1000-1200m
    - **Varietals:** Bourbon, Catuai
    - **Process:** Natural
    - **Roast Level:** Medium-Dark

    ## Tasting Notes
    Chocolate, nuts, caramel

    ## Recommended For
    Espresso, French Press, Moka Pot
    """
    
    extractor = DeepseekExtractor(browser_config=None)
    
    # Test the extraction
    enhanced_product = await extractor.extract(generic_product)
    
    # Check the extracted fields
    assert enhanced_product.get("roast_level") == RoastLevel.MEDIUM_DARK
    assert enhanced_product.get("bean_type") == BeanType.ARABICA
    assert enhanced_product.get("processing_method") == ProcessingMethod.NATURAL
    assert enhanced_product.get("region_name") == "Brazil, Santos"
    assert enhanced_product.get("flavor_profiles") == ["chocolate", "nutty", "caramel"]
    assert enhanced_product.get("brew_methods") == ["espresso", "french-press", "moka-pot"]
    assert enhanced_product.get("deepseek_enriched") == True
    assert enhanced_product.get("extracted_by") == "deepseek"


@pytest.mark.asyncio
async def test_deepseek_extractor_with_description_fallback(mock_crawler, mock_openai, generic_product):
    # Simulate a failed markdown generation but with description available
    mock_crawler.arun.return_value.success = False
    mock_crawler.arun.return_value.markdown.fit_markdown = None
    
    # Add a description to the product
    product_with_desc = generic_product.copy()
    product_with_desc["description"] = "A nutty, chocolatey coffee from Brazil. Medium-dark roast with natural processing."
    
    extractor = DeepseekExtractor(browser_config=None)
    
    # Test the extraction
    enhanced_product = await extractor.extract(product_with_desc)
    
    # Even with a failed crawl, DeepSeek should use the description as fallback
    assert enhanced_product.get("deepseek_enriched") == True
    assert enhanced_product.get("extracted_by") == "deepseek"


@pytest.mark.asyncio
async def test_deepseek_extractor_no_enhancement_needed(mock_crawler, mock_openai):
    # Create a product that already has all the necessary attributes
    complete_product = {
        "name": "Brazil Santos",
        "slug": "brazil-santos",
        "direct_buy_url": "https://example.com/products/brazil-santos",
        "platform": "generic",
        "description": "A nutty, chocolatey coffee from Brazil.",
        "image_url": "https://example.com/brazil.jpg",
        "roast_level": RoastLevel.MEDIUM_DARK,
        "bean_type": BeanType.ARABICA,
        "processing_method": ProcessingMethod.NATURAL,
        "region_name": "Brazil, Santos",
        "flavor_profiles": ["chocolate", "nutty"]
    }
    
    extractor = DeepseekExtractor(browser_config=None)
    
    # Test the extraction
    result = await extractor.extract(complete_product)
    
    # Product should be returned unmodified since it already has necessary attributes
    assert result == complete_product
    assert "deepseek_enriched" not in result
    assert "extracted_by" not in result


@pytest.mark.asyncio
async def test_deepseek_extractor_clean_attributes(mock_openai):
    # Test the attribute cleaning function directly
    extractor = DeepseekExtractor(browser_config=None)
    
    raw_attributes = {
        "roast_level": "Medium-Dark",
        "bean_type": "ARABICA",
        "processing_method": "pulped natural",
        "region_name": "Colombia, Huila",
        "tasting_notes": "Caramel, berry, citrus",
        "flavor_profiles": ["caramel", "berry", "citrus"],
        "brew_methods": ["pour-over", "espresso"],
        "altitude_min": "1600",
        "altitude_max": "1900",
        "is_blend": "false",
        "is_seasonal": True
    }
    
    cleaned = extractor._clean_attributes(raw_attributes)
    
    # Check normalization
    assert cleaned["roast_level"] == RoastLevel.MEDIUM_DARK
    assert cleaned["bean_type"] == BeanType.ARABICA
    assert cleaned["processing_method"] == ProcessingMethod.PULPED_NATURAL
    assert cleaned["region_name"] == "Colombia, Huila"
    assert cleaned["flavor_profiles"] == ["caramel", "berry", "citrus"]
    assert cleaned["brew_methods"] == ["pour-over", "espresso"]
    assert cleaned["altitude_min"] == 1600
    assert cleaned["altitude_max"] == 1900
    assert cleaned["is_blend"] is False
    assert cleaned["is_seasonal"] is True


def test_deepseek_extractor_merge_attributes():
    # Test the attribute merging function directly
    extractor = DeepseekExtractor(browser_config=None)
    
    original_product = {
        "name": "Test Coffee",
        "slug": "test-coffee",
        "roast_level": RoastLevel.UNKNOWN,
        "bean_type": BeanType.UNKNOWN,
        "processing_method": ProcessingMethod.UNKNOWN,
        "existing_value": "Don't override me"
    }
    
    extracted_attributes = {
        "roast_level": RoastLevel.MEDIUM,
        "bean_type": BeanType.ARABICA,
        "processing_method": ProcessingMethod.WASHED,
        "region_name": "Ethiopia",
        "new_value": "Add me",
        "unknown_value": "unknown"  # This should be skipped
    }
    
    merged = extractor._merge_attributes(original_product, extracted_attributes)
    
    # Check that unknown values were filled in
    assert merged["roast_level"] == RoastLevel.MEDIUM
    assert merged["bean_type"] == BeanType.ARABICA
    assert merged["processing_method"] == ProcessingMethod.WASHED
    assert merged["region_name"] == "Ethiopia"
    assert merged["new_value"] == "Add me"
    
    # Check that existing values were preserved
    assert merged["existing_value"] == "Don't override me"
    
    # Check that unknown values were skipped
    assert "unknown_value" not in merged


def test_normalize_roast_level():
    extractor = JsonCssExtractor()
    
    # Test various types of roast level text
    assert extractor._normalize_roast_level("Light Roast") == RoastLevel.LIGHT
    assert extractor._normalize_roast_level("Medium Light") == RoastLevel.MEDIUM_LIGHT
    assert extractor._normalize_roast_level("Medium Roast") == RoastLevel.MEDIUM
    assert extractor._normalize_roast_level("Medium-Dark") == RoastLevel.MEDIUM_DARK
    assert extractor._normalize_roast_level("Dark") == RoastLevel.DARK
    assert extractor._normalize_roast_level("Unknown") == RoastLevel.UNKNOWN


def test_normalize_processing_method():
    extractor = JsonCssExtractor()
    
    # Test various types of processing method text
    assert extractor._normalize_processing_method("Washed Process") == ProcessingMethod.WASHED
    assert extractor._normalize_processing_method("Natural / Dry Process") == ProcessingMethod.NATURAL
    assert extractor._normalize_processing_method("Honey Processed") == ProcessingMethod.HONEY
    assert extractor._normalize_processing_method("Anaerobic Fermentation") == ProcessingMethod.ANAEROBIC
    assert extractor._normalize_processing_method("Pulped Natural") == ProcessingMethod.PULPED_NATURAL
    assert extractor._normalize_processing_method("Unknown") == ProcessingMethod.UNKNOWN