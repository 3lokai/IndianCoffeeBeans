import os
os.environ["DEEPSEEK_API_KEY"] = "fake-key"

# tests/test_extractors.py
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
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
        mock_crawler = AsyncMock()
        mock_crawler_class.return_value.__aenter__.return_value = mock_crawler
        
        # Create mock result
        mock_result = AsyncMock()
        mock_result.success = True
        mock_result.extracted_content = ""
        
        mock_crawler.arun.return_value = mock_result
        
        yield mock_crawler


@pytest.fixture
def mock_openai():
    with patch('openai.OpenAI') as mock_openai_class:
        mock_client = AsyncMock()
        mock_openai_class.return_value = mock_client
        
        # Mock chat completion
        mock_completion = AsyncMock()
        mock_message = AsyncMock()
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
        mock_completion.choices = [mock_message]
        
        mock_client.chat.completions.create.return_value = mock_completion
        
        yield mock_client


@pytest.mark.asyncio
@patch('scrapers.extractors.json_css_extractor.AsyncWebCrawler')
async def test_json_css_extractor_shopify(mock_crawler_cls):
    # Prepare mock crawl result
    extracted = {
        "name": "Ethiopia Yirgacheffe",
        "product_description": "<p>Fruity, floral coffee.</p>",
        "image_url": "https://example.com/test.jpg",
        "variants": [
            {"title": "250g", "price": "550.00"},
            {"title": "500g", "price": "1000.00"}
        ],
        "roast_level": "Light",
        "process_info": "Washed",
        "origin_info": "Ethiopia, Yirgacheffe Region",
    }
    mock_result = AsyncMock()
    mock_result.success = True
    mock_result.extracted_content = json.dumps(extracted)
    mock_crawler = AsyncMock()
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    
    product = {
        "name": "Ethiopia Yirgacheffe",
        "direct_buy_url": "https://example.com/products/ethiopia-yirgacheffe",
        "platform": "shopify"
    }
    extractor = JsonCssExtractor()
    result = await extractor.extract(product)
    assert result["name"] == "Ethiopia Yirgacheffe"
    assert result.get("description") == "Fruity, floral coffee."
    assert result["image_url"] == "https://example.com/test.jpg"
    assert result["roast_level"] == "Light"
    assert result["processing_method"] == "Washed" or result["processing_method"] == ProcessingMethod.WASHED
    assert result["region_name"] == "Ethiopia, Yirgacheffe Region"
    assert result["price_250g"] == 550.0
    assert result["price_500g"] == 1000.0

@pytest.mark.asyncio
@patch('scrapers.extractors.json_css_extractor.AsyncWebCrawler')
async def test_json_css_extractor_woocommerce(mock_crawler_cls):
    extracted = {
        "name": "Colombia Supremo",
        "product_description": "Balanced, smooth coffee.",
        "image_url": "https://example.com/colombia.jpg",
        "price": "500.00",
        "roast_level": "Medium",
        "process_info": "Washed",
        "origin_info": "Colombia"
    }
    mock_result = AsyncMock()
    mock_result.success = True
    mock_result.extracted_content = json.dumps(extracted)
    mock_crawler = AsyncMock()
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    product = {
        "name": "Colombia Supremo",
        "direct_buy_url": "https://example.com/products/colombia-supremo",
        "platform": "woocommerce"
    }
    extractor = JsonCssExtractor()
    result = await extractor.extract(product)
    assert result["name"] == "Colombia Supremo"
    assert result.get("description") == "Balanced, smooth coffee."
    assert result["image_url"] == "https://example.com/colombia.jpg"
    assert result["roast_level"] == "Medium"
    assert result["processing_method"] == "Washed" or result["processing_method"] == ProcessingMethod.WASHED
    assert result["region_name"] == "Colombia"
    assert result["price_250g"] == 500.0

@pytest.mark.asyncio
@patch('scrapers.extractors.json_css_extractor.AsyncWebCrawler')
async def test_json_css_extractor_failure(mock_crawler_cls):
    mock_result = AsyncMock()
    mock_result.success = False
    mock_result.extracted_content = None
    mock_crawler = AsyncMock()
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    product = {"name": "Fail Product", "direct_buy_url": "https://fail.com", "platform": "generic"}
    extractor = JsonCssExtractor()
    result = await extractor.extract(product)
    assert result == product

@pytest.mark.asyncio
@patch('scrapers.extractors.json_css_extractor.AsyncWebCrawler')
async def test_json_css_extractor_generic(mock_crawler_cls):
    extracted = {
        "name": "Brazil Santos",
        "product_description": "Nutty, chocolatey coffee from Brazil.",
        "image_url": "https://example.com/brazil.jpg",
        "price": "450.00",
        "roast_info": "Medium",
        "process_info": "Natural",
        "origin_info": "Brazil"
    }
    mock_result = AsyncMock()
    mock_result.success = True
    mock_result.extracted_content = json.dumps(extracted)
    mock_crawler = AsyncMock()
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    product = {
        "name": "Brazil Santos",
        "direct_buy_url": "https://example.com/products/brazil-santos",
        "platform": "generic"
    }
    extractor = JsonCssExtractor()
    result = await extractor.extract(product)
    assert result["name"] == "Brazil Santos"
    assert result.get("description") == "Nutty, chocolatey coffee from Brazil."
    assert result["image_url"] == "https://example.com/brazil.jpg"
    assert result["roast_level"] == "Medium" or result["roast_level"] == RoastLevel.MEDIUM
    assert result["processing_method"] == "Natural" or result["processing_method"] == ProcessingMethod.NATURAL
    assert result["region_name"] == "Brazil"
    assert result["price_250g"] == 450.0

@pytest.mark.asyncio
@patch('scrapers.extractors.deepseek_extractor.AsyncWebCrawler')
@patch('scrapers.extractors.deepseek_extractor.OpenAI')
async def test_deepseek_extractor_success(mock_openai_cls, mock_crawler_cls):
    # Mock markdown extraction
    mock_crawler = AsyncMock()
    mock_result = AsyncMock()
    mock_result.success = True
    class Markdown:
        fit_markdown = "# Coffee Product\nA great coffee for users who enjoy taste and caffeine. " * 2  # >50 chars
    mock_result.markdown = Markdown()
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    # Mock OpenAI (DeepSeek)
    mock_openai = MagicMock()
    mock_response = MagicMock()
    mock_choice = MagicMock()
    mock_choice.message.content = json.dumps({
        "roast_level": "medium",
        "bean_type": "arabica",
        "processing_method": "washed",
        "region_name": "Colombia",
        "flavor_profiles": ["chocolate", "nutty"],
        "brew_methods": ["espresso", "filter"],
        "prices": {"250": 500},
        "image_url": "https://example.com/colombia.jpg",
        "direct_buy_url": "https://example.com/products/colombia-supremo",
        "is_seasonal": False,
        "is_featured": False,
        "is_single_origin": True,
        "is_available": True,
        "tags": ["colombia", "arabica"],
        "external_links": []
    })
    mock_response.choices = [mock_choice]
    mock_openai().chat.completions.create.return_value = mock_response
    mock_openai_cls.return_value = mock_openai()
    # Test
    product = {"name": "Colombia Supremo", "direct_buy_url": "https://example.com/products/colombia-supremo"}
    extractor = DeepseekExtractor()
    result = await extractor.extract(product)
    print("DEEPSEEK TEST RESULT:", result)
    assert result["roast_level"] == "medium"
    assert result["bean_type"] == "arabica"
    assert result["processing_method"] == "washed"
    assert result["region_name"] == "Colombia"
    assert result["is_single_origin"] is True
    assert result["is_available"] is True
    assert result["deepseek_enriched"] is True
    assert result["extracted_by"] == "deepseek"

@pytest.mark.asyncio
@patch('scrapers.extractors.deepseek_extractor.AsyncWebCrawler')
@patch('scrapers.extractors.deepseek_extractor.OpenAI')
async def test_deepseek_extractor_fallback_to_description(mock_openai_cls, mock_crawler_cls):
    # Markdown extraction fails, fallback to description
    mock_crawler = AsyncMock()
    mock_result = AsyncMock()
    mock_result.success = False
    mock_result.markdown.fit_markdown = ""
    mock_crawler.arun.return_value = mock_result
    mock_crawler_cls.return_value.__aenter__.return_value = mock_crawler
    # Mock OpenAI (DeepSeek)
    mock_openai = MagicMock()
    mock_response = MagicMock()
    mock_choice = MagicMock()
    mock_choice.message.content = json.dumps({"roast_level": "medium"})
    mock_response.choices = [mock_choice]
    mock_openai().chat.completions.create.return_value = mock_response
    mock_openai_cls.return_value = mock_openai()
    product = {"name": "Fallback Coffee", "direct_buy_url": "https://example.com/fallback", "description": "A fallback description with enough length to trigger fallback."}
    extractor = DeepseekExtractor()
    result = await extractor.extract(product)
    assert result.get("roast_level") == "medium"

@pytest.mark.asyncio
@patch('scrapers.extractors.deepseek_extractor.AsyncWebCrawler')
@patch('scrapers.extractors.deepseek_extractor.OpenAI')
async def test_deepseek_extractor_no_enhancement_needed(mock_openai_cls, mock_crawler_cls):
    # Product already has all attributes
    product = {
        "name": "Complete Coffee",
        "direct_buy_url": "https://example.com/complete",
        "roast_level": "medium",
        "bean_type": "arabica",
        "processing_method": "washed",
        "flavor_profiles": ["chocolate", "nutty"]
    }
    extractor = DeepseekExtractor()
    result = await extractor.extract(product)
    assert result == product