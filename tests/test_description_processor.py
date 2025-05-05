import pytest
import asyncio
from common.description_processor import DescriptionProcessor

@pytest.mark.asyncio
async def test_compile_description_fallback():
    sources = {"homepage": "", "about_pages": [], "markdown": ""}
    roaster_name = "Test Roaster"
    # No deepseek_client, no valid sentences, should fallback
    result = await DescriptionProcessor.compile_description(sources, roaster_name)
    assert result == f"{roaster_name} is a specialty coffee roaster focusing on quality beans and expert roasting techniques."

@pytest.mark.asyncio
async def test_compile_description_with_sentences():
    sources = {"homepage": "We are a small batch roaster. We love coffee!", "about_pages": [], "markdown": ""}
    roaster_name = "Test Roaster"
    result = await DescriptionProcessor.compile_description(sources, roaster_name)
    # Should return the cleaned, non-fallback sentences (joined by newline)
    assert "small batch roaster" in result
    assert "love coffee" in result
    assert "Test Roaster" not in result  # Should not be fallback
