#!/usr/bin/env python3
"""
Test helpers for RoasterPipeline with mocked responses
"""
import os
import json
import asyncio
import aiohttp
from unittest.mock import patch, AsyncMock
from bs4 import BeautifulSoup
import logging

from scrapers.roaster_pipeline import RoasterPipeline

logger = logging.getLogger(__name__)

class MockResponse:
    """Mock aiohttp response"""
    def __init__(self, text, status=200):
        self.text_content = text
        self.status = status
    
    async def text(self):
        return self.text_content
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

def create_mock_html(roaster_name, description, founded_year=2015, 
                    city="Bangalore", state="Karnataka", has_subscription=True,
                    has_store=True, instagram="coffeecompany", social_links=None):
    """Generate mock HTML for testing"""
    if social_links is None:
        social_links = [
            f"https://instagram.com/{instagram}",
            "https://facebook.com/coffeeroaster",
            "https://twitter.com/coffeeroaster"
        ]
    
    # Build social links HTML
    social_html = ""
    for link in social_links:
        platform = link.split(".com/")[0].split("//")[1]
        social_html += f'<a href="{link}">{platform.capitalize()}</a>\n'
    
    # Create mock HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{roaster_name}</title>
        <meta name="description" content="{description}">
        <meta property="og:image" content="https://example.com/logo.png">
        <link rel="icon" href="/favicon.ico">
    </head>
    <body>
        <header>
            <div class="logo">
                <img src="/images/logo.png" alt="{roaster_name} Logo">
            </div>
            <nav>
                <ul>
                    <li><a href="/">Home</a></li>
                    <li><a href="/about">About</a></li>
                    <li><a href="/shop">Shop</a></li>
                    <li><a href="/contact">Contact</a></li>
                </ul>
            </nav>
        </header>
        <main>
            <section class="about-us">
                <h2>About Us</h2>
                <p>Founded in {founded_year}, {roaster_name} is dedicated to sourcing and roasting the finest coffee beans from across India. {description}</p>
            </section>
            <section class="social">
                <h3>Follow Us</h3>
                <div class="social-icons">
                    {social_html}
                </div>
            </section>
            <section class="contact">
                <h3>Contact Us</h3>
                <p>Email: <a href="mailto:info@coffeecompany.com">info@coffeecompany.com</a></p>
                <p>Phone: <a href="tel:+919876543210">+91 9876543210</a></p>
                <div class="address">
                    123 Coffee Street, {city}, {state}
                </div>
            </section>
            {"<section class='subscription'><h3>Coffee Subscription</h3><p>Sign up for our monthly subscription.</p><a href='/subscribe'>Subscribe Now</a></section>" if has_subscription else ""}
            {"<section class='cafe'><h3>Visit Our Cafe</h3><p>Our cafe is open from 8am to 8pm every day.</p><a href='/directions'>Get Directions</a></section>" if has_store else ""}
        </main>
        <footer>
            <p>&copy; 2025 {roaster_name}. All rights reserved.</p>
        </footer>
    </body>
    </html>
    """
    return html

async def test_with_mock_html(roaster_data, mock_html):
    """Test roaster pipeline with mocked HTML response"""
    pipeline = RoasterPipeline(refresh_cache=True)
    
    # Mock the fetch_page method to return our custom HTML
    async def mock_fetch_page(url):
        return mock_html
    
    # Apply the mock
    with patch.object(pipeline, 'fetch_page', side_effect=mock_fetch_page):
        # Apply mock to platform detector to avoid actual network calls
        with patch.object(pipeline.platform_detector, 'detect', return_value=AsyncMock(return_value="custom")):
            # Process the roaster
            result = await pipeline.process_roaster(roaster_data)
            
            # Close the pipeline
            await pipeline.close()
            
            return result

async def mock_test_multiple_roasters():
    """Run tests for multiple mock roasters"""
    mock_roasters = [
        {
            "name": "Sunrise Coffee Roasters",
            "description": "We focus on sustainable relationships with farmers and bringing out unique flavors in every batch.",
            "founded_year": 2018,
            "city": "Bangalore",
            "state": "Karnataka",
            "instagram": "sunrise_coffee",
            "has_subscription": True,
            "has_store": True
        },
        {
            "name": "Mountain Peak Coffee",
            "description": "Specializing in high-altitude grown coffee with bright acidity and complex flavors.",
            "founded_year": 2015,
            "city": "Mumbai",
            "state": "Maharashtra",
            "instagram": "mountainpeak",
            "has_subscription": True,
            "has_store": False
        },
        {
            "name": "Urban Brew Coffee",
            "description": "Bringing specialty coffee to urban dwellers with a focus on accessible brewing methods.",
            "founded_year": 2020,
            "city": "Delhi",
            "state": "Delhi",
            "instagram": "urbanbrew",
            "has_subscription": False,
            "has_store": True
        }
    ]
    
    results = []
    
    for mock_data in mock_roasters:
        # Create roaster_data dictionary
        roaster_data = {
            "name": mock_data["name"],
            "website": f"https://{mock_data['name'].lower().replace(' ', '')}.com"
        }
        
        # Create mock HTML
        mock_html = create_mock_html(
            roaster_name=mock_data["name"],
            description=mock_data["description"],
            founded_year=mock_data["founded_year"],
            city=mock_data["city"],
            state=mock_data["state"],
            instagram=mock_data["instagram"],
            has_subscription=mock_data["has_subscription"],
            has_store=mock_data["has_store"]
        )
        
        # Run test with mocked data
        logger.info(f"Testing with mock data for {roaster_data['name']}")
        result = await test_with_mock_html(roaster_data, mock_html)
        
        if result:
            logger.info(f"✅ Successfully processed mock data for {result['name']}")
            results.append(result)
        else:
            logger.error(f"❌ Failed to process mock data for {roaster_data['name']}")
    
    # Save mock test results
    output_dir = "test_results/mock_tests"
    os.makedirs(output_dir, exist_ok=True)
    
    for result in results:
        filename = os.path.join(output_dir, f"{result['name'].lower().replace(' ', '_')}.json")
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Saved mock test result to {filename}")
    
    # Create summary
    summary = {
        "total_tested": len(mock_roasters),
        "successful": len(results),
        "failed": len(mock_roasters) - len(results),
        "results": {r["name"]: {
            "has_description": bool(r.get("description")),
            "has_logo": bool(r.get("logo_url")),
            "social_links_count": len(r.get("social_links", [])),
            "has_instagram": bool(r.get("instagram_handle")),
            "has_location": bool(r.get("city") and r.get("state")),
            "has_founded_year": bool(r.get("founded_year")),
            "has_subscription": r.get("has_subscription", False),
            "has_physical_store": r.get("has_physical_store", False),
        } for r in results}
    }
    
    # Save summary
    with open(os.path.join(output_dir, "mock_summary.json"), 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Mock tests complete: {len(results)}/{len(mock_roasters)} successful")
    return results

if __name__ == "__main__":
    # Run mock tests
    logging.basicConfig(level=logging.INFO)
    asyncio.run(mock_test_multiple_roasters())