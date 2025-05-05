import pytest
from unittest.mock import MagicMock, AsyncMock

@pytest.fixture
def roaster_data():
    """Provides sample roaster data for tests."""
    return {
        "name": "Blue Tokai Coffee Roasters",
        "url": "https://bluetokaicoffee.com/",
        "instagram": "https://www.instagram.com/bluetokaicoffee/",
        # Add other relevant fields if needed by tests
    }

@pytest.fixture
def mock_html():
    """Provides sample HTML content for tests."""
    return """
    <html>
    <head><title>Test Page</title></head>
    <body>
        <h1>Welcome</h1>
        <p>This is a test.</p>
        <a href='/product1'>Product 1</a>
        <a href='/product2'>Product 2</a>
    </body>
    </html>
    """

@pytest.fixture
def mock_aiohttp_client(monkeypatch):
    # Mock response
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock()

    # Mock session.get() context manager
    mock_get_ctx = MagicMock()
    mock_get_ctx.__aenter__.return_value = mock_response

    # Mock session context manager
    mock_session = MagicMock()
    mock_session.get.return_value = mock_get_ctx

    # Patch ClientSession to return the mock session
    monkeypatch.setattr("aiohttp.ClientSession", MagicMock(return_value=mock_session))

    return mock_session, mock_response
