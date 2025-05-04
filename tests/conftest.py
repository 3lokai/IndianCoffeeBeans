import pytest
from unittest.mock import MagicMock, AsyncMock

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
