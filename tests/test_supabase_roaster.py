import pytest
from unittest.mock import patch, MagicMock

# --- test_supabase_roaster_insert ---
def test_supabase_roaster_insert():
    from scrapers.roaster_pipeline import insert_roaster_metadata
    meta = {'name': 'Test Roaster', 'website': 'https://test.com', 'logo': 'logo.png'}
    with patch('scrapers.roaster_pipeline.supabase_client') as mock_client:
        insert_roaster_metadata(meta)
        mock_client.table.assert_called_with('roasters')
        mock_client.insert.assert_called()

# --- test_supabase_roaster_duplicate_handling ---
def test_supabase_roaster_duplicate_handling():
    from scrapers.roaster_pipeline import insert_roaster_metadata
    meta = {'name': 'Test Roaster', 'website': 'https://test.com', 'logo': 'logo.png'}
    with patch('scrapers.roaster_pipeline.supabase_client') as mock_client:
        # Simulate duplicate error then upsert
        mock_client.insert.side_effect = Exception('duplicate')
        insert_roaster_metadata(meta)
        mock_client.upsert.assert_called()
