import pytest
from unittest.mock import patch
import logging

# --- test_error_logging_on_failure ---
def test_error_logging_on_failure(caplog):
    from scrapers.roaster_pipeline import process_roaster
    caplog.set_level(logging.ERROR)
    with patch('scrapers.roaster_pipeline.extract_metadata', side_effect=Exception('fail')):
        process_roaster({'name': 'fail', 'website': 'fail'})
    assert any('fail' in r.message for r in caplog.records)

# --- test_cache_invalidation_and_update ---
def test_cache_invalidation_and_update(tmp_path):
    from scrapers.roaster_pipeline import update_cache, load_cache
    cache_path = tmp_path / 'cache.json'
    update_cache('key', {'val': 1}, cache_path)
    data = load_cache('key', cache_path)
    assert data == {'val': 1}
    # Invalidate
    update_cache('key', {'val': 2}, cache_path)
    data = load_cache('key', cache_path)
    assert data == {'val': 2}
