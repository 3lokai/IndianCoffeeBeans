# IndianCoffeeBeans Testing Plan

## Tests Already Present

### Input, Metadata & Platform
- [x] **test_platform_detector_known_platforms**  
  Covered in `tests/test_platform_detector.py`, `tests/test_discoverers.py`.
- [x] **test_platform_detector_unknown_platform**  
  Covered in `tests/test_platform_detector.py`.
- [x] **test_extract_logo_description_platform**  
  Partially covered in `tests/test_helpers.py` (mock HTML extraction).

### Product Discovery
- [x] **test_products_json_endpoint**  
  Covered in `tests/test_discoverers.py` (`test_shopify_discovery`, `test_woocommerce_discovery`).
- [x] **test_sitemap_parsing**  
  Covered in `tests/test_discoverers.py` (`test_sitemap_discoverer`).
- [x] **test_html_internal_product_links**  
  Covered in `tests/test_discoverers.py` (`test_html_discoverer`).
- [x] **test_structured_data_tag_discovery**  
  Covered in `tests/test_discoverers.py` (`test_structured_data_discoverer`).
- [x] **test_discovery_manager_full_flow**  
  Covered in `tests/test_discoverers.py` (`test_discovery_manager_full_flow`).
- [x] **test_discovery_manager_with_cache**  
  Covered in `tests/test_discoverers.py` (`test_discovery_manager_with_cache`).

### Product Extraction
- [x] **test_json_css_schema_extraction**  
  Covered in `tests/test_extractors.py`, `tests/test_real_extractors.py`.
- [x] **test_fit_markdown_deepseek_fallback**  
  Covered in `tests/test_extractors.py` (`test_deepseek_extractor_with_description_fallback`).

### Integration/End-to-End
- [x] **test_full_pipeline_with_mock_data**  
  Covered in `tests/test_helpers.py` (`mock_test_multiple_roasters`).
- [x] **test_full_pipeline_real_roasters**  
  Covered in `tests/test_multiple_roasters.py`, `tests/test_real_discoverers.py`.


## ✅ All Required Tests Are Now Present

All tests previously listed as "to be created" have now been implemented and are present in the `tests/` directory. You can track their status and progress using the unified execution order above.

- [x] **test_csv_input_format**  (`tests/test_input_csv.py`)
- [x] **test_csv_input_edge_cases**  (`tests/test_input_csv.py`)
- [x] **test_homepage_crawl_success**  (`tests/test_homepage_crawl.py`)
- [x] **test_supabase_roaster_insert**  (`tests/test_supabase_roaster.py`)
- [x] **test_supabase_roaster_duplicate_handling**  (`tests/test_supabase_roaster.py`)
- [x] **test_discovery_fallback_order**  (`tests/test_discovery_fallback.py`)
- [x] **test_product_extraction_field_completeness**  (`tests/test_extractor_completeness.py`)
- [x] **test_pydantic_model_validation**  (`tests/test_data_validation_upload.py`)
- [x] **test_batch_upsert_supabase**  (`tests/test_data_validation_upload.py`)
- [x] **test_save_csv_copy**  (`tests/test_data_validation_upload.py`)
- [x] **test_error_logging_on_failure**  (`tests/test_cache_and_logging.py`)
- [x] **test_cache_invalidation_and_update**  (`tests/test_cache_and_logging.py`)

You can now focus on running, maintaining, and expanding these tests as your pipeline evolves!

---

## Unified Test Execution Order & Commands

Below is the single recommended order to run **all tests** (new, edge-case/unit, and core/integration) for complete pipeline coverage. Run each test individually to debug, or use the full command to run all at once.

### 1. Input & Metadata
1. **test_input_csv.py**
   - `pytest tests/test_input_csv.py`
2. **test_homepage_crawl.py**
   - `pytest tests/test_homepage_crawl.py`
3. **test_supabase_roaster.py**
   - `pytest tests/test_supabase_roaster.py`

### 2. Product Discovery & Platform
4. **test_platform_detector.py**
   - `pytest tests/test_platform_detector.py`
5. **test_discoverers.py**
   - `pytest tests/test_discoverers.py`
6. **test_discovery_fallback.py**
   - `pytest tests/test_discovery_fallback.py`
7. **test_blue_tokai.py**
   - `pytest tests/test_blue_tokai.py`
8. **test_real_discoverers.py**
   - `pytest tests/test_real_discoverers.py`
9. **test_multiple_roasters.py**
   - `pytest tests/test_multiple_roasters.py`

### 3. Product Extraction
10. **test_extractor_completeness.py**
    - `pytest tests/test_extractor_completeness.py`
11. **test_extractors.py**
    - `pytest tests/test_extractors.py`
12. **test_real_extractors.py**
    - `pytest tests/test_real_extractors.py`

### 4. Data Validation + Upload
13. **test_data_validation_upload.py**
    - `pytest tests/test_data_validation_upload.py`

### 5. Caching, Error Logging, Helpers, Setup
14. **test_cache_and_logging.py**
    - `pytest tests/test_cache_and_logging.py`
15. **test_helpers.py**
    - `pytest tests/test_helpers.py`
16. **test_setup.py**
    - `pytest tests/test_setup.py`

---

### To run all tests in order (single command):
```sh
pytest tests/test_input_csv.py tests/test_homepage_crawl.py tests/test_supabase_roaster.py tests/test_platform_detector.py tests/test_discoverers.py tests/test_discovery_fallback.py tests/test_blue_tokai.py tests/test_real_discoverers.py tests/test_multiple_roasters.py tests/test_extractor_completeness.py tests/test_extractors.py tests/test_real_extractors.py tests/test_data_validation_upload.py tests/test_cache_and_logging.py tests/test_helpers.py tests/test_setup.py
```

Or simply run all tests in the suite (recommended for CI):
```sh
pytest
```

---

*This unified order ensures every major pipeline stage and edge case is tested. If you add new test files, append them to this list for consistency!*

---

**Legend:**
- [x] = Already present
- [ ] = Needs to be created

*If you want stubs or code for the missing tests, let me know!*