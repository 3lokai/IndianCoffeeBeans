# ☕ Indian Coffee Beans Scraper – Final PRD (v3)

**Last Updated:** May 2025
**Owner:** GT
**Goal:** Build and maintain a robust, scalable scraper to extract and enrich Indian coffee roaster product data and feed it into Supabase.

---

## 🎯 Objectives

* Crawl and extract coffee product data from \~50 Indian roaster websites.
* Support all platforms (Shopify, WooCommerce, generic).
* Prioritize structured extraction using Crawl4AI.
* Use DeepSeek as intelligent fallback for missing attributes.
* Maintain a unified, cache-aware, async pipeline.
* Enable batch upserts to Supabase + optional CSV exports.

---

## 🗂️ Folder Structure (Confirmed)

```
indian_coffee_beans_scraper/
├── main.py
├── config.py
├── .env
├── requirements.txt

├── schemas/
│   ├── roaster_schema.py
│   └── product_schemas.py

├── scrapers/
│   ├── pipeline.py
│   ├── roaster_pipeline.py
│   ├── platform_detector.py
│   ├── extractors/
│   │   ├── json_css_extractor.py
│   │   └── deepseek_extractor.py
│   └── discoverers/
│       ├── discovery_manager.py
│       ├── sitemap_discoverer.py
│       ├── html_discoverer.py
│       └── structured_data_discoverer.py

├── common/
│   ├── models.py
│   ├── utils.py
│   └── cache.py

├── db/
│   └── supabase_client.py

└── data/
    ├── cache/
    └── output/
```

---

## 🔧 Pipeline Flow

1. **Input:** CSV of roaster info (`name, website`)
2. **Roaster Metadata Extraction:**

   * Crawl homepage → extract `logo`, `description`, `platform`
   * Store in Supabase (`roasters`)
3. **Product Discovery:**
   Try in this order:

   * `/products.json` or known API endpoints
   * `sitemap.xml` parsing
   * HTML crawling for internal product links
   * Structured data tags (e.g. @graph, OpenGraph)
4. **Product Extraction:**

   * JSON-CSS schema with Crawl4AI
   * If ≥2 fields are missing → run `fit_markdown` through DeepSeek
5. **Data Validation + Upload:**

   * Use Pydantic models
   * Batch upsert to Supabase
   * Save CSV copy (optional)
6. **Caching + Error Logging:**

   * Store cache per product + roaster
   * Skip URLs already processed unless `--refresh` is passed

---

## 🧱 Modules to Build (with Priority)

### 🟩 Phase 1: Base Setup (Must-Have)

| Priority | Module          | File                    | Purpose                       |
| -------- | --------------- | ----------------------- | ----------------------------- |
| ✅        | Config loader   | `config.py`             | Loads `.env` and constants    |
| ✅        | Supabase client | `db/supabase_client.py` | Insert/upsert to Supabase     |
| ✅        | Models          | `common/models.py`      | Pydantic data validation      |
| ✅        | Utilities       | `common/utils.py`       | Slugging, price parsing, etc. |

### 🟨 Phase 2: Discovery & Roaster Metadata

| Priority | Module                | File                            | Purpose                   |
| -------- | --------------------- | ------------------------------- | ------------------------- |
| 🔼       | Roaster crawler       | `roaster_pipeline.py`           | Crawl homepage + schema   |
| 🔼       | Platform detector     | `platform_detector.py`          | Heuristic detection logic |
| 🔼       | Discovery manager     | `discovery_manager.py`          | Orchestrates discoverers  |
| 🟡       | Sitemap discoverer    | `sitemap_discoverer.py`         | Crawl sitemap URLs        |
| 🟡       | HTML discoverer       | `html_discoverer.py`            | Shallow crawl for links   |
| 🟡       | Structured discoverer | `structured_data_discoverer.py` | Use `@graph` + OpenGraph  |

### 🟧 Phase 3: Product Extraction

| Priority | Module             | File                    | Purpose                          |
| -------- | ------------------ | ----------------------- | -------------------------------- |
| 🔴       | JSON-CSS extractor | `json_css_extractor.py` | Primary schema-based extractor   |
| 🔴       | DeepSeek extractor | `deepseek_extractor.py` | Fallback extractor from Markdown |
| 🔴       | Schemas            | `product_schemas.py`    | Define Crawl4AI selectors        |

### 🟦 Phase 4: Pipeline Orchestration

| Priority | Module           | File          | Purpose                                      |
| -------- | ---------------- | ------------- | -------------------------------------------- |
| 🔴       | Unified pipeline | `pipeline.py` | Run discovery + extraction per roaster       |
| 🔵       | CLI runner       | `main.py`     | Load CSV input, trigger pipeline, export CSV |

---

## 💡 Tips for Implementation

* Use `AsyncWebCrawler` with caching enabled
* Add retry logic (`tenacity`) on network calls
* Markdown fallback should trigger *only* if ≥2 critical fields missing (`roast_level`, `bean_type`, `price`, etc.)
* Cache both roaster and product-level outputs as JSON
* Use `slugify(name)` + UUID fallback for unique slugs

---

## 🛠 Suggested Commands

```bash
# Basic run
python main.py --input=data/roasters.csv

# Force cache refresh
python main.py --input=data/roasters.csv --refresh

# Export CSVs
python main.py --input=data/roasters.csv --export=coffees.csv
```

---

## 📌 Next Steps

* [ ] Finalize `schemas/product_schemas.py`
* [ ] Complete `json_css_extractor.py` and fallback logic
* [ ] Validate pipeline on 5 real roasters
* [ ] Add logging + DeepSeek usage counter
* [ ] Schedule CI runs via GitHub Actions (optional)

---