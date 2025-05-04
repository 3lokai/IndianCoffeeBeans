# ☕ Indian Coffee Beans Scraper

A robust, scalable web scraper for extracting and enriching Indian coffee roaster product data. The system crawls coffee roaster websites to discover products, extract detailed information, and store it in a structured format for use in the IndianCoffeeBeans.com website.

## 📋 Features

- **Multi-platform support:** Works with Shopify, WooCommerce, and custom websites
- **Intelligent discovery:** Uses multiple strategies to find products including API endpoints, sitemaps, structured data, and HTML crawling
- **Structured extraction:** Extracts detailed product attributes using JSON-CSS selectors
- **AI-powered enrichment:** Falls back to DeepSeek LLM for missing attributes
- **Database integration:** Direct upload to Supabase
- **Export options:** CSV and JSON export capabilities
- **Caching:** Smart caching system to minimize network requests
- **Scalable:** Configurable concurrency and batch processing

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Supabase account (optional)
- DeepSeek API key (optional, for AI enrichment)

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/indian-coffee-beans-scraper.git
cd indian-coffee-beans-scraper
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables by creating a `.env` file:

```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
DEEPSEEK_API_KEY=your_deepseek_api_key
```

4. Run the setup script to initialize the project:

```bash
python -m tests.test_setup
```

### Basic Usage

Run the scraper with a CSV file containing roaster information:

```bash
python main.py --input=data/roasters.csv
```

### Advanced Usage

```bash
# Force cache refresh
python main.py --input=data/roasters.csv --refresh

# Export results as CSV
python main.py --input=data/roasters.csv --export=coffees.csv

# Export as JSON 
python main.py --input=data/roasters.csv --export=coffees.json --format=json

# Skip database upload
python main.py --input=data/roasters.csv --no-db

# Process a single roaster
python main.py --input=data/roasters.csv --roaster="Blue Tokai Coffee Roasters"

# Control concurrency
python main.py --input=data/roasters.csv --concurrency=5

# Enable debug logging
python main.py --input=data/roasters.csv --debug
```

## 🗂️ Project Structure

```
indian_coffee_beans_scraper/
├── main.py                     # CLI runner
├── config.py                   # Configuration 
├── .env                        # Environment variables
├── requirements.txt            # Dependencies

├── schemas/                    # JSON-CSS schemas
│   ├── roaster_schema.py
│   └── product_schemas.py

├── scrapers/                   # Core scraping components
│   ├── pipeline.py             # Main pipeline orchestrator
│   ├── roaster_pipeline.py     # Roaster metadata extraction
│   ├── platform_detector.py    # Website platform detection
│   ├── extractors/             # Product attribute extraction
│   │   ├── json_css_extractor.py
│   │   └── deepseek_extractor.py
│   └── discoverers/            # Product URL discovery
│       ├── discovery_manager.py
│       ├── sitemap_discoverer.py
│       ├── html_discoverer.py
│       └── structured_data_discoverer.py

├── common/                     # Shared utilities
│   ├── models.py               # Pydantic models
│   ├── utils.py                # Helper functions
│   └── cache.py                # Caching system

├── db/                         # Database integration
│   └── supabase_client.py      # Supabase client

└── data/                       # Data files
    ├── cache/                  # Cache storage
    └── output/                 # Export outputs
```

## 🧪 Testing

Run tests to verify the functionality:

```bash
# Run all tests
python -m tests.run_tests

# Run specific tests
python -m tests.test_real_discoverers
python -m tests.test_real_extractors
```

## 📊 Database Schema

The system saves data according to the schema defined in `db_structure.md`.

### Main Tables

- `roasters`: Coffee roaster information
- `coffees`: Core coffee product details
- `coffee_prices`: Price points for different sizes
- `flavor_profiles`: Flavor characteristics
- `brew_methods`: Recommended brewing methods

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.