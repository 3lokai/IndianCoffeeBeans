#!/usr/bin/env python3
"""
Indian Coffee Beans Scraper - Main CLI runner
"""
import os
import sys
import asyncio
import argparse
import csv
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from scrapers.pipeline import Pipeline
from db.supabase_client import SupabaseClient
from common.utils import setup_logging
from config import REFRESH_CACHE, CSV_EXPORT_PATH, MAX_CONCURRENT_EXTRACTORS

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Indian Coffee Beans Scraper - Extract coffee product data from Indian roasters"
    )
    
    # Input options
    parser.add_argument(
        "--input", 
        required=True,
        help="CSV file with roaster information (name,website_url)"
    )
    
    # Cache options
    parser.add_argument(
        "--refresh", 
        action="store_true",
        help="Force refresh data cache"
    )
    
    # Output options
    parser.add_argument(
        "--export", 
        help="Export results to CSV file"
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Export format (default: csv)"
    )
    
    # Database options
    parser.add_argument(
        "--no-db", 
        action="store_true",
        help="Skip database upload"
    )
    
    # Concurrency options
    parser.add_argument(
        "--concurrency", 
        type=int,
        default=MAX_CONCURRENT_EXTRACTORS,
        help=f"Maximum number of concurrent extractors (default: {MAX_CONCURRENT_EXTRACTORS})"
    )
    
    # Logging options
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Enable debug logging"
    )
    
    # Individual roaster processing
    parser.add_argument(
        "--roaster",
        help="Process a single roaster by name (must be in the input CSV)"
    )
    
    return parser.parse_args()

def load_roasters_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """
    Load roaster information from CSV file.
    
    Expected CSV format:
    name,website_url,[optional_fields]
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        List of roaster dicts with at least 'name' and 'website_url'
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
    roasters = []
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        if "name" not in reader.fieldnames or "website_url" not in reader.fieldnames:
            raise ValueError("CSV must have 'name' and 'website_url' columns")
            
        for row in reader:
            # Skip empty rows
            if not row["name"] or not row["website_url"]:
                continue
                
            # Add to list
            roasters.append(row)
    
    logger.info(f"Loaded {len(roasters)} roasters from {csv_path}")
    return roasters

def export_to_csv(results: Dict[str, Any], export_path: Optional[str] = None) -> str:
    """
    Export results to CSV file.
    
    Args:
        results: Dictionary containing the results
        export_path: Path to export the CSV file (optional)
        
    Returns:
        Path to the exported CSV file
    """
    # Use provided path or default
    if not export_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = os.path.join(CSV_EXPORT_PATH, f"coffees_{timestamp}.csv")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(export_path), exist_ok=True)
    
    # Extract coffees from results
    coffees = results.get("coffees", [])
    
    if not coffees:
        logger.warning("No coffees to export")
        return ""
        
    # Flatten coffees for CSV export
    flattened_coffees = []
    
    for coffee in coffees:
        # Basic coffee details
        flat_coffee = {
            "id": coffee.get("id", ""),
            "roaster_id": coffee.get("roaster_id", ""),
            "roaster_name": coffee.get("roaster_name", ""),
            "name": coffee.get("name", ""),
            "slug": coffee.get("slug", ""),
            "description": coffee.get("description", ""),
            "roast_level": coffee.get("roast_level", ""),
            "bean_type": coffee.get("bean_type", ""),
            "processing_method": coffee.get("processing_method", ""),
            "image_url": coffee.get("image_url", ""),
            "direct_buy_url": coffee.get("direct_buy_url", ""),
            "region_name": coffee.get("region_name", ""),
            "is_seasonal": str(coffee.get("is_seasonal", False)),
            "is_available": str(coffee.get("is_available", True)),
            "is_single_origin": str(coffee.get("is_single_origin", True)),
            "deepseek_enriched": str(coffee.get("deepseek_enriched", False))
        }
        
        # Add flavor profiles as semicolon-separated string
        flavor_profiles = coffee.get("flavor_profiles", [])
        flat_coffee["flavor_profiles"] = "; ".join(flavor_profiles)
        
        # Add brew methods as semicolon-separated string
        brew_methods = coffee.get("brew_methods", [])
        flat_coffee["brew_methods"] = "; ".join(brew_methods)
        
        # Add prices with size in column name
        prices = coffee.get("prices", {})
        for size_grams, price in prices.items():
            flat_coffee[f"price_{size_grams}g"] = price
            
        flattened_coffees.append(flat_coffee)
    
    # Write CSV
    if flattened_coffees:
        # Collect all possible fields
        fieldnames = set()
        for coffee in flattened_coffees:
            fieldnames.update(coffee.keys())
        
        fieldnames = sorted(list(fieldnames))
        
        with open(export_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened_coffees)
        
        logger.info(f"Exported {len(flattened_coffees)} coffees to {export_path}")
        return export_path
    
    logger.warning("No coffees to export after flattening")
    return ""

def export_to_json(results: Dict[str, Any], export_path: Optional[str] = None) -> str:
    """
    Export results to JSON file.
    
    Args:
        results: Dictionary containing the results
        export_path: Path to export the JSON file (optional)
        
    Returns:
        Path to the exported JSON file
    """
    # Use provided path or default
    if not export_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = os.path.join(CSV_EXPORT_PATH, f"coffees_{timestamp}.json")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(export_path), exist_ok=True)
    
    # Write JSON
    with open(export_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Exported results to {export_path}")
    return export_path

async def run_pipeline(args):
    """
    Run the scraping pipeline.
    
    Args:
        args: Command-line arguments
    """
    # Initialize database client
    db_client = None
    if not args.no_db:
        db_client = SupabaseClient()
    
    # Initialize pipeline
    pipeline = Pipeline(
        db_client=db_client,
        refresh_cache=args.refresh or REFRESH_CACHE,
        max_concurrency=args.concurrency
    )
    
    try:
        # Load roasters from CSV
        all_roasters = load_roasters_from_csv(args.input)
        
        # Filter to single roaster if specified
        if args.roaster:
            roasters = [r for r in all_roasters if r["name"].lower() == args.roaster.lower()]
            if not roasters:
                logger.error(f"Roaster '{args.roaster}' not found in input CSV")
                return
        else:
            roasters = all_roasters
        
        logger.info(f"Starting pipeline with {len(roasters)} roasters")
        
        # Run pipeline
        pipeline_result = await pipeline.process_roaster_list(roasters)
        stats = pipeline_result["stats"]
        roasters_data = pipeline_result["roasters"]
        coffees_data = pipeline_result["coffees"]
        
        # Print statistics
        logger.info("\n===== PIPELINE RESULTS =====")
        logger.info(f"Roasters processed: {stats['roasters_processed']}/{len(roasters)}")
        logger.info(f"Products discovered: {stats['products_discovered']}")
        logger.info(f"Products extracted: {stats['products_extracted']}")
        logger.info(f"Products enriched with DeepSeek: {stats['products_enriched']}")
        logger.info(f"Products uploaded to database: {stats['products_uploaded']}")
        logger.info(f"Errors encountered: {stats['errors']}")
        
        # Export results if requested
        if args.export:
            # Collect roaster and product data
            results = {
                "timestamp": datetime.now().isoformat(),
                "stats": stats,
                "roasters": roasters_data,
                "coffees": coffees_data
            }
            
            # Export in requested format
            if args.format == "csv":
                export_path = export_to_csv(results, args.export)
            else:
                export_path = export_to_json(results, args.export)
            
            if export_path:
                logger.info(f"Results exported to {export_path}")
        else:
            logger.info("No export requested: results will not be saved to CSV or JSON.")
    
    finally:
        # Close pipeline resources
        await pipeline.close()

async def main():
    # Parse command-line arguments
    args = parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    logger.info(f"Log file: {log_file}")
    
    # Run pipeline
    await run_pipeline(args)

if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())