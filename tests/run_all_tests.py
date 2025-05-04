#!/usr/bin/env python3
"""
Comprehensive test runner for Indian Coffee Beans Scraper
Runs all components: setup checks, roaster pipeline, discoverers, extractors
"""
import asyncio
import logging
import argparse
import sys
import os
import json
from datetime import datetime
# Fix Unicode output on Windows
if sys.platform == 'win32':
    os.system('color')  # Enable ANSI colors
    
    # Configure logging to handle Unicode properly
    import locale
    try:
        # Try to use UTF-8 locale if available
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        pass  # Ignore if locale is not available
        
    # Modify sys.stdout for better Unicode support
    import codecs
    import msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
    
    # Also configure the logging handler to handle Unicode
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stdout)
    
# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import test modules
from common.utils import setup_logging
from tests.test_multiple_roasters import run_tests as run_roaster_tests
from test_helpers import mock_test_multiple_roasters
from test_real_discoverers import run_tests as run_discoverer_tests
from test_real_extractors import run_tests as run_extractor_tests
from test_setup import main as run_setup_test

# Setup logging
logger = logging.getLogger(__name__)

async def run_detections(urls):
    """Run platform detector tests on a list of URLs"""
    from scrapers.platform_detector import PlatformDetector
    
    detector = PlatformDetector()
    results = {}
    
    try:
        for url in urls:
            try:
                result = await detector.detect(url)
                results[url] = result
            except Exception as e:
                results[url] = {"error": str(e)}
    finally:
        await detector.close()
        
    return results

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run comprehensive tests for Indian Coffee Beans Scraper')
    parser.add_argument('--component', choices=['all', 'setup', 'detector', 'roaster', 'discoverer', 'extractor'], 
                      default='all', help='Component to test (default: all)')
    parser.add_argument('--mode', choices=['real', 'mock', 'both'], default='both',
                      help='Test mode: real, mock, or both (default: both)')
    parser.add_argument('--refresh', action='store_true',
                      help='Force refresh cache for all tests')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    
    logger.info(f"Starting comprehensive test run for component: {args.component}, mode: {args.mode}")
    logger.info(f"Log file: {log_file}")
    
    # Create results directory structure
    results_dir = 'test_results'
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(os.path.join(results_dir, 'discoverers'), exist_ok=True)
    os.makedirs(os.path.join(results_dir, 'extractors'), exist_ok=True)
    os.makedirs(os.path.join(results_dir, 'mock_tests'), exist_ok=True)
    
    # Initialize comprehensive summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "components_tested": [],
        "test_mode": args.mode
    }
    
    # Step 1: Run setup tests if selected
    if args.component in ['all', 'setup']:
        logger.info("=== Running Setup Tests ===")
        setup_success = run_setup_test()
        summary["components_tested"].append("setup")
        summary["setup"] = {
            "success": setup_success
        }
    
    # Step 2: Run platform detector tests if selected
    if args.component in ['all', 'detector']:
        logger.info("=== Running Platform Detector Tests ===")
        test_urls = [
            "https://bluetokaicoffee.com",  # Likely Shopify
            "https://www.subko.coffee",     # Likely WooCommerce
            "https://kcroasters.com",       # Might be custom
            "https://www.corridorseven.coffee" # Another platform
        ]
        detector_results = await run_detections(test_urls)
        summary["components_tested"].append("detector")
        summary["detector"] = {
            "urls_tested": len(test_urls),
            "results": detector_results
        }
        logger.info(f"Detected platforms: {', '.join([f'{url}: {result.get('platform', 'unknown')}' for url, result in detector_results.items()])}")
    
    # Step 3: Run roaster pipeline tests if selected
    if args.component in ['all', 'roaster']:
        # Run real-world tests
        if args.mode in ['real', 'both']:
            logger.info("=== Running Real-World Roaster Tests ===")
            try:
                await run_roaster_tests()
                logger.info("Real-world roaster tests completed")
            except Exception as e:
                logger.exception(f"Error in real-world roaster tests: {str(e)}")
        
        # Run mock tests
        if args.mode in ['mock', 'both']:
            logger.info("=== Running Mock Roaster Tests ===")
            try:
                await mock_test_multiple_roasters()
                logger.info("Mock roaster tests completed")
            except Exception as e:
                logger.exception(f"Error in mock roaster tests: {str(e)}")
                
        # Add to summary
        summary["components_tested"].append("roaster")
        
        # Try to load test summaries
        roaster_summary = {}
        real_summary_path = os.path.join(results_dir, 'summary.json')
        mock_summary_path = os.path.join(results_dir, 'mock_tests', 'mock_summary.json')
        
        if os.path.exists(real_summary_path) and args.mode in ['real', 'both']:
            with open(real_summary_path, 'r') as f:
                real_summary = json.load(f)
                roaster_summary["real"] = real_summary
                
        if os.path.exists(mock_summary_path) and args.mode in ['mock', 'both']:
            with open(mock_summary_path, 'r') as f:
                mock_summary = json.load(f)
                roaster_summary["mock"] = mock_summary
                
        summary["roaster"] = roaster_summary
    
    # Step 4: Run discoverer tests if selected
    if args.component in ['all', 'discoverer']:
        logger.info("=== Running Product Discoverer Tests ===")
        try:
            await run_discoverer_tests()
            logger.info("Discoverer tests completed")
            
            # Add to summary
            summary["components_tested"].append("discoverer")
            
            # Try to load test summary
            discoverer_summary_path = os.path.join(results_dir, 'discoverers', 'discovery_summary.json')
            if os.path.exists(discoverer_summary_path):
                with open(discoverer_summary_path, 'r') as f:
                    discoverer_summary = json.load(f)
                    summary["discoverer"] = discoverer_summary
        except Exception as e:
            logger.exception(f"Error in discoverer tests: {str(e)}")
    
    # Step 5: Run extractor tests if selected
    if args.component in ['all', 'extractor']:
        logger.info("=== Running Product Extractor Tests ===")
        try:
            await run_extractor_tests()
            logger.info("Extractor tests completed")
            
            # Add to summary
            summary["components_tested"].append("extractor")
            
            # Try to load test summary
            extractor_summary_path = os.path.join(results_dir, 'extractors', 'extraction_summary.json')
            if os.path.exists(extractor_summary_path):
                with open(extractor_summary_path, 'r') as f:
                    extractor_summary = json.load(f)
                    summary["extractor"] = extractor_summary
        except Exception as e:
            logger.exception(f"Error in extractor tests: {str(e)}")
    
    # Step 6: Run pytest unit tests if selected
    if args.component in ['all']:
        logger.info("=== Running Unit Tests ===")
        try:
            import pytest
            result = pytest.main(["-xvs", "tests/test_discoverers.py", "tests/test_extractors.py"])
            logger.info(f"Unit tests completed with result code: {result}")
            
            # Add to summary
            summary["components_tested"].append("unit_tests")
            summary["unit_tests"] = {
                "result_code": result,
                "success": result == 0
            }
        except Exception as e:
            logger.exception(f"Error running unit tests: {str(e)}")
    
    # Generate comprehensive test summary
    logger.info("=== Generating Comprehensive Test Report ===")
    
    # Calculate success rates
    success_rates = {}
    total_tested = 0
    total_success = 0
    
    # Roaster success rate
    if "roaster" in summary:
        roaster_real_tested = summary["roaster"].get("real", {}).get("total_tested", 0)
        roaster_real_success = summary["roaster"].get("real", {}).get("successful", 0)
        roaster_mock_tested = summary["roaster"].get("mock", {}).get("total_tested", 0)
        roaster_mock_success = summary["roaster"].get("mock", {}).get("successful", 0)
        
        if args.mode == "real":
            roaster_total = roaster_real_tested
            roaster_success = roaster_real_success
        elif args.mode == "mock":
            roaster_total = roaster_mock_tested
            roaster_success = roaster_mock_success
        else:
            roaster_total = roaster_real_tested + roaster_mock_tested
            roaster_success = roaster_real_success + roaster_mock_success
            
        if roaster_total > 0:
            success_rates["roaster"] = f"{roaster_success}/{roaster_total} ({roaster_success/roaster_total*100:.1f}%)"
            total_tested += roaster_total
            total_success += roaster_success
    
    # Discoverer success rate
    if "discoverer" in summary:
        disc_tested = summary["discoverer"].get("total_tested", 0)
        disc_success = summary["discoverer"].get("successful", 0)
        
        if disc_tested > 0:
            success_rates["discoverer"] = f"{disc_success}/{disc_tested} ({disc_success/disc_tested*100:.1f}%)"
            total_tested += disc_tested
            total_success += disc_success
    
    # Extractor success rate
    if "extractor" in summary:
        ext_tested = summary["extractor"].get("total_tested", 0)
        ext_success = summary["extractor"].get("successful", 0)
        
        if ext_tested > 0:
            success_rates["extractor"] = f"{ext_success}/{ext_tested} ({ext_success/ext_tested*100:.1f}%)"
            total_tested += ext_tested
            total_success += ext_success
    
    # Overall success rate
    if total_tested > 0:
        success_rates["overall"] = f"{total_success}/{total_tested} ({total_success/total_tested*100:.1f}%)"
    
    # Add success rates to summary
    summary["success_rates"] = success_rates
    
    # Save comprehensive summary
    summary_path = os.path.join(results_dir, "comprehensive_test_summary.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Comprehensive test summary saved to {summary_path}")
    
    # Print overall results
    logger.info("\n===== OVERALL TEST RESULTS =====")
    logger.info(f"Components tested: {', '.join(summary['components_tested'])}")
    
    for component, rate in success_rates.items():
        logger.info(f"{component.capitalize()} success rate: {rate}")
    
    logger.info("Test run completed")
    
    # Return success status
    return total_success == total_tested if total_tested > 0 else None

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result is None or result else 1)