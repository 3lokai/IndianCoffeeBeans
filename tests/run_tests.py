#!/usr/bin/env python3
"""
Test runner for RoasterPipeline - runs both real-world and mock tests
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
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
    
# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_multiple_roasters import run_tests as run_real_tests
from test_helpers import mock_test_multiple_roasters

# Setup logging
def setup_logging(log_level=logging.INFO):
    """Setup logging with proper formatting"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Get current timestamp for log filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/test_run_{timestamp}.log'
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return log_file

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run RoasterPipeline tests')
    parser.add_argument('--mode', choices=['real', 'mock', 'both'], default='both',
                      help='Test mode: real, mock, or both (default)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting test run in {args.mode} mode")
    logger.info(f"Log file: {log_file}")
    
    # Create results directory
    results_dir = 'test_results'
    os.makedirs(results_dir, exist_ok=True)
    
    # Run tests based on mode
    if args.mode in ['real', 'both']:
        logger.info("=== Running real-world tests ===")
        try:
            await run_real_tests()
            logger.info("Real-world tests completed")
        except Exception as e:
            logger.exception(f"Error in real-world tests: {str(e)}")
    
    if args.mode in ['mock', 'both']:
        logger.info("=== Running mock tests ===")
        try:
            await mock_test_multiple_roasters()
            logger.info("Mock tests completed")
        except Exception as e:
            logger.exception(f"Error in mock tests: {str(e)}")
    
    # Generate combined report if both test types were run
    if args.mode == 'both':
        try:
            logger.info("Generating combined report...")
            
            # Load real test summary
            real_summary_path = os.path.join(results_dir, 'summary.json')
            real_summary = {}
            if os.path.exists(real_summary_path):
                with open(real_summary_path, 'r') as f:
                    real_summary = json.load(f)
            
            # Load mock test summary
            mock_summary_path = os.path.join(results_dir, 'mock_tests', 'mock_summary.json')
            mock_summary = {}
            if os.path.exists(mock_summary_path):
                with open(mock_summary_path, 'r') as f:
                    mock_summary = json.load(f)
            
            # Create combined report
            combined_report = {
                "timestamp": datetime.now().isoformat(),
                "real_tests": real_summary,
                "mock_tests": mock_summary,
                "summary": {
                    "real_tests_success_rate": f"{real_summary.get('successful', 0)}/{real_summary.get('total_tested', 0)}",
                    "mock_tests_success_rate": f"{mock_summary.get('successful', 0)}/{mock_summary.get('total_tested', 0)}",
                    "total_success_rate": f"{(real_summary.get('successful', 0) + mock_summary.get('successful', 0))}/{(real_summary.get('total_tested', 0) + mock_summary.get('total_tested', 0))}"
                }
            }
            
            # Save combined report
            combined_report_path = os.path.join(results_dir, 'combined_report.json')
            with open(combined_report_path, 'w') as f:
                json.dump(combined_report, f, indent=2)
            
            logger.info(f"Combined report saved to {combined_report_path}")
            
        except Exception as e:
            logger.exception(f"Error generating combined report: {str(e)}")
    
    logger.info("Test run completed")

if __name__ == "__main__":
    asyncio.run(main())