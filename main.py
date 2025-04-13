#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main entry point for the E-commerce Sales ETL pipeline.
"""

import argparse
import logging
import sys
import os
from datetime import datetime

# Import local modules
from config import Config
from orchestrator import Orchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'logs', f'etl_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='E-commerce Sales ETL Pipeline')
    parser.add_argument('--source', choices=['csv', 'json', 'excel', 'pdf', 'db', 'xml', 'ftp', 'email', 'api', 'all'], 
                        default='all', help='Specify the data source to process')
    parser.add_argument('--platform', choices=['shopify', 'amazon', 'ebay', 'etsy', 'all'],
                        default='all', help='Specify the e-commerce platform for API extraction')
    parser.add_argument('--start-date', type=str,
                        help='Start date for data filtering in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                        help='End date for data filtering in YYYY-MM-DD format')
    parser.add_argument('--product-category', type=str,
                        help='Filter by product category')
    parser.add_argument('--customer-segment', type=str,
                        help='Filter by customer segment')
    parser.add_argument('--config', type=str, default='config.py',
                        help='Path to configuration file')
    parser.add_argument('--report', action='store_true',
                        help='Generate summary reports after processing')
    
    return parser.parse_args()

def main():
    """Main function to run the ETL pipeline."""
    logger.info("Starting E-commerce Sales ETL Pipeline")
    
    # Parse command line arguments
    args = parse_args()
    
    # Load configuration
    config = Config()
    logger.info(f"Using source: {args.source}")
    
    # Set up data source options
    data_sources = []
    if args.source == 'all':
        data_sources = ['csv', 'json', 'excel', 'pdf', 'db', 'xml', 'ftp', 'email', 'api']
    else:
        data_sources = [args.source]
    
    # Initialize orchestrator
    orchestrator = Orchestrator(config)
    
    # Run ETL pipeline for each data source
    for source in data_sources:
        try:
            logger.info(f"Processing {source} data source")
            orchestrator.run_etl(
                source, 
                platform=args.platform if args.platform != 'all' else None,
                start_date=args.start_date or config.default_start_date,
                end_date=args.end_date or config.default_end_date,
                product_category=args.product_category,
                customer_segment=args.customer_segment
            )
        except Exception as e:
            logger.error(f"Error processing {source} data source: {str(e)}")
    
    # Generate reports if requested
    if args.report:
        try:
            logger.info("Generating reports")
            orchestrator.generate_reports()
        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")
    
    logger.info("E-commerce Sales ETL Pipeline completed")

if __name__ == "__main__":
    main()
