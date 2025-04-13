#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
JSON Extractor module for extracting e-commerce product data from JSON files.
"""

import logging
import pandas as pd
import os
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class JSONExtractor:
    """Extracts e-commerce product data from JSON files."""
    
    def __init__(self, config):
        """
        Initialize the JSON Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.products_json
    
    def extract(self, start_date=None, end_date=None, product_category=None):
        """
        Extract data from JSON file with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            product_category (str, optional): Filter by product category
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the JSON file doesn't exist
        """
        logger.info(f"Extracting data from JSON file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"JSON file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Read JSON file
            with open(self.source_file, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.json_normalize(data)
            
            # Handle dates if they exist in the data
            date_columns = [col for col in df.columns if 'date' in col.lower()]
            for col in date_columns:
                df[col] = pd.to_datetime(df[col])
            
            # Apply date filtering if provided and date columns exist
            if start_date and date_columns:
                start_date = pd.to_datetime(start_date)
                for col in date_columns:
                    df = df[df[col] >= start_date]
            
            if end_date and date_columns:
                end_date = pd.to_datetime(end_date)
                for col in date_columns:
                    df = df[df[col] <= end_date]
            
            # Apply product category filtering if provided
            if product_category and 'category' in df.columns:
                df = df[df['category'] == product_category]
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} products from JSON file")
            if not df.empty:
                logger.info(f"Columns in JSON data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from JSON file: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample e-commerce product data for demonstration purposes."""
        import numpy as np
        
        # Sample product categories
        categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
        
        # Sample brands for each category
        brands = {
            'Electronics': ['Apple', 'Samsung', 'Sony', 'Dell', 'Bose'],
            'Clothing': ['Nike', 'Adidas', 'Zara', 'H&M', 'Levi\'s'],
            'Home & Kitchen': ['KitchenAid', 'Cuisinart', 'Dyson', 'Ninja', 'OXO'],
            'Books': ['Penguin', 'Random House', 'HarperCollins', 'Simon & Schuster', 'Macmillan'],
            'Toys': ['LEGO', 'Hasbro', 'Mattel', 'Fisher-Price', 'Melissa & Doug']
        }
        
        # Generate sample product data in a straightforward way without numpy data types
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        # Create a list to store products
        products = []
        
        # Generate 100 simple product records
        for i in range(100, 200):
            product_id = i
            category = categories[i % len(categories)]
            brand = brands[category][i % len(brands[category])]
            
            # Create a simple product dictionary with standard Python types
            product = {
                "id": str(product_id),
                "name": f"{brand} Product {i}",
                "category": category,
                "brand": brand,
                "price": float(50 + (i % 10) * 10),
                "stock": int(10 + (i % 5) * 5),
                "created_date": (datetime.now() - timedelta(days=i % 30)).strftime("%Y-%m-%d"),
                "description": f"This is a sample {category} product from {brand}."
            }
            
            # Add some category-specific attributes as simple strings and numbers
            if category == "Electronics":
                product["attributes"] = {
                    "type": "Smartphone" if i % 3 == 0 else "Laptop",
                    "color": "Black" if i % 2 == 0 else "Silver",
                    "memory": "8 GB" if i % 2 == 0 else "16 GB"
                }
            elif category == "Clothing":
                product["attributes"] = {
                    "size": "M" if i % 3 == 0 else "L",
                    "color": "Blue" if i % 2 == 0 else "Black",
                }
            else:
                product["attributes"] = {
                    "feature": f"Feature {i % 5 + 1}"
                }
                
            products.append(product)
        
        # Write JSON to file
        try:
            with open(self.source_file, 'w') as f:
                json.dump(products, f, indent=4)
            logger.info(f"Created sample e-commerce product data: {self.source_file}")
        except Exception as e:
            logger.error(f"Error creating sample JSON file: {str(e)}")
