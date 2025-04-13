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
        
        # Generate dates for the product creation/update
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=365)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Create empty list for products
        products = []
        
        # Generate random product data
        for product_id in range(100, 300):
            # Select random category and brand
            category = np.random.choice(categories)
            brand = np.random.choice(brands[category])
            
            # Generate product attributes based on category
            if category == 'Electronics':
                names = ['Smartphone', 'Laptop', 'Headphones', 'Tablet', 'Smart Watch', 'Camera', 'Speaker', 'TV']
                name = f"{brand} {np.random.choice(names)}"
                attributes = {
                    "display": np.random.choice(["HD", "Full HD", "4K", "OLED", "QLED"]),
                    "memory": f"{np.random.choice([4, 8, 16, 32, 64, 128])} GB",
                    "storage": f"{np.random.choice([128, 256, 512, 1024])} GB",
                    "color": np.random.choice(["Black", "Silver", "White", "Gold", "Blue"])
                }
            elif category == 'Clothing':
                names = ['T-shirt', 'Jeans', 'Dress', 'Jacket', 'Socks', 'Sweater', 'Hoodie', 'Pants']
                name = f"{brand} {np.random.choice(names)}"
                attributes = {
                    "size": np.random.choice(["XS", "S", "M", "L", "XL", "XXL"]),
                    "color": np.random.choice(["Black", "White", "Blue", "Red", "Green", "Yellow", "Grey"]),
                    "material": np.random.choice(["Cotton", "Polyester", "Wool", "Denim", "Leather"]),
                    "gender": np.random.choice(["Men", "Women", "Unisex"])
                }
            elif category == 'Home & Kitchen':
                names = ['Blender', 'Coffee Maker', 'Toaster', 'Cookware Set', 'Knife Set', 'Air Fryer', 'Mixer', 'Vacuum']
                name = f"{brand} {np.random.choice(names)}"
                attributes = {
                    "color": np.random.choice(["Black", "Silver", "White", "Red", "Blue"]),
                    "material": np.random.choice(["Stainless Steel", "Plastic", "Glass", "Ceramic", "Aluminum"]),
                    "power": f"{np.random.choice([300, 500, 750, 1000, 1200, 1500])} W",
                    "warranty": f"{np.random.choice([1, 2, 3, 5])} years"
                }
            elif category == 'Books':
                genres = ['Fiction', 'Biography', 'Self-Help', 'Cooking', 'History', 'Science', 'Fantasy', 'Mystery']
                genre = np.random.choice(genres)
                name = f"{genre} Book by {brand}"
                attributes = {
                    "author": f"Author {np.random.randint(1, 100)}",
                    "pages": np.random.randint(100, 500),
                    "language": "English",
                    "format": np.random.choice(["Hardcover", "Paperback", "Ebook", "Audiobook"]),
                    "genre": genre
                }
            else:  # Toys
                ages = ['0-2', '3-5', '6-9', '10-13', '14+']
                names = ['Action Figure', 'Board Game', 'Puzzle', 'Stuffed Animal', 'Building Blocks', 'Doll', 'Remote Control Car']
                name = f"{brand} {np.random.choice(names)}"
                attributes = {
                    "age_range": np.random.choice(ages),
                    "material": np.random.choice(["Plastic", "Wood", "Fabric", "Metal", "Rubber"]),
                    "batteries_required": np.random.choice([True, False]),
                    "safety_certified": np.random.choice([True, False], p=[0.8, 0.2])
                }
            
            # Generate price and stock data
            base_price = np.random.uniform(10, 500)
            price = round(base_price, 2)
            sale_price = round(base_price * np.random.uniform(0.7, 0.95), 2) if np.random.random() < 0.3 else None
            stock = np.random.randint(0, 100)
            stock_status = "In Stock" if stock > 0 else "Out of Stock"
            
            # Generate dates
            created_date = np.random.choice(dates).strftime('%Y-%m-%d')
            updated_date = (pd.to_datetime(created_date) + pd.Timedelta(days=np.random.randint(1, 180))).strftime('%Y-%m-%d')
            
            # Generate product ratings
            rating = round(np.random.uniform(1, 5), 1)
            ratings_count = np.random.randint(0, 500)
            
            # Create product dictionary
            product = {
                "id": str(product_id),
                "name": name,
                "sku": f"SKU-{product_id:05d}",
                "category": category,
                "subcategory": attributes.get("genre") if category == "Books" else None,
                "brand": brand,
                "price": price,
                "sale_price": sale_price,
                "currency": "USD",
                "cost": round(price * 0.6, 2),  # 60% of retail price as cost
                "stock": stock,
                "stock_status": stock_status,
                "attributes": attributes,
                "description": f"This is a {name} from {brand}. It is a high-quality product in the {category} category.",
                "tags": [category, brand, name.split()[-1]],
                "created_at": created_date,
                "updated_at": updated_date,
                "rating": rating,
                "ratings_count": ratings_count,
                "is_featured": np.random.choice([True, False], p=[0.1, 0.9]),
                "is_active": np.random.choice([True, False], p=[0.9, 0.1])
            }
            
            products.append(product)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        # Save to JSON file
        with open(self.source_file, 'w') as f:
            json.dump(products, f, indent=2)
        
        logger.info(f"Created sample e-commerce product data: {self.source_file}")
