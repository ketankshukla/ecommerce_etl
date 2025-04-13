#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CSV Extractor module for extracting e-commerce sales data from CSV files.
"""

import logging
import pandas as pd
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class CSVExtractor:
    """Extracts e-commerce sales data from CSV files."""
    
    def __init__(self, config):
        """
        Initialize the CSV Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.orders_csv
    
    def extract(self, start_date=None, end_date=None, product_category=None, customer_segment=None):
        """
        Extract data from CSV file with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            product_category (str, optional): Filter by product category
            customer_segment (str, optional): Filter by customer segment
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
        """
        logger.info(f"Extracting data from CSV file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"CSV file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Read CSV file
            df = pd.read_csv(self.source_file, parse_dates=['order_date', 'ship_date'])
            
            # Apply date filtering if provided
            if start_date:
                start_date = pd.to_datetime(start_date)
                df = df[df['order_date'] >= start_date]
            
            if end_date:
                end_date = pd.to_datetime(end_date)
                df = df[df['order_date'] <= end_date]
            
            # Apply product category filtering if provided
            if product_category:
                df = df[df['product_category'] == product_category]
            
            # Apply customer segment filtering if provided
            if customer_segment:
                df = df[df['customer_segment'] == customer_segment]
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from CSV file")
            if not df.empty:
                logger.info(f"Date range: {df['order_date'].min().date()} to {df['order_date'].max().date()}")
                logger.info(f"Columns in CSV data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from CSV file: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample e-commerce sales data for demonstration purposes."""
        import numpy as np
        
        # Generate dates for the past 90 days
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=90)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Sample product categories and names
        product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
        product_names = {
            'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Tablet', 'Smart Watch'],
            'Clothing': ['T-shirt', 'Jeans', 'Dress', 'Jacket', 'Socks'],
            'Home & Kitchen': ['Blender', 'Coffee Maker', 'Toaster', 'Cookware Set', 'Knife Set'],
            'Books': ['Fiction Novel', 'Cookbook', 'Biography', 'Self-Help Book', 'Children Book'],
            'Toys': ['Action Figure', 'Board Game', 'Puzzle', 'Stuffed Animal', 'Building Blocks']
        }
        
        # Customer segments
        customer_segments = ['New', 'Regular', 'VIP', 'Inactive', 'Wholesale']
        
        # Payment methods
        payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery', 'Gift Card']
        
        # Shipping methods
        shipping_methods = ['Standard', 'Express', 'Next Day', 'International', 'Local Pickup']
        
        # Create empty list to store data
        data = []
        
        # Generate random order data
        order_id = 10000
        for _ in range(500):  # Generate 500 orders
            order_date = np.random.choice(dates)
            ship_date = order_date + pd.Timedelta(days=np.random.randint(1, 7))
            
            # Select random product category and name
            category = np.random.choice(product_categories)
            product_name = np.random.choice(product_names[category])
            
            # Generate random quantities and prices
            quantity = np.random.randint(1, 5)
            unit_price = round(np.random.uniform(5, 200), 2)
            total_price = round(quantity * unit_price, 2)
            discount = round(np.random.uniform(0, 0.2) * total_price, 2)
            
            # Add taxes and shipping
            tax_rate = 0.08
            tax_amount = round(tax_rate * (total_price - discount), 2)
            shipping_cost = round(np.random.uniform(3, 15), 2)
            final_price = round(total_price - discount + tax_amount + shipping_cost, 2)
            
            # Customer info
            customer_id = np.random.randint(1000, 10000)
            customer_segment = np.random.choice(customer_segments)
            city = np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami', 'Seattle', 'Boston'])
            state = np.random.choice(['NY', 'CA', 'IL', 'TX', 'FL', 'WA', 'MA'])
            country = 'USA'
            
            # Order status
            if ship_date > datetime.now():
                status = np.random.choice(['Pending', 'Processing'])
            else:
                status = np.random.choice(['Shipped', 'Delivered', 'Returned'], p=[0.3, 0.6, 0.1])
            
            # Payment and shipping
            payment_method = np.random.choice(payment_methods)
            shipping_method = np.random.choice(shipping_methods)
            
            # Add to data list
            data.append({
                'order_id': order_id,
                'order_date': order_date,
                'ship_date': ship_date,
                'customer_id': customer_id,
                'customer_segment': customer_segment,
                'product_id': np.random.randint(100, 1000),
                'product_name': product_name,
                'product_category': category,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price,
                'discount': discount,
                'tax': tax_amount,
                'shipping_cost': shipping_cost,
                'final_price': final_price,
                'payment_method': payment_method,
                'shipping_method': shipping_method,
                'status': status,
                'city': city,
                'state': state,
                'country': country
            })
            
            order_id += 1
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        # Save to CSV
        df.to_csv(self.source_file, index=False)
        logger.info(f"Created sample e-commerce sales data: {self.source_file}")
