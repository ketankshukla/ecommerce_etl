#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API Extractor module for extracting e-commerce data from various platform APIs.
"""

import logging
import pandas as pd
import os
import json
import requests
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class APIExtractor:
    """Extracts e-commerce data from various platform APIs."""
    
    def __init__(self, config):
        """
        Initialize the API Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        
        # Shopify credentials
        self.shopify_api_key = config.shopify_api_key
        self.shopify_api_secret = config.shopify_api_secret
        self.shopify_store = config.shopify_store
        
        # Amazon credentials
        self.amazon_access_key = config.amazon_access_key
        self.amazon_secret_key = config.amazon_secret_key
        self.amazon_seller_id = config.amazon_seller_id
        self.amazon_marketplace_id = config.amazon_marketplace_id
        
        # Output directory
        self.output_dir = os.path.join(config.data_dir, 'api')
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract(self, platform=None, start_date=None, end_date=None, resource_type=None):
        """
        Extract data from e-commerce platform APIs.
        
        Args:
            platform (str, optional): E-commerce platform ('shopify', 'amazon', etc.)
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            resource_type (str, optional): Type of resource to extract ('orders', 'products', 'customers')
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            ValueError: If platform is not supported
        """
        # Set default platform if not provided
        if platform is None:
            platform = 'all'
        
        # Set default resource type if not provided
        if resource_type is None:
            resource_type = 'orders'
        
        logger.info(f"Extracting {resource_type} data from {platform} platform")
        
        # Parse dates
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            # Default to 30 days ago
            start_date_obj = datetime.now() - timedelta(days=30)
            start_date = start_date_obj.strftime('%Y-%m-%d')
        
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            # Default to today
            end_date_obj = datetime.now()
            end_date = end_date_obj.strftime('%Y-%m-%d')
        
        # Extract data based on platform
        if platform.lower() == 'all':
            # Extract from all supported platforms and combine
            all_data = []
            
            # Try Shopify
            try:
                shopify_data = self._extract_from_shopify(resource_type, start_date, end_date)
                if shopify_data is not None and not shopify_data.empty:
                    shopify_data['platform'] = 'shopify'
                    all_data.append(shopify_data)
            except Exception as e:
                logger.error(f"Error extracting from Shopify: {str(e)}")
            
            # Try Amazon
            try:
                amazon_data = self._extract_from_amazon(resource_type, start_date, end_date)
                if amazon_data is not None and not amazon_data.empty:
                    amazon_data['platform'] = 'amazon'
                    all_data.append(amazon_data)
            except Exception as e:
                logger.error(f"Error extracting from Amazon: {str(e)}")
            
            # Combine data from all platforms
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                logger.info(f"Extracted {len(combined_data)} total records from all platforms")
                return combined_data
            else:
                logger.warning("No data extracted from any platform")
                return pd.DataFrame()
            
        elif platform.lower() == 'shopify':
            return self._extract_from_shopify(resource_type, start_date, end_date)
            
        elif platform.lower() == 'amazon':
            return self._extract_from_amazon(resource_type, start_date, end_date)
            
        else:
            logger.error(f"Unsupported platform: {platform}")
            raise ValueError(f"Unsupported platform: {platform}")
    
    def _extract_from_shopify(self, resource_type, start_date, end_date):
        """
        Extract data from Shopify API.
        
        Args:
            resource_type (str): Type of resource to extract ('orders', 'products', 'customers')
            start_date (str): Start date for filtering in YYYY-MM-DD format
            end_date (str): End date for filtering in YYYY-MM-DD format
        
        Returns:
            pandas.DataFrame: Extracted data
        """
        logger.info(f"Extracting {resource_type} from Shopify: {start_date} to {end_date}")
        
        # Check if credentials are provided
        if self.shopify_api_key == 'demo_key' or self.shopify_store == 'demo-store.myshopify.com':
            logger.warning("Using mock Shopify data since no real credentials are configured")
            return self._mock_shopify_data(resource_type, start_date, end_date)
        
        # In a real implementation, we would connect to the Shopify API
        # For now, we'll use mock data
        logger.warning("Using mock Shopify data (real API implementation would be here)")
        return self._mock_shopify_data(resource_type, start_date, end_date)
    
    def _extract_from_amazon(self, resource_type, start_date, end_date):
        """
        Extract data from Amazon Seller API.
        
        Args:
            resource_type (str): Type of resource to extract ('orders', 'products', 'inventory')
            start_date (str): Start date for filtering in YYYY-MM-DD format
            end_date (str): End date for filtering in YYYY-MM-DD format
        
        Returns:
            pandas.DataFrame: Extracted data
        """
        logger.info(f"Extracting {resource_type} from Amazon: {start_date} to {end_date}")
        
        # Check if credentials are provided
        if self.amazon_access_key == 'demo_key' or self.amazon_seller_id == 'demo_seller':
            logger.warning("Using mock Amazon data since no real credentials are configured")
            return self._mock_amazon_data(resource_type, start_date, end_date)
        
        # In a real implementation, we would connect to the Amazon API
        # For now, we'll use mock data
        logger.warning("Using mock Amazon data (real API implementation would be here)")
        return self._mock_amazon_data(resource_type, start_date, end_date)
    
    def _mock_shopify_data(self, resource_type, start_date, end_date):
        """
        Create mock Shopify data for demonstration purposes.
        
        Args:
            resource_type (str): Type of resource ('orders', 'products', 'customers')
            start_date (str): Start date for filtering
            end_date (str): End date for filtering
        
        Returns:
            pandas.DataFrame: Mock data
        """
        logger.info(f"Generating mock Shopify {resource_type} data")
        
        # Parse date range
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create mock data based on resource type
        if resource_type.lower() == 'orders':
            # Generate simplified order data
            data = []
            
            # Use simple customer IDs
            customer_ids = [str(i+1000) for i in range(20)]
            
            for i in range(50):  # Generate 50 orders
                # Generate a random date within range using standard Python datetime
                days_range = (end_date_obj - start_date_obj).days
                if days_range <= 0:
                    days_range = 1
                random_days = i % days_range
                order_date = start_date_obj + timedelta(days=random_days)
                
                # Create simple order record
                order = {
                    'id': str(i+1000),
                    'order_number': i+1000,
                    'customer_id': customer_ids[i % len(customer_ids)],
                    'created_at': order_date.strftime('%Y-%m-%d'),
                    'total_price': float(50 + (i % 10) * 10),
                    'currency': 'USD',
                    'financial_status': 'paid' if i % 5 != 0 else 'pending',
                    'item_count': (i % 3) + 1
                }
                data.append(order)
            
            return pd.DataFrame(data)
            
        elif resource_type.lower() == 'products':
            # Generate simplified product data
            data = []
            
            for i in range(30):  # Generate 30 products
                product = {
                    'id': str(i+2000),
                    'title': f"Shopify Product {i+1}",
                    'product_type': ['Clothing', 'Electronics', 'Home', 'Books'][i % 4],
                    'created_at': (start_date_obj - timedelta(days=i % 30)).strftime('%Y-%m-%d'),
                    'vendor': f"Vendor {(i % 5) + 1}",
                    'inventory_quantity': (i % 50) + 5,
                    'price': float(20 + (i % 20) * 5)
                }
                data.append(product)
            
            return pd.DataFrame(data)
            
        elif resource_type.lower() == 'customers':
            # Generate simplified customer data
            data = []
            
            for i in range(20):  # Generate 20 customers
                customer = {
                    'id': str(i+1000),
                    'first_name': f"First{i+1}",
                    'last_name': f"Last{i+1}",
                    'email': f"customer{i+1}@example.com",
                    'created_at': (start_date_obj - timedelta(days=i*10)).strftime('%Y-%m-%d'),
                    'orders_count': (i % 8) + 1,
                    'total_spent': float(100 + i * 50)
                }
                data.append(customer)
            
            return pd.DataFrame(data)
        
        else:
            logger.warning(f"Unsupported resource type for Shopify: {resource_type}")
            return pd.DataFrame()
    
    def _mock_amazon_data(self, resource_type, start_date, end_date):
        """
        Create mock Amazon data for demonstration purposes.
        
        Args:
            resource_type (str): Type of resource ('orders', 'products', 'inventory')
            start_date (str): Start date for filtering
            end_date (str): End date for filtering
        
        Returns:
            pandas.DataFrame: Mock data
        """
        logger.info(f"Generating mock Amazon {resource_type} data")
        
        # Parse date range
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create mock data based on resource type
        if resource_type.lower() == 'orders':
            # Generate simplified Amazon order data
            data = []
            
            for i in range(40):  # Generate 40 orders
                # Generate a random date within range using standard Python datetime
                days_range = (end_date_obj - start_date_obj).days
                if days_range <= 0:
                    days_range = 1
                random_days = i % days_range
                order_date = start_date_obj + timedelta(days=random_days)
                
                # Create simple order record
                order = {
                    'amazon_order_id': f"A{100000 + i}",
                    'purchase_date': order_date.strftime('%Y-%m-%d'),
                    'order_status': 'Shipped' if i % 5 != 0 else 'Pending',
                    'order_total': float(75 + (i % 15) * 5),
                    'number_of_items_shipped': (i % 3) + 1,
                    'shipping_address_state': ['NY', 'CA', 'TX', 'FL'][i % 4],
                    'shipping_address_country': 'US',
                    'marketplace_id': self.amazon_marketplace_id
                }
                data.append(order)
            
            return pd.DataFrame(data)
            
        elif resource_type.lower() == 'products' or resource_type.lower() == 'inventory':
            # Generate simplified product/inventory data
            data = []
            
            for i in range(30):  # Generate 30 products
                product = {
                    'sku': f"SKU-{3000 + i}",
                    'asin': f"B00{1000 + i}",
                    'product_name': f"Amazon Product {i+1}",
                    'price': float(15 + (i % 30) * 3),
                    'quantity': (i % 30) + 2,
                    'product_category': ['Electronics', 'Home', 'Kitchen', 'Sports'][i % 4],
                    'condition': 'New' if i % 10 != 0 else 'Used'
                }
                data.append(product)
            
            return pd.DataFrame(data)
        
        else:
            logger.warning(f"Unsupported resource type for Amazon: {resource_type}")
            return pd.DataFrame()
    
    def save_extracted_data(self, data, platform, resource_type):
        """
        Save extracted data to a file for reference.
        
        Args:
            data (pandas.DataFrame): Data to save
            platform (str): Platform name
            resource_type (str): Type of resource
        
        Returns:
            str: Path to saved file
        """
        if data is None or data.empty:
            logger.warning("No data to save")
            return None
        
        # Create file name with timestamp
        file_name = f"{platform}_{resource_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Save as CSV
        csv_path = os.path.join(self.output_dir, f"{file_name}.csv")
        data.to_csv(csv_path, index=False)
        logger.info(f"Saved extracted data to {csv_path}")
        
        # Optionally save as JSON
        json_path = os.path.join(self.output_dir, f"{file_name}.json")
        try:
            # Convert DataFrame to JSON
            json_data = data.to_json(orient='records', date_format='iso')
            with open(json_path, 'w') as f:
                f.write(json_data)
            logger.info(f"Saved extracted data to {json_path}")
        except Exception as e:
            logger.error(f"Error saving data as JSON: {str(e)}")
        
        return csv_path
