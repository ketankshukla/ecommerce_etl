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
            
            # Combine data if available
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"Extracted {len(combined_df)} rows from all platforms")
                return combined_df
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
        
        try:
            # Build API URL
            base_url = f"https://{self.shopify_store}/admin/api/2023-07"
            
            if resource_type.lower() == 'orders':
                endpoint = f"{base_url}/orders.json"
                params = {
                    'status': 'any',
                    'created_at_min': start_date,
                    'created_at_max': end_date,
                    'limit': 250  # Maximum allowed by Shopify
                }
            elif resource_type.lower() == 'products':
                endpoint = f"{base_url}/products.json"
                params = {
                    'limit': 250
                }
            elif resource_type.lower() == 'customers':
                endpoint = f"{base_url}/customers.json"
                params = {
                    'created_at_min': start_date,
                    'created_at_max': end_date,
                    'limit': 250
                }
            else:
                logger.error(f"Unsupported resource type for Shopify: {resource_type}")
                return pd.DataFrame()
            
            # Set up authentication
            auth = (self.shopify_api_key, self.shopify_api_secret)
            
            # Make API request
            response = requests.get(endpoint, params=params, auth=auth)
            
            # Check response
            if response.status_code == 200:
                data = response.json()
                
                # Save raw response for reference
                output_file = os.path.join(self.output_dir, f"shopify_{resource_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Convert to DataFrame
                if resource_type.lower() in data:
                    df = pd.json_normalize(data[resource_type])
                    logger.info(f"Extracted {len(df)} {resource_type} from Shopify")
                    return df
                else:
                    logger.warning(f"No {resource_type} found in Shopify response")
                    return pd.DataFrame()
            else:
                logger.error(f"Shopify API error: {response.status_code} - {response.text}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error extracting from Shopify: {str(e)}")
            logger.info("Falling back to mock data...")
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
        
        try:
            # Note: Implementing a real Amazon Seller API connection would require the sp-api
            # Python package and comprehensive authentication and request logic.
            # For now, we'll use mock data as a placeholder.
            logger.warning("Real Amazon API implementation requires sp-api package")
            logger.info("Using mock Amazon data as a placeholder")
            return self._mock_amazon_data(resource_type, start_date, end_date)
                
        except Exception as e:
            logger.error(f"Error extracting from Amazon: {str(e)}")
            logger.info("Falling back to mock data...")
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
        import numpy as np
        
        logger.info(f"Generating mock Shopify {resource_type} data")
        
        # Parse date range
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate dates within range
        date_range = pd.date_range(start=start_date_obj, end=end_date_obj)
        
        # Create mock data based on resource type
        if resource_type.lower() == 'orders':
            # Generate order data
            orders = []
            
            # Use consistent customer IDs
            customer_ids = [f"{i+1000000000}" for i in range(20)]
            
            for i in range(100):  # Generate 100 mock orders
                # Select random date from range
                order_date = np.random.choice(date_range)
                
                # Generate order data
                order_id = f"{i+1000}"
                customer_id = np.random.choice(customer_ids)
                
                # Generate line items
                num_items = np.random.randint(1, 5)
                line_items = []
                subtotal = 0
                
                for j in range(num_items):
                    price = round(np.random.uniform(10, 200), 2)
                    quantity = np.random.randint(1, 4)
                    item_total = price * quantity
                    subtotal += item_total
                    
                    line_items.append({
                        'id': f"{order_id}-{j+1}",
                        'product_id': f"{j+2000}",
                        'title': f"Shopify Product {j+1}",
                        'quantity': quantity,
                        'price': price,
                        'total_price': item_total
                    })
                
                # Calculate totals
                taxes = round(subtotal * 0.08, 2)
                shipping = round(np.random.uniform(5, 15), 2)
                total = subtotal + taxes + shipping
                
                # Generate full order
                order = {
                    'id': order_id,
                    'order_number': int(order_id),
                    'customer.id': customer_id,
                    'created_at': order_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'processed_at': order_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'updated_at': order_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'subtotal_price': subtotal,
                    'total_tax': taxes,
                    'total_price': total,
                    'shipping_price': shipping,
                    'currency': 'USD',
                    'financial_status': np.random.choice(['paid', 'pending', 'refunded'], p=[0.8, 0.15, 0.05]),
                    'fulfillment_status': np.random.choice(['fulfilled', 'partial', 'unfulfilled'], p=[0.7, 0.1, 0.2]),
                    'line_items.count': num_items
                }
                
                # Add order to list
                orders.append(order)
            
            # Convert to DataFrame
            df = pd.DataFrame(orders)
            logger.info(f"Generated {len(df)} mock Shopify orders")
            return df
            
        elif resource_type.lower() == 'products':
            # Generate product data
            products = []
            
            for i in range(50):  # Generate 50 mock products
                # Calculate inventory level (some out of stock)
                inventory = np.random.randint(0, 100) if np.random.random() > 0.1 else 0
                
                # Generate product data
                product = {
                    'id': f"{i+2000}",
                    'title': f"Shopify Product {i+1}",
                    'handle': f"shopify-product-{i+1}",
                    'product_type': np.random.choice(['Clothing', 'Electronics', 'Home', 'Books']),
                    'created_at': (start_date_obj - timedelta(days=np.random.randint(30, 365))).strftime('%Y-%m-%dT%H:%M:%S'),
                    'updated_at': (start_date_obj - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%dT%H:%M:%S'),
                    'published_at': (start_date_obj - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%dT%H:%M:%S'),
                    'vendor': np.random.choice(['Vendor A', 'Vendor B', 'Vendor C']),
                    'inventory_quantity': inventory,
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny',
                    'price': round(np.random.uniform(10, 500), 2),
                    'compare_at_price': round(np.random.uniform(20, 600), 2) if np.random.random() > 0.7 else None,
                    'requires_shipping': True,
                    'taxable': True,
                    'weight': round(np.random.uniform(0.1, 10), 2),
                    'weight_unit': 'kg',
                    'variants.count': np.random.randint(1, 5)
                }
                
                # Add product to list
                products.append(product)
            
            # Convert to DataFrame
            df = pd.DataFrame(products)
            logger.info(f"Generated {len(df)} mock Shopify products")
            return df
            
        elif resource_type.lower() == 'customers':
            # Generate customer data
            customers = []
            
            # Customer IDs consistent with orders
            customer_ids = [f"{i+1000000000}" for i in range(20)]
            
            for i, customer_id in enumerate(customer_ids):
                # Generate registration date
                created_at = (start_date_obj - timedelta(days=np.random.randint(30, 365))).strftime('%Y-%m-%dT%H:%M:%S')
                
                # Generate customer data
                customer = {
                    'id': customer_id,
                    'first_name': f"FirstName{i+1}",
                    'last_name': f"LastName{i+1}",
                    'email': f"customer{i+1}@example.com",
                    'phone': f"+1-{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}",
                    'created_at': created_at,
                    'updated_at': created_at,
                    'orders_count': np.random.randint(1, 10),
                    'total_spent': round(np.random.uniform(100, 2000), 2),
                    'currency': 'USD',
                    'accepts_marketing': np.random.choice([True, False]),
                    'verified_email': True,
                    'state': 'enabled',
                    'default_address.country': 'United States',
                    'default_address.province': np.random.choice(['NY', 'CA', 'TX', 'FL']),
                    'default_address.city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
                    'default_address.zip': f"{np.random.randint(10000, 99999)}"
                }
                
                # Add customer to list
                customers.append(customer)
            
            # Convert to DataFrame
            df = pd.DataFrame(customers)
            logger.info(f"Generated {len(df)} mock Shopify customers")
            return df
        
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
        import numpy as np
        
        logger.info(f"Generating mock Amazon {resource_type} data")
        
        # Parse date range
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate dates within range
        date_range = pd.date_range(start=start_date_obj, end=end_date_obj)
        
        # Create mock data based on resource type
        if resource_type.lower() == 'orders':
            # Generate order data
            orders = []
            
            for i in range(80):  # Generate 80 mock Amazon orders
                # Select random date from range
                order_date = np.random.choice(date_range)
                
                # Generate order data
                order_id = f"A{np.random.randint(100000000, 999999999)}"
                
                # Generate order item data
                num_items = np.random.randint(1, 4)
                item_price = round(np.random.uniform(10, 200), 2)
                quantity = np.random.randint(1, 3)
                
                # Calculate totals
                item_total = item_price * quantity
                shipping = round(np.random.uniform(3, 12), 2)
                total = item_total + shipping
                
                # Add to orders list
                orders.append({
                    'AmazonOrderId': order_id,
                    'PurchaseDate': order_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'LastUpdateDate': (order_date + timedelta(days=np.random.randint(0, 3))).strftime('%Y-%m-%dT%H:%M:%S'),
                    'OrderStatus': np.random.choice(['Shipped', 'Unshipped', 'Canceled'], p=[0.8, 0.15, 0.05]),
                    'FulfillmentChannel': np.random.choice(['AFN', 'MFN'], p=[0.6, 0.4]),  # Amazon or Merchant fulfilled
                    'SalesChannel': 'Amazon.com',
                    'OrderTotal.Amount': total,
                    'OrderTotal.CurrencyCode': 'USD',
                    'NumberOfItemsShipped': quantity if np.random.random() > 0.2 else 0,
                    'NumberOfItemsUnshipped': 0 if np.random.random() > 0.2 else quantity,
                    'PaymentMethod': np.random.choice(['Credit Card', 'Gift Card', 'Points']),
                    'ShipmentServiceLevelCategory': np.random.choice(['Standard', 'Expedited', 'Priority']),
                    'OrderType': 'StandardOrder',
                    'EarliestShipDate': (order_date + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S'),
                    'LatestShipDate': (order_date + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S'),
                    'IsBusinessOrder': False,
                    'IsPrime': np.random.choice([True, False], p=[0.7, 0.3]),
                    'ShippingPrice.Amount': shipping,
                    'ItemPrice.Amount': item_total
                })
            
            # Convert to DataFrame
            df = pd.DataFrame(orders)
            logger.info(f"Generated {len(df)} mock Amazon orders")
            return df
            
        elif resource_type.lower() == 'products' or resource_type.lower() == 'inventory':
            # Generate product/inventory data
            products = []
            
            for i in range(40):  # Generate 40 mock Amazon products
                # Calculate inventory level (some out of stock)
                inventory = np.random.randint(0, 100) if np.random.random() > 0.1 else 0
                
                # Generate ASIN and SKU
                asin = f"B{np.random.randint(10000000, 99999999)}"
                sku = f"SKU-A{i+1000}"
                
                # Generate price and cost
                price = round(np.random.uniform(10, 500), 2)
                
                # Generate product data
                product = {
                    'ASIN': asin,
                    'SellerSKU': sku,
                    'ProductName': f"Amazon Product {i+1}",
                    'ItemCondition': 'New',
                    'Category': np.random.choice(['Electronics', 'Home & Kitchen', 'Clothing', 'Books', 'Toys']),
                    'Status': 'Active' if inventory > 0 else 'Inactive',
                    'ListingPrice.Amount': price,
                    'ListingPrice.CurrencyCode': 'USD',
                    'Quantity': inventory,
                    'FulfillmentChannel': np.random.choice(['AMAZON', 'MERCHANT']),
                    'ItemWeight': round(np.random.uniform(0.1, 10), 2),
                    'ItemWeightUnit': 'pounds',
                    'ItemDimensions.Length': round(np.random.uniform(1, 30), 1),
                    'ItemDimensions.Width': round(np.random.uniform(1, 20), 1),
                    'ItemDimensions.Height': round(np.random.uniform(1, 15), 1),
                    'ItemDimensions.Unit': 'inches',
                    'ReservedQuantity': np.random.randint(0, 5) if inventory > 0 else 0,
                    'InStockSupplyDays': np.random.randint(0, 30) if inventory > 0 else 0,
                    'TotalSupplyQuantity': inventory + np.random.randint(0, 50)
                }
                
                # Add product to list
                products.append(product)
            
            # Convert to DataFrame
            df = pd.DataFrame(products)
            logger.info(f"Generated {len(df)} mock Amazon products/inventory items")
            return df
        
        else:
            logger.warning(f"Unsupported resource type for Amazon: {resource_type}")
            return pd.DataFrame()
