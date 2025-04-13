#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration settings for the E-commerce Sales ETL pipeline.
"""

import os
from datetime import datetime, timedelta

class Config:
    """Configuration class for ETL pipeline settings."""
    
    def __init__(self):
        # Base paths
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.logs_dir = os.path.join(self.base_dir, 'logs')
        
        # Ensure directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, 'processed'), exist_ok=True)
        
        # Input data paths
        self.orders_csv = os.path.join(self.data_dir, 'orders.csv')
        self.products_json = os.path.join(self.data_dir, 'products.json')
        self.customers_excel = os.path.join(self.data_dir, 'customers.xlsx')
        self.invoices_pdf = os.path.join(self.data_dir, 'invoices.pdf')
        self.inventory_xml = os.path.join(self.data_dir, 'inventory.xml')
        self.historical_db = os.path.join(self.data_dir, 'historical_sales.db')
        
        # FTP settings
        self.ftp_host = os.environ.get('FTP_HOST', 'ftp.example.com')
        self.ftp_user = os.environ.get('FTP_USER', 'user')
        self.ftp_password = os.environ.get('FTP_PASSWORD', 'password')
        self.ftp_port = int(os.environ.get('FTP_PORT', 21))
        self.ftp_path = os.environ.get('FTP_PATH', '/exports/')
        
        # Email settings for data extraction
        self.email_server = os.environ.get('EMAIL_SERVER', 'imap.example.com')
        self.email_user = os.environ.get('EMAIL_USER', 'user@example.com')
        self.email_password = os.environ.get('EMAIL_PASSWORD', 'password')
        
        # API settings for e-commerce platforms
        self.shopify_api_key = os.environ.get('SHOPIFY_API_KEY', 'demo_key')
        self.shopify_api_secret = os.environ.get('SHOPIFY_API_SECRET', 'demo_secret')
        self.shopify_store = os.environ.get('SHOPIFY_STORE', 'demo-store.myshopify.com')
        
        self.amazon_access_key = os.environ.get('AMAZON_ACCESS_KEY', 'demo_key')
        self.amazon_secret_key = os.environ.get('AMAZON_SECRET_KEY', 'demo_secret')
        self.amazon_seller_id = os.environ.get('AMAZON_SELLER_ID', 'demo_seller')
        self.amazon_marketplace_id = os.environ.get('AMAZON_MARKETPLACE_ID', 'ATVPDKIKX0DER')  # US marketplace
        
        # Output data paths
        self.processed_data_dir = os.path.join(self.data_dir, 'processed')
        self.output_csv = os.path.join(self.processed_data_dir, f'sales_data_{datetime.now().strftime("%Y%m%d")}.csv')
        self.reports_dir = os.path.join(self.processed_data_dir, 'reports')
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # Database settings
        self.db_type = 'sqlite'  # 'sqlite', 'mysql', 'postgresql', 'mongodb'
        self.db_path = os.path.join(self.data_dir, 'ecommerce_sales.db')
        self.db_connection_string = f'sqlite:///{self.db_path}'
        
        # MySQL settings
        self.mysql_host = os.environ.get('MYSQL_HOST', 'localhost')
        self.mysql_port = int(os.environ.get('MYSQL_PORT', 3306))
        self.mysql_user = os.environ.get('MYSQL_USER', 'root')
        self.mysql_password = os.environ.get('MYSQL_PASSWORD', 'password')
        self.mysql_database = os.environ.get('MYSQL_DATABASE', 'ecommerce_etl')
        
        # MongoDB settings
        self.mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
        self.mongo_db = os.environ.get('MONGO_DB', 'ecommerce_etl')
        
        # Default query parameters
        self.default_start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.default_end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Analysis parameters
        self.sales_trend_window = 7  # 7-day sales trend
        self.inventory_threshold = 10  # Alert if inventory falls below this
        self.customer_segment_count = 5  # Number of customer segments
        
        # Validation thresholds
        self.max_missing_percentage = 0.1  # Maximum allowed percentage of missing values
        self.min_order_value = 0.01
        self.max_order_value = 10000.0  # Flag suspicious orders above this value
