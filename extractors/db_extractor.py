#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Database Extractor module for extracting e-commerce data from SQLite database.
"""

import logging
import pandas as pd
import os
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

class DBExtractor:
    """Extracts e-commerce data from SQLite database."""
    
    def __init__(self, config):
        """
        Initialize the Database Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.db_path = config.historical_db
    
    def extract(self, start_date=None, end_date=None, query=None):
        """
        Extract data from SQLite database with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            query (str, optional): Custom SQL query to execute
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            Exception: If there's an error connecting to the database
        """
        logger.info(f"Extracting data from SQLite database: {self.db_path}")
        
        # Check if database exists
        if not os.path.exists(self.db_path):
            # If the database doesn't exist, create a sample database for demo purposes
            logger.warning(f"SQLite database not found: {self.db_path}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_path)
            
            # Execute query based on parameters
            if query:
                logger.info(f"Executing custom query: {query}")
                df = pd.read_sql(query, conn)
            else:
                # Build SQL query with date filters
                sql = "SELECT * FROM historical_sales"
                params = []
                
                if start_date or end_date:
                    sql += " WHERE"
                
                if start_date:
                    sql += " order_date >= ?"
                    params.append(start_date)
                
                if end_date:
                    if start_date:
                        sql += " AND"
                    sql += " order_date <= ?"
                    params.append(end_date)
                
                logger.info(f"Executing query: {sql} with params: {params}")
                df = pd.read_sql(sql, conn, params=params)
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from database")
            if not df.empty:
                logger.info(f"Columns in database data: {', '.join(df.columns)}")
            
            # Close connection
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from database: {str(e)}")
            raise
    
    def extract_all_tables(self):
        """
        Extract data from all tables in the database.
        
        Returns:
            dict: Dictionary with table names as keys and DataFrames as values
        """
        logger.info("Extracting all tables from database")
        
        # Check if database exists
        if not os.path.exists(self.db_path):
            # If the database doesn't exist, create a sample database for demo purposes
            logger.warning(f"SQLite database not found: {self.db_path}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_path)
            
            # Get all table names
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in cursor.fetchall()]
            
            # Extract data from each table
            data = {}
            for table in tables:
                logger.info(f"Extracting data from table: {table}")
                data[table] = pd.read_sql(f"SELECT * FROM {table}", conn)
                logger.info(f"Extracted {len(data[table])} rows from table {table}")
            
            # Close connection
            conn.close()
            
            return data
            
        except Exception as e:
            logger.error(f"Error extracting all tables from database: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample e-commerce historical sales data in SQLite database."""
        import numpy as np
        
        logger.info(f"Creating sample database: {self.db_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Generate dates for the past 2 years
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=730)  # ~2 years
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Sample product categories and names
        product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
        
        # Customer segments
        customer_segments = ['New', 'Regular', 'VIP', 'Inactive', 'Wholesale']
        
        # Create empty list to store data
        historical_sales = []
        transactions = []
        customers = []
        
        # Generate random historical sales data
        order_id = 1000
        for _ in range(1000):  # Generate 1000 historical orders
            order_date_np = np.random.choice(dates)
            # Convert numpy.datetime64 to Python datetime via pandas Timestamp
            order_date = pd.Timestamp(order_date_np).to_pydatetime()
            # More recent dates should be more common
            days_ago = (end_date - order_date).days
            if np.random.random() > days_ago / 730:  # Probability decreases with age
                continue
                
            category = np.random.choice(product_categories)
            
            # Generate random quantities and prices
            quantity = np.random.randint(1, 10)
            unit_price = round(np.random.uniform(5, 200), 2)
            total_price = round(quantity * unit_price, 2)
            
            # Customer info
            customer_id = np.random.randint(1000, 2000)
            customer_segment = np.random.choice(customer_segments)
            
            # Add to historical sales data
            historical_sales.append({
                'order_id': order_id,
                'order_date': order_date.strftime('%Y-%m-%d'),
                'customer_id': customer_id,
                'customer_segment': customer_segment,
                'product_category': category,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price
            })
            
            # Add transaction data
            payment_status = np.random.choice(['Paid', 'Pending', 'Failed', 'Refunded'], p=[0.85, 0.1, 0.03, 0.02])
            payment_method = np.random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery', 'Gift Card'])
            
            transactions.append({
                'transaction_id': f"TRANS-{order_id}",
                'order_id': order_id,
                'transaction_date': (order_date + pd.Timedelta(days=np.random.randint(0, 2))).strftime('%Y-%m-%d'),
                'amount': total_price,
                'payment_method': payment_method,
                'status': payment_status
            })
            
            order_id += 1
        
        # Generate customer data
        for customer_id in range(1000, 2000):
            if customer_id % 10 == 0:  # Only create records for some customers
                continue
                
            registration_date_np = np.random.choice(dates)
            registration_date = pd.Timestamp(registration_date_np).to_pydatetime()
            last_purchase_date = registration_date + pd.Timedelta(days=np.random.randint(1, 365))
            if last_purchase_date > end_date:
                last_purchase_date = registration_date
            
            customers.append({
                'customer_id': customer_id,
                'name': f"Customer {customer_id}",
                'email': f"customer{customer_id}@example.com",
                'phone': f"555-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}",
                'registration_date': registration_date.strftime('%Y-%m-%d'),
                'last_purchase_date': last_purchase_date.strftime('%Y-%m-%d'),
                'total_purchases': np.random.randint(1, 50),
                'total_spent': round(np.random.uniform(50, 5000), 2),
                'segment': np.random.choice(customer_segments)
            })
        
        # Convert to DataFrames
        df_sales = pd.DataFrame(historical_sales)
        df_transactions = pd.DataFrame(transactions)
        df_customers = pd.DataFrame(customers)
        
        # Connect to SQLite database and create tables
        conn = sqlite3.connect(self.db_path)
        
        # Create tables and insert data
        df_sales.to_sql('historical_sales', conn, index=False, if_exists='replace')
        df_transactions.to_sql('transactions', conn, index=False, if_exists='replace')
        df_customers.to_sql('customers', conn, index=False, if_exists='replace')
        
        # Create some indexes for better performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sales_order_id ON historical_sales (order_id);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON historical_sales (customer_id);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sales_order_date ON historical_sales (order_date);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_trans_order_id ON transactions (order_id);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_customers_id ON customers (customer_id);')
        
        # Commit and close connection
        conn.commit()
        conn.close()
        
        logger.info(f"Created sample historical sales database with {len(df_sales)} sales records, {len(df_transactions)} transactions, and {len(df_customers)} customers")
