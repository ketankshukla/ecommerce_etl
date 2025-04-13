#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Customer Transformer module for transforming e-commerce customer data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class CustomerTransformer:
    """Transforms raw e-commerce customer data into a structured format."""
    
    def __init__(self, config):
        """
        Initialize the Customer Transformer.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
    
    def transform(self, data):
        """
        Transform raw customer data into a structured format.
        
        Args:
            data (pandas.DataFrame): Raw customer data, which could be sales data that needs
                                     to be transformed to extract customer information
        
        Returns:
            pandas.DataFrame: Transformed customer data
        """
        if data is None or data.empty:
            logger.warning("No data to transform")
            return pd.DataFrame()
            
        logger.info(f"Transforming customer data from input data with {len(data)} rows")
        
        try:
            # Check if this is already customer data
            if self._is_customer_data(data):
                logger.info("Input data appears to be customer data, normalizing format")
                return self._normalize_customer_data(data)
            
            # Extract customer data from sales data
            customer_data = self._extract_customer_data_from_sales(data)
            
            # Process and return the customer data
            logger.info(f"Extracted {len(customer_data)} unique customers from sales data")
            return customer_data
            
        except Exception as e:
            logger.error(f"Error transforming customer data: {str(e)}")
            raise
    
    def _is_customer_data(self, data):
        """
        Check if the input data is already customer data rather than sales data.
        
        Args:
            data (pandas.DataFrame): Input data to check
        
        Returns:
            bool: True if input appears to be customer data, False otherwise
        """
        # Look for common customer data columns
        customer_indicators = [
            # Check for customer identifiers
            'customer_id' in data.columns,
            
            # Check for customer personal info
            'name' in data.columns,
            'first_name' in data.columns and 'last_name' in data.columns,
            'email' in data.columns,
            'phone' in data.columns,
            
            # Check for customer address fields
            'address' in data.columns,
            'city' in data.columns,
            'state' in data.columns,
            'zip_code' in data.columns,
            'country' in data.columns,
            
            # Check for customer segmentation fields
            'segment' in data.columns,
            'customer_segment' in data.columns,
            
            # Check for customer purchase history
            'total_orders' in data.columns,
            'total_spent' in data.columns,
            'last_purchase_date' in data.columns,
            'registration_date' in data.columns
        ]
        
        # Check if the data has a significant number of customer indicators
        return sum(customer_indicators) >= 3
    
    def _normalize_customer_data(self, data):
        """
        Normalize customer data from various sources into a standard format.
        
        Args:
            data (pandas.DataFrame): Customer data to normalize
        
        Returns:
            pandas.DataFrame: Normalized customer data
        """
        # Make a copy to avoid modifying the original
        df = data.copy()
        
        # Standardize column names
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        
        # Map common column names to standard names
        column_mapping = {
            # ID fields
            'customer_id': 'customer_id',
            'id': 'customer_id',
            'user_id': 'customer_id',
            'customerid': 'customer_id',
            
            # Personal info
            'customer_name': 'full_name',
            'name': 'full_name',
            'full_name': 'full_name',
            
            'first_name': 'first_name',
            'firstname': 'first_name',
            'fname': 'first_name',
            
            'last_name': 'last_name',
            'lastname': 'last_name',
            'lname': 'last_name',
            
            'email': 'email',
            'email_address': 'email',
            
            'phone': 'phone',
            'phone_number': 'phone',
            'telephone': 'phone',
            
            # Address fields
            'address': 'address',
            'street_address': 'address',
            'address_line_1': 'address',
            
            'city': 'city',
            'state': 'state',
            'province': 'state',
            'region': 'state',
            
            'postal_code': 'zip_code',
            'zip': 'zip_code',
            'zip_code': 'zip_code',
            'postcode': 'zip_code',
            
            'country': 'country',
            'country_code': 'country_code',
            
            # Segments
            'segment': 'segment',
            'customer_segment': 'segment',
            'customer_type': 'segment',
            
            # Dates
            'registration_date': 'registration_date',
            'signup_date': 'registration_date',
            'date_joined': 'registration_date',
            'created_at': 'registration_date',
            
            'last_purchase_date': 'last_purchase_date',
            'last_order_date': 'last_purchase_date',
            'most_recent_order': 'last_purchase_date',
            
            # Purchase metrics
            'total_orders': 'total_orders',
            'order_count': 'total_orders',
            'orders_count': 'total_orders',
            'num_orders': 'total_orders',
            
            'total_spent': 'total_spent',
            'lifetime_value': 'total_spent',
            'customer_value': 'total_spent',
            'ltv': 'total_spent',
            
            'avg_order_value': 'avg_order_value',
            'average_order_value': 'avg_order_value',
            'aov': 'avg_order_value'
        }
        
        # Rename columns based on mapping
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and old_col != new_col:
                if new_col not in df.columns:
                    # Only rename if we're not overwriting an existing column
                    df.rename(columns={old_col: new_col}, inplace=True)
        
        # Create full_name from first_name and last_name if needed
        if 'full_name' not in df.columns and 'first_name' in df.columns and 'last_name' in df.columns:
            df['full_name'] = df['first_name'] + ' ' + df['last_name']
        
        # Convert date columns to datetime
        date_columns = ['registration_date', 'last_purchase_date']
        for col in date_columns:
            if col in df.columns and not pd.api.types.is_datetime64_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Ensure numeric columns have the right type
        numeric_columns = ['total_orders', 'total_spent', 'avg_order_value']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate avg_order_value if missing but we have total_spent and total_orders
        if 'avg_order_value' not in df.columns and 'total_spent' in df.columns and 'total_orders' in df.columns:
            mask = df['total_orders'] > 0
            df.loc[mask, 'avg_order_value'] = df.loc[mask, 'total_spent'] / df.loc[mask, 'total_orders']
        
        # Ensure required columns exist
        required_columns = ['customer_id']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Required column {col} not found in customer data")
                return pd.DataFrame()
        
        # Drop duplicate customers
        df.drop_duplicates(subset=['customer_id'], keep='first', inplace=True)
        
        logger.info(f"Normalized {len(df)} customer records")
        return df
    
    def _extract_customer_data_from_sales(self, sales_data):
        """
        Extract customer data from sales data.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
        
        Returns:
            pandas.DataFrame: Extracted customer data
        """
        # Check for required columns
        if 'customer_id' not in sales_data.columns:
            logger.warning("Sales data doesn't contain customer_id column")
            return pd.DataFrame()
        
        # Columns to extract directly from sales data
        extract_columns = ['customer_id']
        
        # Add other customer-related columns if available
        potential_columns = [
            'customer_name', 'customer_email', 'customer_phone',
            'customer_segment', 'city', 'state', 'country'
        ]
        
        for col in potential_columns:
            if col in sales_data.columns:
                extract_columns.append(col)
        
        # Extract unique customers with their basic info
        unique_customers = sales_data[extract_columns].drop_duplicates(subset=['customer_id'])
        
        # Calculate customer metrics
        
        # Count orders per customer
        order_counts = None
        if 'order_id' in sales_data.columns:
            order_counts = sales_data.groupby('customer_id')['order_id'].nunique().reset_index()
            order_counts.rename(columns={'order_id': 'total_orders'}, inplace=True)
        else:
            # If no order_id, count each row as an order (assumption)
            order_counts = sales_data.groupby('customer_id').size().reset_index(name='total_orders')
        
        # Merge with unique_customers
        unique_customers = pd.merge(unique_customers, order_counts, on='customer_id', how='left')
        
        # Calculate total spent per customer if relevant columns exist
        if 'final_price' in sales_data.columns:
            spend_totals = sales_data.groupby('customer_id')['final_price'].sum().reset_index()
            spend_totals.rename(columns={'final_price': 'total_spent'}, inplace=True)
            
            # Merge with unique_customers
            unique_customers = pd.merge(unique_customers, spend_totals, on='customer_id', how='left')
        elif 'total_price' in sales_data.columns:
            spend_totals = sales_data.groupby('customer_id')['total_price'].sum().reset_index()
            spend_totals.rename(columns={'total_price': 'total_spent'}, inplace=True)
            
            # Merge with unique_customers
            unique_customers = pd.merge(unique_customers, spend_totals, on='customer_id', how='left')
        
        # Calculate average order value
        if 'total_spent' in unique_customers.columns:
            mask = unique_customers['total_orders'] > 0
            unique_customers.loc[mask, 'avg_order_value'] = unique_customers.loc[mask, 'total_spent'] / unique_customers.loc[mask, 'total_orders']
        
        # Get first and last order dates if order_date exists
        if 'order_date' in sales_data.columns:
            # First order date (proxy for registration date)
            first_order_dates = sales_data.groupby('customer_id')['order_date'].min().reset_index()
            first_order_dates.rename(columns={'order_date': 'first_order_date'}, inplace=True)
            
            # Last order date
            last_order_dates = sales_data.groupby('customer_id')['order_date'].max().reset_index()
            last_order_dates.rename(columns={'order_date': 'last_purchase_date'}, inplace=True)
            
            # Merge with unique_customers
            unique_customers = pd.merge(unique_customers, first_order_dates, on='customer_id', how='left')
            unique_customers = pd.merge(unique_customers, last_order_dates, on='customer_id', how='left')
        
        # Calculate days since last purchase
        if 'last_purchase_date' in unique_customers.columns:
            current_date = datetime.now()
            unique_customers['days_since_purchase'] = (current_date - pd.to_datetime(unique_customers['last_purchase_date'])).dt.days
        
        # Calculate days between first and last purchase
        if 'first_order_date' in unique_customers.columns and 'last_purchase_date' in unique_customers.columns:
            unique_customers['customer_tenure_days'] = (pd.to_datetime(unique_customers['last_purchase_date']) - pd.to_datetime(unique_customers['first_order_date'])).dt.days
        
        # Create customer segments if not present
        if 'customer_segment' not in unique_customers.columns and 'total_orders' in unique_customers.columns:
            unique_customers['customer_segment'] = 'Unknown'
            
            # Segment based on number of orders
            mask = unique_customers['total_orders'] == 1
            unique_customers.loc[mask, 'customer_segment'] = 'New'
            
            mask = unique_customers['total_orders'] > 1
            unique_customers.loc[mask, 'customer_segment'] = 'Repeat'
            
            # Identify inactive customers (if we have days_since_purchase)
            if 'days_since_purchase' in unique_customers.columns:
                mask = (unique_customers['days_since_purchase'] > 90) & (unique_customers['total_orders'] > 0)
                unique_customers.loc[mask, 'customer_segment'] = 'Inactive'
            
            # Identify VIP customers (top 10% by spend or orders)
            if 'total_spent' in unique_customers.columns:
                spend_threshold = unique_customers['total_spent'].quantile(0.9)
                mask = unique_customers['total_spent'] >= spend_threshold
                unique_customers.loc[mask, 'customer_segment'] = 'VIP'
        
        # Handle missing values
        unique_customers.fillna({
            'total_orders': 0,
            'total_spent': 0,
            'avg_order_value': 0
        }, inplace=True)
        
        # Standardize customer_segment column name
        if 'customer_segment' in unique_customers.columns and 'segment' not in unique_customers.columns:
            unique_customers.rename(columns={'customer_segment': 'segment'}, inplace=True)
        
        logger.info(f"Extracted {len(unique_customers)} unique customers from sales data")
        return unique_customers
