#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Sales Transformer module for transforming e-commerce sales data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class SalesTransformer:
    """Transforms raw e-commerce sales data into a structured format."""
    
    def __init__(self, config):
        """
        Initialize the Sales Transformer.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
    
    def transform(self, data):
        """
        Transform raw sales data into a structured format.
        
        Args:
            data (pandas.DataFrame): Raw sales data
        
        Returns:
            pandas.DataFrame: Transformed sales data
        """
        if data is None or data.empty:
            logger.warning("No data to transform")
            return pd.DataFrame()
            
        logger.info(f"Transforming sales data with {len(data)} rows")
        
        try:
            # Make a copy of the dataframe to avoid modifying the original
            df = data.copy()
            
            # Standardize column names
            df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
            
            # Identify and convert date columns
            date_columns = [col for col in df.columns if 'date' in col]
            for col in date_columns:
                if df[col].dtype == 'object':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Add date-related fields for time series analysis
            if 'order_date' in df.columns:
                df['order_year'] = df['order_date'].dt.year
                df['order_month'] = df['order_date'].dt.month
                df['order_day'] = df['order_date'].dt.day
                df['order_dayofweek'] = df['order_date'].dt.dayofweek
                df['order_quarter'] = df['order_date'].dt.quarter
                df['order_week'] = df['order_date'].dt.isocalendar().week
            
            # Calculate order processing time if both dates are available
            if 'order_date' in df.columns and 'ship_date' in df.columns:
                df['processing_time_days'] = (df['ship_date'] - df['order_date']).dt.total_seconds() / (24 * 60 * 60)
                df['processing_time_days'] = df['processing_time_days'].round(1)
                
                # Handle negative or extreme values
                df.loc[df['processing_time_days'] < 0, 'processing_time_days'] = np.nan
                df.loc[df['processing_time_days'] > 30, 'processing_time_days'] = 30  # Cap at 30 days
            
            # Calculate price-related fields if they exist
            # Make sure numeric columns are properly typed
            numeric_cols = ['quantity', 'unit_price', 'total_price', 'discount', 'tax', 'shipping_cost', 'final_price']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate missing fields
            if 'quantity' in df.columns and 'unit_price' in df.columns and 'total_price' not in df.columns:
                df['total_price'] = df['quantity'] * df['unit_price']
            
            if 'total_price' in df.columns and 'discount' in df.columns and 'tax' in df.columns and 'shipping_cost' in df.columns and 'final_price' not in df.columns:
                df['final_price'] = df['total_price'] - df['discount'] + df['tax'] + df['shipping_cost']
            
            # Calculate profit if cost information is available
            if 'unit_cost' in df.columns and 'quantity' in df.columns and 'total_price' in df.columns:
                df['total_cost'] = df['unit_cost'] * df['quantity']
                df['profit'] = df['total_price'] - df['total_cost']
                df['profit_margin'] = (df['profit'] / df['total_price']).round(4)
            
            # Add a discount percentage if discount is available
            if 'discount' in df.columns and 'total_price' in df.columns:
                df['discount_percentage'] = (df['discount'] / (df['total_price'] + df['discount'])).fillna(0).round(4)
            
            # Add average order value grouping
            if 'order_id' in df.columns and 'final_price' in df.columns:
                # Calculate order totals
                order_totals = df.groupby('order_id')['final_price'].sum().reset_index()
                order_totals.rename(columns={'final_price': 'order_total'}, inplace=True)
                
                # Merge back to original dataframe
                df = pd.merge(df, order_totals, on='order_id', how='left')
                
                # Create AOV (Average Order Value) categories
                bins = [0, 20, 50, 100, 200, 500, 1000, float('inf')]
                labels = ['0-20', '21-50', '51-100', '101-200', '201-500', '501-1000', '1000+']
                df['aov_category'] = pd.cut(df['order_total'], bins=bins, labels=labels)
            
            # Add a returned flag if status column exists
            if 'status' in df.columns:
                df['is_returned'] = df['status'].str.lower().str.contains('return').fillna(False)
            
            # Add international flag if country is available
            if 'country' in df.columns:
                df['is_international'] = df['country'] != 'USA'
            
            # Create a new column that counts reorders by customer if customer_id exists
            if 'customer_id' in df.columns and 'order_date' in df.columns:
                # Sort by customer_id and order_date
                df = df.sort_values(['customer_id', 'order_date'])
                
                # Group by customer_id and calculate the order number
                df['customer_order_number'] = df.groupby('customer_id').cumcount() + 1
                
                # Flag first-time vs returning customer
                df['is_returning_customer'] = df['customer_order_number'] > 1
            
            # Handle missing values
            # For numeric columns, replace NaN with 0
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(0)
            
            # For categorical columns, replace NaN with 'Unknown'
            categorical_columns = df.select_dtypes(include=['object']).columns
            df[categorical_columns] = df[categorical_columns].fillna('Unknown')
            
            # Ensure all date columns have no NaN values
            for col in date_columns:
                # Replace NaN dates with a safe default (e.g., min date in the dataset)
                if df[col].isna().any():
                    min_date = df[col].min()
                    if pd.isna(min_date):
                        min_date = datetime.now()
                    df[col] = df[col].fillna(min_date)
            
            logger.info(f"Sales data transformation complete. Result has {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"Columns in transformed data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error transforming sales data: {str(e)}")
            raise
