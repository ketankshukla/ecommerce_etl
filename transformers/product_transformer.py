#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Product Transformer module for transforming e-commerce product data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class ProductTransformer:
    """Transforms raw e-commerce product data into a structured format."""
    
    def __init__(self, config):
        """
        Initialize the Product Transformer.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
    
    def transform(self, data):
        """
        Transform raw product data into a structured format.
        
        Args:
            data (pandas.DataFrame): Raw product data, which could be sales data that needs
                                    to be transformed to extract product information
        
        Returns:
            pandas.DataFrame: Transformed product data
        """
        if data is None or data.empty:
            logger.warning("No data to transform")
            return pd.DataFrame()
            
        logger.info(f"Transforming product data from sales data with {len(data)} rows")
        
        try:
            # Check if this is already product data
            if self._is_product_data(data):
                logger.info("Input data appears to be product data, normalizing format")
                return self._normalize_product_data(data)
            
            # Extract product data from sales data
            product_data = self._extract_product_data_from_sales(data)
            
            # Process and return the product data
            logger.info(f"Extracted {len(product_data)} unique products from sales data")
            return product_data
            
        except Exception as e:
            logger.error(f"Error transforming product data: {str(e)}")
            raise
    
    def _is_product_data(self, data):
        """
        Check if the input data is already product data rather than sales data.
        
        Args:
            data (pandas.DataFrame): Input data to check
        
        Returns:
            bool: True if input appears to be product data, False otherwise
        """
        # Look for common product data columns
        product_indicators = [
            # Check for inventory-related columns
            'stock' in data.columns,
            'inventory' in data.columns,
            'quantity_available' in data.columns,
            
            # Check for product-specific fields
            'product_description' in data.columns,
            'sku' in data.columns,
            
            # Check for pricing structure typical of product data
            'price' in data.columns and 'cost' in data.columns,
            
            # Check for brand/manufacturer info
            'brand' in data.columns,
            'manufacturer' in data.columns,
            
            # Check for dimensions/weight typical of product data
            'weight' in data.columns,
            'dimensions' in data.columns
        ]
        
        # Check if the data has a significant number of product indicators
        return sum(product_indicators) >= 2
    
    def _normalize_product_data(self, data):
        """
        Normalize product data from various sources into a standard format.
        
        Args:
            data (pandas.DataFrame): Product data to normalize
        
        Returns:
            pandas.DataFrame: Normalized product data
        """
        # Make a copy to avoid modifying the original
        df = data.copy()
        
        # Standardize column names
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        
        # Map common column names to standard names
        column_mapping = {
            # ID fields
            'product_id': 'product_id',
            'id': 'product_id',
            'sku': 'sku',
            'asin': 'asin',
            'sellersku': 'sku',
            'item_id': 'product_id',
            
            # Basic product info
            'product_name': 'product_name',
            'name': 'product_name',
            'title': 'product_name',
            'product_title': 'product_name',
            'item_name': 'product_name',
            
            'product_description': 'product_description',
            'description': 'product_description',
            
            'product_category': 'product_category',
            'category': 'product_category',
            
            # Inventory
            'stock': 'stock',
            'inventory_quantity': 'stock',
            'quantity_available': 'stock',
            'quantity': 'stock',
            
            'stock_status': 'stock_status',
            'inventory_status': 'stock_status',
            
            # Pricing
            'price': 'price',
            'list_price': 'price',
            'retail_price': 'price',
            'unit_price': 'price',
            'listingprice.amount': 'price',
            
            'sale_price': 'sale_price',
            'discount_price': 'sale_price',
            'special_price': 'sale_price',
            
            'cost': 'cost',
            'unit_cost': 'cost',
            'supplier_price': 'cost',
            
            # Dimensions and weight
            'weight': 'weight',
            'product_weight': 'weight',
            'item_weight': 'weight',
            'itemweight': 'weight',
            
            # Dates
            'created_date': 'created_date',
            'created_at': 'created_date',
            'date_added': 'created_date',
            
            'updated_date': 'updated_date',
            'updated_at': 'updated_date',
            'last_updated': 'updated_date',
            'last_updated_date': 'updated_date'
        }
        
        # Rename columns based on mapping
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and old_col != new_col:
                if new_col not in df.columns:
                    # Only rename if we're not overwriting an existing column
                    df.rename(columns={old_col: new_col}, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['product_id', 'product_name', 'product_category', 'price']
        
        # Create missing columns with None/NaN values
        for col in required_columns:
            if col not in df.columns:
                # Try to derive it from other columns
                if col == 'product_id' and 'sku' in df.columns:
                    df['product_id'] = df['sku']
                elif col == 'product_name' and 'product_description' in df.columns:
                    df['product_name'] = df['product_description'].str.slice(0, 50)
                else:
                    df[col] = None
        
        # Handle date columns
        date_columns = ['created_date', 'updated_date']
        for col in date_columns:
            if col in df.columns and df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns to appropriate types
        numeric_columns = ['price', 'sale_price', 'cost', 'stock', 'weight']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Derive stock status if missing
        if 'stock' in df.columns and 'stock_status' not in df.columns:
            df['stock_status'] = df['stock'].apply(lambda x: 'In Stock' if pd.notna(x) and x > 0 else 'Out of Stock')
        
        # Ensure consistent format for SKU and ASIN
        if 'sku' in df.columns:
            df['sku'] = df['sku'].astype(str).str.strip()
        
        if 'asin' in df.columns:
            df['asin'] = df['asin'].astype(str).str.strip()
        
        # Calculate sale percentage if we have both prices
        if 'price' in df.columns and 'sale_price' in df.columns:
            mask = (df['price'] > 0) & (df['sale_price'] > 0) & (df['sale_price'] < df['price'])
            if mask.any():
                df.loc[mask, 'discount_percentage'] = ((df.loc[mask, 'price'] - df.loc[mask, 'sale_price']) / df.loc[mask, 'price']) * 100
                df['discount_percentage'] = df['discount_percentage'].round(2)
        
        # Drop duplicate products
        if 'product_id' in df.columns:
            df.drop_duplicates(subset=['product_id'], keep='first', inplace=True)
        
        logger.info(f"Normalized {len(df)} product records")
        return df
    
    def _extract_product_data_from_sales(self, sales_data):
        """
        Extract product data from sales data.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
        
        Returns:
            pandas.DataFrame: Extracted product data
        """
        # Check for required columns
        if 'product_id' not in sales_data.columns and 'product_name' not in sales_data.columns:
            logger.warning("Sales data doesn't contain product_id or product_name columns")
            return pd.DataFrame()
        
        # Determine the identifier column to use
        id_column = 'product_id' if 'product_id' in sales_data.columns else 'product_name'
        
        # Columns to extract from sales data
        extract_columns = [id_column]
        
        # Add other product-related columns if available
        potential_columns = [
            'product_name', 'product_category', 'unit_price', 'product_description',
            'brand', 'sku', 'asin', 'category'
        ]
        
        for col in potential_columns:
            if col in sales_data.columns and col != id_column:
                extract_columns.append(col)
        
        # Extract unique products
        unique_products = sales_data[extract_columns].drop_duplicates(subset=[id_column])
        
        # Rename unit_price to price if it exists
        if 'unit_price' in unique_products.columns and 'price' not in unique_products.columns:
            unique_products.rename(columns={'unit_price': 'price'}, inplace=True)
        
        # Get last modified date from sales data if available
        if 'order_date' in sales_data.columns:
            # For each product, find the most recent order date
            latest_dates = sales_data.groupby(id_column)['order_date'].max().reset_index()
            latest_dates.rename(columns={'order_date': 'last_ordered_date'}, inplace=True)
            
            # Merge with unique_products
            unique_products = pd.merge(unique_products, latest_dates, on=id_column, how='left')
        
        # Calculate metrics if possible
        
        # Count occurrences of each product (popularity)
        product_counts = sales_data[id_column].value_counts().reset_index()
        product_counts.columns = [id_column, 'order_count']
        
        # Merge with unique_products
        unique_products = pd.merge(unique_products, product_counts, on=id_column, how='left')
        
        # Calculate total sales quantity if quantity column exists
        if 'quantity' in sales_data.columns:
            quantity_totals = sales_data.groupby(id_column)['quantity'].sum().reset_index()
            quantity_totals.rename(columns={'quantity': 'total_quantity_sold'}, inplace=True)
            
            # Merge with unique_products
            unique_products = pd.merge(unique_products, quantity_totals, on=id_column, how='left')
        
        # Calculate total revenue if relevant columns exist
        if 'total_price' in sales_data.columns:
            revenue_totals = sales_data.groupby(id_column)['total_price'].sum().reset_index()
            revenue_totals.rename(columns={'total_price': 'total_revenue'}, inplace=True)
            
            # Merge with unique_products
            unique_products = pd.merge(unique_products, revenue_totals, on=id_column, how='left')
        
        # Handle missing values
        unique_products.fillna({
            'order_count': 0,
            'total_quantity_sold': 0,
            'total_revenue': 0
        }, inplace=True)
        
        # Rename id_column to product_id if it's not already
        if id_column != 'product_id':
            unique_products.rename(columns={id_column: 'product_id'}, inplace=True)
        
        logger.info(f"Extracted {len(unique_products)} unique products from sales data")
        return unique_products
