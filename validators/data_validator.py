#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Validator module for validating e-commerce data quality and consistency.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates e-commerce data for quality and consistency."""
    
    def __init__(self, config):
        """
        Initialize the Data Validator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.max_missing_percentage = config.max_missing_percentage
        self.min_order_value = config.min_order_value
        self.max_order_value = config.max_order_value
    
    def validate(self, data):
        """
        Validate data quality and consistency.
        
        Args:
            data (dict): Dictionary containing DataFrames with data to validate
                         or a single DataFrame
        
        Returns:
            dict or pandas.DataFrame: Dictionary containing validated DataFrames
                                     or a single validated DataFrame
        """
        if data is None:
            logger.warning("No data to validate")
            return None
            
        # Check if data is a dictionary of DataFrames or a single DataFrame
        if isinstance(data, dict):
            logger.info(f"Validating {len(data)} data sources")
            
            # Initialize dictionary for validated data
            validated_data = {}
            
            # Validate each DataFrame
            for key, df in data.items():
                if df is not None and not df.empty:
                    logger.info(f"Validating {key} data ({len(df)} rows)")
                    validated_data[key] = self._validate_dataframe(df, key)
                else:
                    logger.warning(f"No data to validate for {key}")
                    validated_data[key] = df
            
            return validated_data
        else:
            # Data is a single DataFrame
            logger.info(f"Validating DataFrame ({len(data)} rows)")
            return self._validate_dataframe(data)
    
    def _validate_dataframe(self, df, source_name=None):
        """
        Validate a single DataFrame.
        
        Args:
            df (pandas.DataFrame): DataFrame to validate
            source_name (str, optional): Name of the data source for logging
        
        Returns:
            pandas.DataFrame: Validated DataFrame
        """
        if df is None or df.empty:
            logger.warning(f"Empty DataFrame for {source_name or 'data'}")
            return df
        
        # Make a copy to avoid modifying the original
        validated_df = df.copy()
        
        # Track validation results
        validation_results = {
            'total_rows': len(validated_df),
            'rows_flagged': 0,
            'rows_removed': 0,
            'rows_fixed': 0,
            'validation_errors': []
        }
        
        try:
            # Check for missing values
            missing_values = validated_df.isnull().sum()
            missing_percentage = missing_values / len(validated_df)
            
            # Log columns with significant missing values
            for col, pct in missing_percentage.items():
                if pct > 0:
                    if pct >= self.max_missing_percentage:
                        message = f"Column '{col}' has {pct:.2%} missing values (above threshold of {self.max_missing_percentage:.2%})"
                        logger.warning(message)
                        validation_results['validation_errors'].append(message)
                    else:
                        logger.info(f"Column '{col}' has {pct:.2%} missing values")
            
            # Handle missing values based on data type
            for col in validated_df.columns:
                if missing_percentage[col] > 0:
                    # Skip if too many missing values
                    if missing_percentage[col] >= self.max_missing_percentage:
                        continue
                    
                    # For numeric columns, replace with 0 or mean
                    if np.issubdtype(validated_df[col].dtype, np.number):
                        # Use mean for non-monetary values
                        if 'price' not in col and 'cost' not in col and 'value' not in col:
                            validated_df[col] = validated_df[col].fillna(validated_df[col].mean())
                        # Use 0 for monetary values
                        else:
                            validated_df[col] = validated_df[col].fillna(0)
                    
                    # For date columns, use forward and backward fill
                    elif pd.api.types.is_datetime64_dtype(validated_df[col]):
                        validated_df[col] = validated_df[col].ffill().bfill()
                    
                    # For categorical/string columns, use 'Unknown'
                    else:
                        validated_df[col] = validated_df[col].fillna('Unknown')
                    
                    validation_results['rows_fixed'] += missing_values[col]
            
            # Validate data types and convert if needed
            # Check for numeric columns with non-numeric data
            for col in validated_df.columns:
                if 'price' in col or 'cost' in col or 'amount' in col or 'value' in col:
                    if not np.issubdtype(validated_df[col].dtype, np.number):
                        # Try to convert to numeric
                        try:
                            validated_df[col] = pd.to_numeric(validated_df[col], errors='coerce')
                            non_numeric_count = validated_df[col].isna().sum()
                            if non_numeric_count > 0:
                                message = f"Converted column '{col}' to numeric, {non_numeric_count} non-numeric values were set to NaN"
                                logger.warning(message)
                                validation_results['validation_errors'].append(message)
                                validation_results['rows_fixed'] += non_numeric_count
                        except Exception as e:
                            message = f"Failed to convert column '{col}' to numeric: {str(e)}"
                            logger.error(message)
                            validation_results['validation_errors'].append(message)
            
            # Validate date columns
            date_columns = [col for col in validated_df.columns if 'date' in col.lower()]
            for col in date_columns:
                if not pd.api.types.is_datetime64_dtype(validated_df[col]):
                    # Try to convert to datetime
                    try:
                        validated_df[col] = pd.to_datetime(validated_df[col], errors='coerce')
                        invalid_dates = validated_df[col].isna().sum()
                        if invalid_dates > 0:
                            message = f"Converted column '{col}' to datetime, {invalid_dates} invalid dates were set to NaN"
                            logger.warning(message)
                            validation_results['validation_errors'].append(message)
                            validation_results['rows_fixed'] += invalid_dates
                    except Exception as e:
                        message = f"Failed to convert column '{col}' to datetime: {str(e)}"
                        logger.error(message)
                        validation_results['validation_errors'].append(message)
            
            # Business rule validations
            # Check for negative prices or quantities
            price_columns = [col for col in validated_df.columns if 'price' in col.lower() or 'cost' in col.lower()]
            for col in price_columns:
                if np.issubdtype(validated_df[col].dtype, np.number):
                    negative_count = (validated_df[col] < 0).sum()
                    if negative_count > 0:
                        message = f"Found {negative_count} negative values in '{col}'"
                        logger.warning(message)
                        validation_results['validation_errors'].append(message)
                        validation_results['rows_flagged'] += negative_count
                        
                        # Fix by taking absolute value
                        validated_df.loc[validated_df[col] < 0, col] = validated_df.loc[validated_df[col] < 0, col].abs()
                        validation_results['rows_fixed'] += negative_count
            
            if 'quantity' in validated_df.columns and np.issubdtype(validated_df['quantity'].dtype, np.number):
                negative_qty = (validated_df['quantity'] < 0).sum()
                if negative_qty > 0:
                    message = f"Found {negative_qty} negative values in 'quantity'"
                    logger.warning(message)
                    validation_results['validation_errors'].append(message)
                    validation_results['rows_flagged'] += negative_qty
                    
                    # Fix by taking absolute value
                    validated_df.loc[validated_df['quantity'] < 0, 'quantity'] = validated_df.loc[validated_df['quantity'] < 0, 'quantity'].abs()
                    validation_results['rows_fixed'] += negative_qty
            
            # Check for suspicious order values
            if 'final_price' in validated_df.columns or 'total_price' in validated_df.columns:
                price_col = 'final_price' if 'final_price' in validated_df.columns else 'total_price'
                
                # Flag suspicious low values
                suspicious_low = (validated_df[price_col] < self.min_order_value).sum()
                if suspicious_low > 0:
                    message = f"Found {suspicious_low} orders with suspiciously low value (< {self.min_order_value})"
                    logger.warning(message)
                    validation_results['validation_errors'].append(message)
                    validation_results['rows_flagged'] += suspicious_low
                
                # Flag suspicious high values
                suspicious_high = (validated_df[price_col] > self.max_order_value).sum()
                if suspicious_high > 0:
                    message = f"Found {suspicious_high} orders with suspiciously high value (> {self.max_order_value})"
                    logger.warning(message)
                    validation_results['validation_errors'].append(message)
                    validation_results['rows_flagged'] += suspicious_high
            
            # Check for future dates
            current_date = datetime.now().date()
            for col in date_columns:
                if pd.api.types.is_datetime64_dtype(validated_df[col]):
                    future_dates = (validated_df[col].dt.date > current_date).sum()
                    if future_dates > 0:
                        message = f"Found {future_dates} future dates in '{col}'"
                        logger.warning(message)
                        validation_results['validation_errors'].append(message)
                        validation_results['rows_flagged'] += future_dates
            
            # Check for data consistency
            # If order_date and ship_date present, validate ship_date >= order_date
            if 'order_date' in validated_df.columns and 'ship_date' in validated_df.columns:
                if pd.api.types.is_datetime64_dtype(validated_df['order_date']) and pd.api.types.is_datetime64_dtype(validated_df['ship_date']):
                    invalid_dates = (validated_df['ship_date'] < validated_df['order_date']).sum()
                    if invalid_dates > 0:
                        message = f"Found {invalid_dates} records where ship_date is before order_date"
                        logger.warning(message)
                        validation_results['validation_errors'].append(message)
                        validation_results['rows_flagged'] += invalid_dates
                        
                        # Fix by swapping dates
                        swap_mask = validated_df['ship_date'] < validated_df['order_date']
                        temp = validated_df.loc[swap_mask, 'ship_date'].copy()
                        validated_df.loc[swap_mask, 'ship_date'] = validated_df.loc[swap_mask, 'order_date']
                        validated_df.loc[swap_mask, 'order_date'] = temp
                        validation_results['rows_fixed'] += invalid_dates
            
            # Log validation summary
            logger.info(f"Validation complete for {source_name or 'data'}: "
                      f"{validation_results['rows_flagged']} rows flagged, "
                      f"{validation_results['rows_fixed']} issues fixed, "
                      f"{validation_results['rows_removed']} rows removed")
            
            # Add validation results as dataframe metadata
            validated_df.attrs['validation_results'] = validation_results
            
            return validated_df
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            # Return original dataframe if validation fails
            return df
    
    def validate_consistency(self, sales_data=None, product_data=None, customer_data=None):
        """
        Validate consistency across multiple data sources.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
            product_data (pandas.DataFrame): Product data
            customer_data (pandas.DataFrame): Customer data
        
        Returns:
            dict: Dictionary containing validation results
        """
        consistency_results = {
            'consistent': True,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Skip if data sources are missing
            if sales_data is None or product_data is None or customer_data is None:
                message = "Cannot validate consistency: one or more data sources missing"
                logger.warning(message)
                consistency_results['warnings'].append(message)
                consistency_results['consistent'] = False
                return consistency_results
            
            # Validate product IDs in sales data exist in product data
            if 'product_id' in sales_data.columns and 'product_id' in product_data.columns:
                sales_product_ids = set(sales_data['product_id'].unique())
                product_ids = set(product_data['product_id'].unique())
                
                missing_products = sales_product_ids - product_ids
                if missing_products:
                    message = f"Found {len(missing_products)} product IDs in sales data that don't exist in product data"
                    logger.warning(message)
                    consistency_results['warnings'].append(message)
                    consistency_results['consistent'] = False
            
            # Validate customer IDs in sales data exist in customer data
            if 'customer_id' in sales_data.columns and 'customer_id' in customer_data.columns:
                sales_customer_ids = set(sales_data['customer_id'].unique())
                customer_ids = set(customer_data['customer_id'].unique())
                
                missing_customers = sales_customer_ids - customer_ids
                if missing_customers:
                    message = f"Found {len(missing_customers)} customer IDs in sales data that don't exist in customer data"
                    logger.warning(message)
                    consistency_results['warnings'].append(message)
                    consistency_results['consistent'] = False
            
            # Check if total orders in customer data match orders in sales data
            if 'customer_id' in sales_data.columns and 'total_orders' in customer_data.columns:
                # Count orders by customer in sales data
                orders_by_customer = sales_data.groupby('customer_id')['order_id'].nunique().reset_index()
                orders_by_customer.rename(columns={'order_id': 'orders_in_sales'}, inplace=True)
                
                # Merge with customer data
                customer_orders = customer_data[['customer_id', 'total_orders']].merge(orders_by_customer, on='customer_id', how='left')
                customer_orders['orders_in_sales'] = customer_orders['orders_in_sales'].fillna(0)
                
                # Compare order counts
                customer_orders['order_difference'] = customer_orders['total_orders'] - customer_orders['orders_in_sales']
                inconsistent_orders = (customer_orders['order_difference'] != 0).sum()
                
                if inconsistent_orders > 0:
                    message = f"Found {inconsistent_orders} customers with inconsistent order counts between sales and customer data"
                    logger.warning(message)
                    consistency_results['warnings'].append(message)
                    consistency_results['consistent'] = False
            
            # Additional consistency checks can be added here
            
            return consistency_results
            
        except Exception as e:
            message = f"Error validating data consistency: {str(e)}"
            logger.error(message)
            consistency_results['errors'].append(message)
            consistency_results['consistent'] = False
            return consistency_results
