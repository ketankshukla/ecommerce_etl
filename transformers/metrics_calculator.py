#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Metrics Calculator module for generating business insights from transformed e-commerce data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """Calculates business metrics from transformed e-commerce data."""
    
    def __init__(self, config):
        """
        Initialize the Metrics Calculator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
    
    def calculate(self, sales_data=None, product_data=None, customer_data=None):
        """
        Calculate business metrics from transformed data.
        
        Args:
            sales_data (pandas.DataFrame): Transformed sales data
            product_data (pandas.DataFrame): Transformed product data
            customer_data (pandas.DataFrame): Transformed customer data
        
        Returns:
            dict: Dictionary containing DataFrames with calculated metrics
        """
        logger.info("Calculating business metrics")
        
        # Initialize results dictionary
        metrics = {}
        
        # Track which metrics were calculated
        calculated_metrics = []
        
        # Check if we have sales data
        if sales_data is not None and not sales_data.empty:
            # Calculate sales metrics
            sales_metrics = self._calculate_sales_metrics(sales_data)
            metrics['sales_metrics'] = sales_metrics
            calculated_metrics.append('sales_metrics')
            
            # Calculate time-based metrics if we have date columns
            if 'order_date' in sales_data.columns:
                time_metrics = self._calculate_time_metrics(sales_data)
                metrics['time_metrics'] = time_metrics
                calculated_metrics.append('time_metrics')
        
        # Check if we have product data
        if product_data is not None and not product_data.empty:
            # Calculate product metrics
            product_metrics = self._calculate_product_metrics(product_data)
            metrics['product_metrics'] = product_metrics
            calculated_metrics.append('product_metrics')
        
        # Check if we have customer data
        if customer_data is not None and not customer_data.empty:
            # Calculate customer metrics
            customer_metrics = self._calculate_customer_metrics(customer_data)
            metrics['customer_metrics'] = customer_metrics
            calculated_metrics.append('customer_metrics')
        
        # Calculate cross-metrics if we have multiple data sources
        if sales_data is not None and not sales_data.empty and customer_data is not None and not customer_data.empty:
            # Customer segmentation metrics
            segmentation_metrics = self._calculate_segmentation_metrics(sales_data, customer_data)
            metrics['segmentation_metrics'] = segmentation_metrics
            calculated_metrics.append('segmentation_metrics')
        
        if sales_data is not None and not sales_data.empty and product_data is not None and not product_data.empty:
            # Product performance metrics
            product_performance = self._calculate_product_performance(sales_data, product_data)
            metrics['product_performance'] = product_performance
            calculated_metrics.append('product_performance')
        
        # Log which metrics were calculated
        logger.info(f"Calculated metrics: {', '.join(calculated_metrics)}")
        
        return metrics
    
    def _calculate_sales_metrics(self, sales_data):
        """
        Calculate sales-related metrics.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
        
        Returns:
            pandas.DataFrame: DataFrame with sales metrics
        """
        logger.info("Calculating sales metrics")
        
        # Initialize metrics dictionary
        metrics = {}
        
        try:
            # Total revenue
            if 'final_price' in sales_data.columns:
                metrics['total_revenue'] = sales_data['final_price'].sum()
            elif 'total_price' in sales_data.columns:
                metrics['total_revenue'] = sales_data['total_price'].sum()
            
            # Total orders (unique order IDs)
            if 'order_id' in sales_data.columns:
                metrics['total_orders'] = sales_data['order_id'].nunique()
                
                # Average order value
                if 'final_price' in sales_data.columns:
                    metrics['average_order_value'] = sales_data.groupby('order_id')['final_price'].sum().mean()
                elif 'total_price' in sales_data.columns:
                    metrics['average_order_value'] = sales_data.groupby('order_id')['total_price'].sum().mean()
            
            # Total units sold
            if 'quantity' in sales_data.columns:
                metrics['total_units_sold'] = sales_data['quantity'].sum()
                
                # Average units per order
                if 'order_id' in sales_data.columns:
                    metrics['average_units_per_order'] = sales_data.groupby('order_id')['quantity'].sum().mean()
            
            # Return rate
            if 'is_returned' in sales_data.columns:
                metrics['return_rate'] = sales_data['is_returned'].mean()
            
            # Shipping metrics
            if 'shipping_cost' in sales_data.columns:
                metrics['average_shipping_cost'] = sales_data['shipping_cost'].mean()
            
            if 'processing_time_days' in sales_data.columns:
                metrics['average_processing_time'] = sales_data['processing_time_days'].mean()
            
            # Discount metrics
            if 'discount' in sales_data.columns:
                metrics['total_discounts'] = sales_data['discount'].sum()
                metrics['average_discount'] = sales_data['discount'].mean()
                
                if 'discount_percentage' in sales_data.columns:
                    metrics['average_discount_percentage'] = sales_data['discount_percentage'].mean()
            
            # Tax metrics
            if 'tax' in sales_data.columns:
                metrics['total_tax'] = sales_data['tax'].sum()
                metrics['average_tax'] = sales_data['tax'].mean()
            
            # Profit metrics
            if 'profit' in sales_data.columns:
                metrics['total_profit'] = sales_data['profit'].sum()
                metrics['average_profit'] = sales_data['profit'].mean()
                
                if 'profit_margin' in sales_data.columns:
                    metrics['average_profit_margin'] = sales_data['profit_margin'].mean()
            
            # Payment method breakdown
            if 'payment_method' in sales_data.columns:
                payment_counts = sales_data['payment_method'].value_counts()
                payment_percentages = (payment_counts / payment_counts.sum() * 100).round(2)
                
                # Create a dictionary of payment methods and their percentages
                payment_breakdown = {}
                for method, percentage in payment_percentages.items():
                    payment_breakdown[f'payment_{method.lower().replace(" ", "_")}'] = percentage
                
                # Add to metrics
                metrics.update(payment_breakdown)
            
            # Shipping method breakdown
            if 'shipping_method' in sales_data.columns:
                shipping_counts = sales_data['shipping_method'].value_counts()
                shipping_percentages = (shipping_counts / shipping_counts.sum() * 100).round(2)
                
                # Create a dictionary of shipping methods and their percentages
                shipping_breakdown = {}
                for method, percentage in shipping_percentages.items():
                    shipping_breakdown[f'shipping_{method.lower().replace(" ", "_")}'] = percentage
                
                # Add to metrics
                metrics.update(shipping_breakdown)
            
            # Convert metrics to DataFrame
            metrics_df = pd.DataFrame([metrics])
            
            return metrics_df
            
        except Exception as e:
            logger.error(f"Error calculating sales metrics: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_time_metrics(self, sales_data):
        """
        Calculate time-based metrics for trend analysis.
        
        Args:
            sales_data (pandas.DataFrame): Sales data with date columns
        
        Returns:
            pandas.DataFrame: DataFrame with time-based metrics
        """
        logger.info("Calculating time-based metrics")
        
        try:
            # Ensure order_date is datetime
            if 'order_date' in sales_data.columns:
                sales_data['order_date'] = pd.to_datetime(sales_data['order_date'])
            else:
                logger.warning("No order_date column found, cannot calculate time metrics")
                return pd.DataFrame()
            
            # Determine the revenue column
            if 'final_price' in sales_data.columns:
                revenue_col = 'final_price'
            elif 'total_price' in sales_data.columns:
                revenue_col = 'total_price'
            else:
                logger.warning("No revenue column found, cannot calculate time metrics")
                return pd.DataFrame()
            
            # Group by date and calculate daily metrics
            daily_sales = sales_data.groupby(sales_data['order_date'].dt.date).agg({
                revenue_col: 'sum',
                'order_id': 'nunique',
                'quantity': 'sum' if 'quantity' in sales_data.columns else 'size'
            }).reset_index()
            
            daily_sales.rename(columns={
                revenue_col: 'daily_revenue',
                'order_id': 'daily_orders',
                'quantity': 'daily_units'
            }, inplace=True)
            
            # Calculate rolling averages for trend analysis
            window_size = self.config.sales_trend_window
            daily_sales['revenue_7d_avg'] = daily_sales['daily_revenue'].rolling(window=window_size, min_periods=1).mean()
            daily_sales['orders_7d_avg'] = daily_sales['daily_orders'].rolling(window=window_size, min_periods=1).mean()
            daily_sales['units_7d_avg'] = daily_sales['daily_units'].rolling(window=window_size, min_periods=1).mean()
            
            # Calculate month-to-date (MTD) metrics
            daily_sales['month'] = pd.to_datetime(daily_sales['order_date']).dt.to_period('M')
            daily_sales['day_of_month'] = pd.to_datetime(daily_sales['order_date']).dt.day
            
            # Calculate MTD
            mtd_sales = daily_sales.groupby(['month', 'day_of_month']).agg({
                'daily_revenue': 'sum',
                'daily_orders': 'sum',
                'daily_units': 'sum'
            }).groupby(level=0).cumsum().reset_index()
            
            mtd_sales.rename(columns={
                'daily_revenue': 'mtd_revenue',
                'daily_orders': 'mtd_orders',
                'daily_units': 'mtd_units'
            }, inplace=True)
            
            # Merge MTD back to daily
            daily_sales = pd.merge(
                daily_sales, 
                mtd_sales[['month', 'day_of_month', 'mtd_revenue', 'mtd_orders', 'mtd_units']], 
                on=['month', 'day_of_month'], 
                how='left'
            )
            
            # Clean up intermediate columns
            daily_sales.drop(['month', 'day_of_month'], axis=1, inplace=True)
            
            # Calculate growth metrics (day-over-day, week-over-week)
            daily_sales['revenue_daily_change'] = daily_sales['daily_revenue'].pct_change() * 100
            daily_sales['orders_daily_change'] = daily_sales['daily_orders'].pct_change() * 100
            daily_sales['units_daily_change'] = daily_sales['daily_units'].pct_change() * 100
            
            # Week-over-week (compare to 7 days ago)
            daily_sales['revenue_wow_change'] = (daily_sales['daily_revenue'] / daily_sales['daily_revenue'].shift(7) - 1) * 100
            daily_sales['orders_wow_change'] = (daily_sales['daily_orders'] / daily_sales['daily_orders'].shift(7) - 1) * 100
            daily_sales['units_wow_change'] = (daily_sales['daily_units'] / daily_sales['daily_units'].shift(7) - 1) * 100
            
            # Fill NaN values with 0 for better presentation
            daily_sales.fillna(0, inplace=True)
            
            # Round all metrics for better readability
            numeric_cols = daily_sales.select_dtypes(include=[np.number]).columns
            daily_sales[numeric_cols] = daily_sales[numeric_cols].round(2)
            
            return daily_sales
            
        except Exception as e:
            logger.error(f"Error calculating time metrics: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_product_metrics(self, product_data):
        """
        Calculate product-related metrics.
        
        Args:
            product_data (pandas.DataFrame): Product data
        
        Returns:
            pandas.DataFrame: DataFrame with product metrics
        """
        logger.info("Calculating product metrics")
        
        try:
            # Initialize metrics
            metrics = {}
            
            # Total product count
            metrics['total_products'] = len(product_data)
            
            # Products by category
            if 'category' in product_data.columns:
                category_counts = product_data['category'].value_counts()
                
                # Add category counts to metrics
                for category, count in category_counts.items():
                    metrics[f'products_in_{category.lower().replace(" ", "_")}'] = count
                
                # Calculate category distribution percentages
                category_percentages = (category_counts / category_counts.sum() * 100).round(2)
                
                for category, percentage in category_percentages.items():
                    metrics[f'category_{category.lower().replace(" ", "_")}_percentage'] = percentage
            
            # Inventory metrics
            if 'stock' in product_data.columns:
                metrics['total_inventory'] = product_data['stock'].sum()
                metrics['average_stock_per_product'] = product_data['stock'].mean()
                
                # Count products with zero stock
                metrics['out_of_stock_count'] = (product_data['stock'] == 0).sum()
                metrics['out_of_stock_percentage'] = ((product_data['stock'] == 0).sum() / len(product_data) * 100).round(2)
                
                # Count low stock products (below threshold)
                threshold = self.config.inventory_threshold
                metrics['low_stock_count'] = ((product_data['stock'] > 0) & (product_data['stock'] < threshold)).sum()
                metrics['low_stock_percentage'] = (((product_data['stock'] > 0) & (product_data['stock'] < threshold)).sum() / len(product_data) * 100).round(2)
            
            # Price metrics
            if 'price' in product_data.columns:
                metrics['average_price'] = product_data['price'].mean()
                metrics['median_price'] = product_data['price'].median()
                metrics['min_price'] = product_data['price'].min()
                metrics['max_price'] = product_data['price'].max()
                
                # Price range distribution
                bins = [0, 10, 25, 50, 100, 250, 500, float('inf')]
                labels = ['0-10', '10-25', '25-50', '50-100', '100-250', '250-500', '500+']
                price_ranges = pd.cut(product_data['price'], bins=bins, labels=labels)
                price_distribution = price_ranges.value_counts().sort_index()
                
                # Add price distribution to metrics
                for price_range, count in price_distribution.items():
                    metrics[f'products_price_{price_range}'] = count
            
            # Products on sale metrics
            if 'sale_price' in product_data.columns:
                # Count products on sale
                on_sale = product_data['sale_price'].notna()
                metrics['products_on_sale_count'] = on_sale.sum()
                metrics['products_on_sale_percentage'] = (on_sale.sum() / len(product_data) * 100).round(2)
                
                # Average discount percentage
                if 'price' in product_data.columns:
                    discount_pct = (1 - product_data['sale_price'] / product_data['price']) * 100
                    metrics['average_discount_percentage'] = discount_pct[on_sale].mean().round(2)
            
            # Convert metrics to DataFrame
            metrics_df = pd.DataFrame([metrics])
            
            return metrics_df
            
        except Exception as e:
            logger.error(f"Error calculating product metrics: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_customer_metrics(self, customer_data):
        """
        Calculate customer-related metrics.
        
        Args:
            customer_data (pandas.DataFrame): Customer data
        
        Returns:
            pandas.DataFrame: DataFrame with customer metrics
        """
        logger.info("Calculating customer metrics")
        
        try:
            # Initialize metrics
            metrics = {}
            
            # Total customer count
            metrics['total_customers'] = len(customer_data)
            
            # Customer segments
            if 'segment' in customer_data.columns:
                segment_counts = customer_data['segment'].value_counts()
                
                # Add segment counts to metrics
                for segment, count in segment_counts.items():
                    metrics[f'customers_in_{segment.lower().replace(" ", "_")}'] = count
                
                # Calculate segment distribution percentages
                segment_percentages = (segment_counts / segment_counts.sum() * 100).round(2)
                
                for segment, percentage in segment_percentages.items():
                    metrics[f'segment_{segment.lower().replace(" ", "_")}_percentage'] = percentage
            
            # Geographic distribution
            if 'country' in customer_data.columns:
                country_counts = customer_data['country'].value_counts()
                
                # Add top 5 countries to metrics
                for country, count in country_counts.head(5).items():
                    metrics[f'customers_in_{country.lower().replace(" ", "_")}'] = count
                
                # Calculate percentage of international customers (non-US)
                if 'USA' in country_counts:
                    metrics['international_customers_percentage'] = ((len(customer_data) - country_counts['USA']) / len(customer_data) * 100).round(2)
            
            # Customer lifetime value metrics
            if 'total_spent' in customer_data.columns:
                metrics['average_customer_ltv'] = customer_data['total_spent'].mean()
                metrics['median_customer_ltv'] = customer_data['total_spent'].median()
                metrics['max_customer_ltv'] = customer_data['total_spent'].max()
                
                # LTV distribution
                bins = [0, 50, 100, 250, 500, 1000, 5000, float('inf')]
                labels = ['0-50', '50-100', '100-250', '250-500', '500-1000', '1000-5000', '5000+']
                ltv_ranges = pd.cut(customer_data['total_spent'], bins=bins, labels=labels)
                ltv_distribution = ltv_ranges.value_counts().sort_index()
                
                # Add LTV distribution to metrics
                for ltv_range, count in ltv_distribution.items():
                    metrics[f'customers_ltv_{ltv_range}'] = count
            
            # Customer order metrics
            if 'total_orders' in customer_data.columns:
                metrics['average_orders_per_customer'] = customer_data['total_orders'].mean()
                
                # Count one-time vs. repeat customers
                one_time = (customer_data['total_orders'] == 1).sum()
                repeat = (customer_data['total_orders'] > 1).sum()
                metrics['one_time_customers'] = one_time
                metrics['repeat_customers'] = repeat
                metrics['repeat_purchase_rate'] = (repeat / len(customer_data) * 100).round(2)
            
            # Activity metrics
            if 'last_purchase_date' in customer_data.columns:
                # Ensure date is in datetime format
                customer_data['last_purchase_date'] = pd.to_datetime(customer_data['last_purchase_date'])
                
                # Calculate days since last purchase
                current_date = datetime.now()
                customer_data['days_since_purchase'] = (current_date - customer_data['last_purchase_date']).dt.days
                
                # Active customers (purchased in last 90 days)
                active = (customer_data['days_since_purchase'] <= 90).sum()
                metrics['active_customers'] = active
                metrics['active_customers_percentage'] = (active / len(customer_data) * 100).round(2)
                
                # At-risk customers (91-180 days)
                at_risk = ((customer_data['days_since_purchase'] > 90) & (customer_data['days_since_purchase'] <= 180)).sum()
                metrics['at_risk_customers'] = at_risk
                metrics['at_risk_customers_percentage'] = (at_risk / len(customer_data) * 100).round(2)
                
                # Lapsed customers (180+ days)
                lapsed = (customer_data['days_since_purchase'] > 180).sum()
                metrics['lapsed_customers'] = lapsed
                metrics['lapsed_customers_percentage'] = (lapsed / len(customer_data) * 100).round(2)
            
            # Convert metrics to DataFrame
            metrics_df = pd.DataFrame([metrics])
            
            return metrics_df
            
        except Exception as e:
            logger.error(f"Error calculating customer metrics: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_segmentation_metrics(self, sales_data, customer_data):
        """
        Calculate customer segmentation metrics by combining sales and customer data.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
            customer_data (pandas.DataFrame): Customer data
        
        Returns:
            pandas.DataFrame: DataFrame with segmentation metrics
        """
        logger.info("Calculating customer segmentation metrics")
        
        try:
            # Check if we have required columns
            if 'customer_id' not in sales_data.columns or 'customer_id' not in customer_data.columns:
                logger.warning("Missing customer_id column in data, cannot calculate segmentation metrics")
                return pd.DataFrame()
            
            # Determine revenue column
            if 'final_price' in sales_data.columns:
                revenue_col = 'final_price'
            elif 'total_price' in sales_data.columns:
                revenue_col = 'total_price'
            else:
                logger.warning("No revenue column found, cannot calculate segmentation metrics")
                return pd.DataFrame()
            
            # Merge sales data with customer segments
            if 'segment' in customer_data.columns:
                # Get segments for each customer
                customer_segments = customer_data[['customer_id', 'segment']].drop_duplicates()
                
                # Merge with sales data
                segmented_sales = pd.merge(sales_data, customer_segments, on='customer_id', how='left')
                
                # Fill missing segments
                segmented_sales['segment'].fillna('Unknown', inplace=True)
                
                # Calculate metrics by segment
                segment_metrics = segmented_sales.groupby('segment').agg({
                    revenue_col: 'sum',
                    'order_id': 'nunique',
                    'customer_id': 'nunique',
                    'quantity': 'sum' if 'quantity' in sales_data.columns else 'size'
                }).reset_index()
                
                segment_metrics.rename(columns={
                    revenue_col: 'total_revenue',
                    'order_id': 'total_orders',
                    'customer_id': 'customer_count',
                    'quantity': 'total_units'
                }, inplace=True)
                
                # Calculate additional metrics
                segment_metrics['average_order_value'] = segment_metrics['total_revenue'] / segment_metrics['total_orders']
                segment_metrics['revenue_per_customer'] = segment_metrics['total_revenue'] / segment_metrics['customer_count']
                segment_metrics['orders_per_customer'] = segment_metrics['total_orders'] / segment_metrics['customer_count']
                segment_metrics['units_per_order'] = segment_metrics['total_units'] / segment_metrics['total_orders']
                
                # Calculate percentage of total
                total_revenue = segment_metrics['total_revenue'].sum()
                segment_metrics['revenue_percentage'] = (segment_metrics['total_revenue'] / total_revenue * 100).round(2)
                
                # Round all numeric columns
                numeric_cols = segment_metrics.select_dtypes(include=[np.number]).columns
                segment_metrics[numeric_cols] = segment_metrics[numeric_cols].round(2)
                
                return segment_metrics
            else:
                logger.warning("No segment column found in customer data, cannot calculate segmentation metrics")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error calculating segmentation metrics: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_product_performance(self, sales_data, product_data):
        """
        Calculate product performance metrics by combining sales and product data.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
            product_data (pandas.DataFrame): Product data
        
        Returns:
            pandas.DataFrame: DataFrame with product performance metrics
        """
        logger.info("Calculating product performance metrics")
        
        try:
            # Check if we have required columns
            product_id_col = None
            for col in ['product_id', 'id', 'sku']:
                if col in sales_data.columns and col in product_data.columns:
                    product_id_col = col
                    break
            
            if product_id_col is None:
                logger.warning("No matching product ID column found in both datasets, cannot calculate product performance")
                return pd.DataFrame()
            
            # Determine revenue column
            if 'final_price' in sales_data.columns:
                revenue_col = 'final_price'
            elif 'total_price' in sales_data.columns:
                revenue_col = 'total_price'
            else:
                logger.warning("No revenue column found, cannot calculate product performance")
                return pd.DataFrame()
            
            # Get product metadata
            product_info = product_data[[product_id_col, 'name', 'category']] if 'name' in product_data.columns and 'category' in product_data.columns else product_data[[product_id_col]]
            
            # Calculate product performance metrics
            product_performance = sales_data.groupby(product_id_col).agg({
                revenue_col: 'sum',
                'order_id': 'nunique',
                'quantity': 'sum' if 'quantity' in sales_data.columns else 'size'
            }).reset_index()
            
            product_performance.rename(columns={
                revenue_col: 'total_revenue',
                'order_id': 'order_count',
                'quantity': 'units_sold'
            }, inplace=True)
            
            # Merge with product info
            if len(product_info.columns) > 1:
                product_performance = pd.merge(product_performance, product_info, on=product_id_col, how='left')
            
            # Calculate additional metrics
            product_performance['revenue_per_order'] = product_performance['total_revenue'] / product_performance['order_count']
            product_performance['average_unit_price'] = product_performance['total_revenue'] / product_performance['units_sold']
            
            # Calculate profit if available
            if 'profit' in sales_data.columns:
                profit_by_product = sales_data.groupby(product_id_col)['profit'].sum().reset_index()
                product_performance = pd.merge(product_performance, profit_by_product, on=product_id_col, how='left')
                product_performance['profit_margin'] = (product_performance['profit'] / product_performance['total_revenue'] * 100).round(2)
            
            # Calculate percentage of total revenue
            total_revenue = product_performance['total_revenue'].sum()
            product_performance['revenue_percentage'] = (product_performance['total_revenue'] / total_revenue * 100).round(2)
            
            # Calculate inventory turnover if stock data is available
            if 'stock' in product_data.columns:
                stock_by_product = product_data[[product_id_col, 'stock']]
                product_performance = pd.merge(product_performance, stock_by_product, on=product_id_col, how='left')
                
                # Calculate inventory turnover (units sold / current stock)
                product_performance['inventory_turnover'] = product_performance['units_sold'] / product_performance['stock']
                product_performance['inventory_turnover'] = product_performance['inventory_turnover'].replace([np.inf, -np.inf], np.nan)
                
                # Fill missing values
                product_performance['inventory_turnover'].fillna(0, inplace=True)
                
                # Days of inventory remaining
                product_performance['days_of_inventory'] = product_performance['stock'] / (product_performance['units_sold'] / 30)  # Assuming units_sold is from the last 30 days
                product_performance['days_of_inventory'] = product_performance['days_of_inventory'].replace([np.inf, -np.inf], 365)  # Cap at 1 year for display
                product_performance['days_of_inventory'] = product_performance['days_of_inventory'].fillna(0)
            
            # Sort by revenue
            product_performance = product_performance.sort_values('total_revenue', ascending=False)
            
            # Round all numeric columns
            numeric_cols = product_performance.select_dtypes(include=[np.number]).columns
            product_performance[numeric_cols] = product_performance[numeric_cols].round(2)
            
            return product_performance
            
        except Exception as e:
            logger.error(f"Error calculating product performance metrics: {str(e)}")
            return pd.DataFrame()
