#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Report Generator module for creating formatted reports from processed e-commerce data.
"""

import logging
import pandas as pd
import os
import numpy as np
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generates formatted reports from processed e-commerce data."""
    
    def __init__(self, config):
        """
        Initialize the Report Generator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.reports_dir = config.reports_dir
    
    def generate(self, data, report_type='sales_summary', output_dir=None):
        """
        Generate reports from processed data.
        
        Args:
            data (pandas.DataFrame or dict): Data to generate reports from
            report_type (str): Type of report to generate
                              ('sales_summary', 'product_performance', 'customer_segmentation', 'inventory_status')
            output_dir (str, optional): Output directory for reports
        
        Returns:
            dict: Dictionary containing report file paths
        """
        if data is None:
            logger.warning("No data to generate reports from")
            return {}
        
        # Set output directory
        output_dir = output_dir or self.reports_dir
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Get current timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize dictionary for report files
        report_files = {}
        
        try:
            # Generate report based on type
            if report_type == 'sales_summary':
                report_files = self._generate_sales_summary(data, output_dir, timestamp)
            elif report_type == 'product_performance':
                report_files = self._generate_product_performance(data, output_dir, timestamp)
            elif report_type == 'customer_segmentation':
                report_files = self._generate_customer_segmentation(data, output_dir, timestamp)
            elif report_type == 'inventory_status':
                report_files = self._generate_inventory_status(data, output_dir, timestamp)
            else:
                logger.warning(f"Unsupported report type: {report_type}")
            
            return report_files
            
        except Exception as e:
            logger.error(f"Error generating {report_type} report: {str(e)}")
            return {}
    
    def _generate_sales_summary(self, data, output_dir, timestamp):
        """
        Generate sales summary report.
        
        Args:
            data (pandas.DataFrame or dict): Data to generate report from
            output_dir (str): Output directory for report
            timestamp (str): Timestamp for filenames
        
        Returns:
            dict: Dictionary containing report file paths
        """
        logger.info("Generating sales summary report")
        
        # Initialize report files dictionary
        report_files = {}
        
        # Extract required data
        sales_data = None
        sales_metrics = None
        time_metrics = None
        
        if isinstance(data, dict):
            # Try to find sales data in dictionary
            if 'sales_data' in data:
                sales_data = data['sales_data']
            elif 'sales' in data:
                sales_data = data['sales']
            
            # Try to find metrics data
            if 'sales_metrics' in data:
                sales_metrics = data['sales_metrics']
            
            if 'time_metrics' in data:
                time_metrics = data['time_metrics']
        else:
            # Assume data is sales data
            sales_data = data
        
        # Generate CSV report
        csv_file = os.path.join(output_dir, f"sales_summary_{timestamp}.csv")
        
        # Generate report content based on available data
        if sales_metrics is not None and not sales_metrics.empty:
            # Use pre-calculated metrics
            sales_metrics.to_csv(csv_file, index=False)
            report_files['csv'] = csv_file
            logger.info(f"Generated sales summary CSV report: {csv_file}")
        elif sales_data is not None and not sales_data.empty:
            # Calculate metrics from sales data
            metrics = self._calculate_sales_metrics(sales_data)
            metrics.to_csv(csv_file, index=False)
            report_files['csv'] = csv_file
            logger.info(f"Generated sales summary CSV report: {csv_file}")
        else:
            logger.warning("No sales data available for report")
            return report_files
        
        # Generate HTML report
        html_file = os.path.join(output_dir, f"sales_summary_{timestamp}.html")
        
        # Prepare HTML content
        html_content = self._generate_html_header("E-commerce Sales Summary")
        
        # Add summary metrics
        if sales_metrics is not None and not sales_metrics.empty:
            html_content += "<h2>Sales Summary Metrics</h2>"
            html_content += self._metrics_to_html_table(sales_metrics)
        
        # Add time series data if available
        if time_metrics is not None and not time_metrics.empty:
            html_content += "<h2>Sales Trends</h2>"
            html_content += "<p>Daily sales trends and metrics</p>"
            html_content += time_metrics.tail(30).to_html(index=False, classes="data-table")
        
        # Close HTML document
        html_content += "</body></html>"
        
        # Write HTML file
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        report_files['html'] = html_file
        logger.info(f"Generated sales summary HTML report: {html_file}")
        
        # Generate JSON report
        json_file = os.path.join(output_dir, f"sales_summary_{timestamp}.json")
        
        # Prepare JSON content
        json_content = {}
        
        if sales_metrics is not None and not sales_metrics.empty:
            # Convert metrics to dictionary
            metrics_dict = sales_metrics.to_dict(orient='records')[0]
            json_content['sales_metrics'] = metrics_dict
        
        if time_metrics is not None and not time_metrics.empty:
            # Get recent trends (last 30 days)
            recent_trends = time_metrics.tail(30).copy()
            
            # Convert datetime columns to strings
            for col in recent_trends.select_dtypes(include=['datetime64']).columns:
                recent_trends[col] = recent_trends[col].dt.strftime('%Y-%m-%d')
            
            json_content['time_trends'] = recent_trends.to_dict(orient='records')
        
        # Write JSON file
        with open(json_file, 'w') as f:
            json.dump(json_content, f, indent=2)
        
        report_files['json'] = json_file
        logger.info(f"Generated sales summary JSON report: {json_file}")
        
        return report_files
    
    def _generate_product_performance(self, data, output_dir, timestamp):
        """
        Generate product performance report.
        
        Args:
            data (pandas.DataFrame or dict): Data to generate report from
            output_dir (str): Output directory for report
            timestamp (str): Timestamp for filenames
        
        Returns:
            dict: Dictionary containing report file paths
        """
        logger.info("Generating product performance report")
        
        # Initialize report files dictionary
        report_files = {}
        
        # Extract required data
        product_data = None
        product_metrics = None
        product_performance = None
        
        if isinstance(data, dict):
            # Try to find product data in dictionary
            if 'product_data' in data:
                product_data = data['product_data']
            elif 'products' in data:
                product_data = data['products']
            
            # Try to find metrics data
            if 'product_metrics' in data:
                product_metrics = data['product_metrics']
            
            if 'product_performance' in data:
                product_performance = data['product_performance']
        else:
            # Assume data is product data
            product_data = data
        
        # Generate CSV report for product performance
        if product_performance is not None and not product_performance.empty:
            csv_file = os.path.join(output_dir, f"product_performance_{timestamp}.csv")
            product_performance.to_csv(csv_file, index=False)
            report_files['csv'] = csv_file
            logger.info(f"Generated product performance CSV report: {csv_file}")
        else:
            logger.warning("No product performance data available for report")
        
        # Generate HTML report
        html_file = os.path.join(output_dir, f"product_performance_{timestamp}.html")
        
        # Prepare HTML content
        html_content = self._generate_html_header("E-commerce Product Performance")
        
        # Add product metrics if available
        if product_metrics is not None and not product_metrics.empty:
            html_content += "<h2>Product Metrics Overview</h2>"
            html_content += self._metrics_to_html_table(product_metrics)
        
        # Add product performance data if available
        if product_performance is not None and not product_performance.empty:
            html_content += "<h2>Top Performing Products</h2>"
            html_content += "<p>Products sorted by revenue (top 20 shown)</p>"
            
            # Get top 20 products by revenue
            top_products = product_performance.head(20)
            html_content += top_products.to_html(index=False, classes="data-table")
        
        # Add product data if available
        if product_data is not None and not product_data.empty:
            html_content += "<h2>Product Inventory</h2>"
            
            # Filter columns to display
            display_columns = ['id', 'name', 'category', 'price', 'stock', 'stock_status'] if all(col in product_data.columns for col in ['id', 'name', 'category', 'price', 'stock', 'stock_status']) else None
            
            if display_columns:
                html_content += product_data[display_columns].head(20).to_html(index=False, classes="data-table")
            else:
                html_content += product_data.head(20).to_html(index=False, classes="data-table")
        
        # Close HTML document
        html_content += "</body></html>"
        
        # Write HTML file
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        report_files['html'] = html_file
        logger.info(f"Generated product performance HTML report: {html_file}")
        
        return report_files
    
    def _generate_customer_segmentation(self, data, output_dir, timestamp):
        """
        Generate customer segmentation report.
        
        Args:
            data (pandas.DataFrame or dict): Data to generate report from
            output_dir (str): Output directory for report
            timestamp (str): Timestamp for filenames
        
        Returns:
            dict: Dictionary containing report file paths
        """
        logger.info("Generating customer segmentation report")
        
        # Initialize report files dictionary
        report_files = {}
        
        # Extract required data
        customer_data = None
        customer_metrics = None
        segmentation_metrics = None
        
        if isinstance(data, dict):
            # Try to find customer data in dictionary
            if 'customer_data' in data:
                customer_data = data['customer_data']
            elif 'customers' in data:
                customer_data = data['customers']
            
            # Try to find metrics data
            if 'customer_metrics' in data:
                customer_metrics = data['customer_metrics']
            
            if 'segmentation_metrics' in data:
                segmentation_metrics = data['segmentation_metrics']
        else:
            # Assume data is customer data
            customer_data = data
        
        # Generate CSV report for segmentation metrics
        if segmentation_metrics is not None and not segmentation_metrics.empty:
            csv_file = os.path.join(output_dir, f"customer_segmentation_{timestamp}.csv")
            segmentation_metrics.to_csv(csv_file, index=False)
            report_files['csv'] = csv_file
            logger.info(f"Generated customer segmentation CSV report: {csv_file}")
        
        # Generate HTML report
        html_file = os.path.join(output_dir, f"customer_segmentation_{timestamp}.html")
        
        # Prepare HTML content
        html_content = self._generate_html_header("E-commerce Customer Segmentation")
        
        # Add customer metrics if available
        if customer_metrics is not None and not customer_metrics.empty:
            html_content += "<h2>Customer Metrics Overview</h2>"
            html_content += self._metrics_to_html_table(customer_metrics)
        
        # Add segmentation metrics if available
        if segmentation_metrics is not None and not segmentation_metrics.empty:
            html_content += "<h2>Customer Segment Performance</h2>"
            html_content += segmentation_metrics.to_html(index=False, classes="data-table")
        
        # Add customer data if available
        if customer_data is not None and not customer_data.empty:
            html_content += "<h2>Customer Sample</h2>"
            
            # Filter columns to display (exclude sensitive information)
            exclude_columns = ['email', 'phone', 'address', 'credit_card']
            display_columns = [col for col in customer_data.columns if col not in exclude_columns]
            
            html_content += customer_data[display_columns].head(20).to_html(index=False, classes="data-table")
        
        # Close HTML document
        html_content += "</body></html>"
        
        # Write HTML file
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        report_files['html'] = html_file
        logger.info(f"Generated customer segmentation HTML report: {html_file}")
        
        return report_files
    
    def _generate_inventory_status(self, data, output_dir, timestamp):
        """
        Generate inventory status report.
        
        Args:
            data (pandas.DataFrame or dict): Data to generate report from
            output_dir (str): Output directory for report
            timestamp (str): Timestamp for filenames
        
        Returns:
            dict: Dictionary containing report file paths
        """
        logger.info("Generating inventory status report")
        
        # Initialize report files dictionary
        report_files = {}
        
        # Extract required data
        product_data = None
        inventory_data = None
        
        if isinstance(data, dict):
            # Try to find product/inventory data in dictionary
            if 'product_data' in data:
                product_data = data['product_data']
            elif 'products' in data:
                product_data = data['products']
            
            if 'inventory_data' in data:
                inventory_data = data['inventory_data']
            elif 'inventory' in data:
                inventory_data = data['inventory']
        else:
            # Assume data is product/inventory data
            product_data = data
        
        # Use product data if inventory data is not available
        if inventory_data is None and product_data is not None:
            inventory_data = product_data
        
        # Check if we have inventory data to report on
        if inventory_data is None or inventory_data.empty:
            logger.warning("No inventory data available for report")
            return report_files
        
        # Generate inventory status report
        # Check if we have stock information
        has_stock_info = 'stock' in inventory_data.columns
        
        if has_stock_info:
            # Generate CSV report
            csv_file = os.path.join(output_dir, f"inventory_status_{timestamp}.csv")
            
            # Create a sorted copy of inventory data
            sorted_inventory = inventory_data.sort_values('stock') if 'stock' in inventory_data.columns else inventory_data
            
            sorted_inventory.to_csv(csv_file, index=False)
            report_files['csv'] = csv_file
            logger.info(f"Generated inventory status CSV report: {csv_file}")
            
            # Generate HTML report
            html_file = os.path.join(output_dir, f"inventory_status_{timestamp}.html")
            
            # Prepare HTML content
            html_content = self._generate_html_header("E-commerce Inventory Status")
            
            # Add inventory metrics
            html_content += "<h2>Inventory Overview</h2>"
            
            if 'stock' in inventory_data.columns:
                # Calculate inventory metrics
                total_inventory = inventory_data['stock'].sum()
                out_of_stock = (inventory_data['stock'] == 0).sum()
                low_stock_threshold = self.config.inventory_threshold
                low_stock = ((inventory_data['stock'] > 0) & (inventory_data['stock'] <= low_stock_threshold)).sum()
                
                # Create a metrics summary
                metrics_html = f"""
                <div class="metrics-summary">
                    <div class="metric">
                        <span class="metric-value">{total_inventory}</span>
                        <span class="metric-label">Total Items in Stock</span>
                    </div>
                    <div class="metric">
                        <span class="metric-value">{len(inventory_data)}</span>
                        <span class="metric-label">Total Products</span>
                    </div>
                    <div class="metric">
                        <span class="metric-value">{out_of_stock}</span>
                        <span class="metric-label">Out of Stock Items</span>
                    </div>
                    <div class="metric">
                        <span class="metric-value">{low_stock}</span>
                        <span class="metric-label">Low Stock Items (≤ {low_stock_threshold})</span>
                    </div>
                </div>
                """
                
                html_content += metrics_html
            
            # Add out of stock items
            if 'stock' in inventory_data.columns:
                html_content += "<h2>Out of Stock Items</h2>"
                out_of_stock_items = inventory_data[inventory_data['stock'] == 0]
                
                if not out_of_stock_items.empty:
                    # Filter columns to display
                    display_columns = ['id', 'name', 'category', 'price', 'stock_status'] if all(col in out_of_stock_items.columns for col in ['id', 'name', 'category', 'price', 'stock_status']) else None
                    
                    if display_columns:
                        html_content += out_of_stock_items[display_columns].to_html(index=False, classes="data-table")
                    else:
                        html_content += out_of_stock_items.to_html(index=False, classes="data-table")
                else:
                    html_content += "<p>No items are currently out of stock.</p>"
                
                # Add low stock items
                html_content += f"<h2>Low Stock Items (≤ {low_stock_threshold})</h2>"
                low_stock_items = inventory_data[(inventory_data['stock'] > 0) & (inventory_data['stock'] <= low_stock_threshold)]
                
                if not low_stock_items.empty:
                    # Sort by stock level (ascending)
                    low_stock_items = low_stock_items.sort_values('stock')
                    
                    # Filter columns to display
                    display_columns = ['id', 'name', 'category', 'price', 'stock', 'stock_status'] if all(col in low_stock_items.columns for col in ['id', 'name', 'category', 'price', 'stock', 'stock_status']) else None
                    
                    if display_columns:
                        html_content += low_stock_items[display_columns].to_html(index=False, classes="data-table")
                    else:
                        html_content += low_stock_items.to_html(index=False, classes="data-table")
                else:
                    html_content += f"<p>No items are currently below the stock threshold of {low_stock_threshold}.</p>"
            
            # Close HTML document
            html_content += "</body></html>"
            
            # Write HTML file
            with open(html_file, 'w') as f:
                f.write(html_content)
            
            report_files['html'] = html_file
            logger.info(f"Generated inventory status HTML report: {html_file}")
        
        return report_files
    
    def _generate_html_header(self, title):
        """
        Generate HTML header with CSS styling.
        
        Args:
            title (str): Report title
        
        Returns:
            str: HTML header content
        """
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    color: #333;
                    background-color: #f9f9f9;
                }}
                h1 {{
                    color: #2c3e50;
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 10px;
                }}
                h2 {{
                    color: #2980b9;
                    margin-top: 30px;
                }}
                .data-table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px 0;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                .data-table th {{
                    background-color: #3498db;
                    color: white;
                    padding: 12px 15px;
                    text-align: left;
                }}
                .data-table td {{
                    border: 1px solid #ddd;
                    padding: 10px 15px;
                }}
                .data-table tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                .data-table tr:hover {{
                    background-color: #e3f2fd;
                }}
                .metrics-table {{
                    margin: 20px 0;
                    border-collapse: collapse;
                    width: 100%;
                }}
                .metrics-table td {{
                    padding: 8px 15px;
                    border: 1px solid #ddd;
                }}
                .metrics-table td:first-child {{
                    font-weight: bold;
                    background-color: #f2f2f2;
                    width: 40%;
                }}
                .metrics-summary {{
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: space-between;
                    margin: 20px 0;
                }}
                .metric {{
                    background-color: white;
                    border-radius: 8px;
                    padding: 20px;
                    margin: 10px 0;
                    width: calc(25% - 20px);
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    text-align: center;
                }}
                .metric-value {{
                    display: block;
                    font-size: 2em;
                    font-weight: bold;
                    color: #3498db;
                    margin-bottom: 10px;
                }}
                .metric-label {{
                    display: block;
                    font-size: 0.9em;
                    color: #7f8c8d;
                }}
                @media (max-width: 768px) {{
                    .metric {{
                        width: calc(50% - 20px);
                    }}
                }}
                footer {{
                    margin-top: 50px;
                    text-align: center;
                    color: #7f8c8d;
                    font-size: 0.8em;
                }}
            </style>
        </head>
        <body>
            <h1>{title}</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
    
    def _metrics_to_html_table(self, metrics_df):
        """
        Convert metrics DataFrame to an HTML table.
        
        Args:
            metrics_df (pandas.DataFrame): Metrics DataFrame
        
        Returns:
            str: HTML table content
        """
        # Check if metrics is empty
        if metrics_df is None or metrics_df.empty:
            return "<p>No metrics data available.</p>"
        
        # Convert DataFrame to a single row format
        if len(metrics_df) == 1:
            # Transpose the DataFrame to make it key-value pairs
            metrics_transposed = metrics_df.transpose().reset_index()
            metrics_transposed.columns = ['Metric', 'Value']
            
            # Format the HTML table
            html = '<table class="metrics-table">'
            
            for _, row in metrics_transposed.iterrows():
                metric = row['Metric']
                value = row['Value']
                
                # Format numeric values
                if isinstance(value, (int, float)):
                    if 'percentage' in metric.lower() or metric.lower().endswith('_pct'):
                        value = f"{value:.2f}%"
                    elif 'price' in metric.lower() or 'revenue' in metric.lower() or 'sales' in metric.lower() or 'value' in metric.lower():
                        value = f"${value:,.2f}"
                    elif isinstance(value, int):
                        value = f"{value:,}"
                    else:
                        value = f"{value:,.2f}"
                
                html += f'<tr><td>{metric}</td><td>{value}</td></tr>'
            
            html += '</table>'
            return html
        else:
            # Return regular DataFrame HTML
            return metrics_df.to_html(index=False, classes="data-table")
    
    def _calculate_sales_metrics(self, sales_data):
        """
        Calculate basic sales metrics from sales data.
        
        Args:
            sales_data (pandas.DataFrame): Sales data
        
        Returns:
            pandas.DataFrame: DataFrame with calculated metrics
        """
        metrics = {}
        
        try:
            # Determine revenue column
            if 'final_price' in sales_data.columns:
                revenue_col = 'final_price'
            elif 'total_price' in sales_data.columns:
                revenue_col = 'total_price'
            else:
                # No revenue column found
                metrics['total_orders'] = len(sales_data) if 'order_id' not in sales_data.columns else sales_data['order_id'].nunique()
                return pd.DataFrame([metrics])
            
            # Calculate basic metrics
            metrics['total_revenue'] = sales_data[revenue_col].sum()
            metrics['total_orders'] = len(sales_data) if 'order_id' not in sales_data.columns else sales_data['order_id'].nunique()
            metrics['average_order_value'] = metrics['total_revenue'] / metrics['total_orders']
            
            if 'quantity' in sales_data.columns:
                metrics['total_units_sold'] = sales_data['quantity'].sum()
                metrics['average_units_per_order'] = metrics['total_units_sold'] / metrics['total_orders']
            
            # Return as DataFrame
            return pd.DataFrame([metrics])
            
        except Exception as e:
            logger.error(f"Error calculating sales metrics: {str(e)}")
            return pd.DataFrame()
