#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Excel Extractor module for extracting e-commerce customer data from Excel files.
"""

import logging
import pandas as pd
import os
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class ExcelExtractor:
    """Extracts e-commerce customer data from Excel files."""
    
    def __init__(self, config):
        """
        Initialize the Excel Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.customers_excel
    
    def extract(self, start_date=None, end_date=None, sheet_name=None):
        """
        Extract data from Excel file with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            sheet_name (str, optional): Name of the Excel sheet to extract data from
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the Excel file doesn't exist
        """
        logger.info(f"Extracting data from Excel file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"Excel file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # If sheet_name is provided, use it, otherwise read the first sheet
            if sheet_name:
                logger.info(f"Reading sheet: {sheet_name}")
                df = pd.read_excel(self.source_file, sheet_name=sheet_name, engine='openpyxl')
            else:
                # Get available sheet names
                xlsx = pd.ExcelFile(self.source_file, engine='openpyxl')
                sheet_names = xlsx.sheet_names
                logger.info(f"Available sheets: {', '.join(sheet_names)}")
                
                # Read all sheets into a dictionary of dataframes
                all_sheets = pd.read_excel(self.source_file, sheet_name=None, engine='openpyxl')
                
                # If there's only one sheet, return it
                if len(all_sheets) == 1:
                    df = list(all_sheets.values())[0]
                else:
                    # If there's a sheet named 'Customers', use it
                    if 'Customers' in all_sheets:
                        df = all_sheets['Customers']
                    else:
                        # Otherwise, use the first sheet
                        df = list(all_sheets.values())[0]
            
            # Identify date columns
            date_columns = []
            for col in df.columns:
                if 'date' in str(col).lower():
                    # Try to convert to datetime
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        date_columns.append(col)
                    except:
                        pass
            
            # Apply date filtering if provided
            if start_date and date_columns:
                start_date = pd.to_datetime(start_date)
                for col in date_columns:
                    # Create a temporary filter for each date column
                    filter_condition = df[col] >= start_date
                    # Apply filter only if any matches are found
                    if filter_condition.any():
                        df = df[filter_condition]
                        logger.info(f"Applied start date filter on column {col}")
                        break  # Only filter by one date column
            
            if end_date and date_columns:
                end_date = pd.to_datetime(end_date)
                for col in date_columns:
                    # Create a temporary filter for each date column
                    filter_condition = df[col] <= end_date
                    # Apply filter only if any matches are found
                    if filter_condition.any():
                        df = df[filter_condition]
                        logger.info(f"Applied end date filter on column {col}")
                        break  # Only filter by one date column
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from Excel file")
            if not df.empty:
                logger.info(f"Columns in Excel data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from Excel file: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample e-commerce customer data in Excel format."""
        try:
            # Check if openpyxl is installed
            import openpyxl
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
            
            # Generate sample customer data
            customers = []
            
            # Generate random dates over the past 3 years
            end_date = datetime.now()
            start_date = end_date - pd.Timedelta(days=3*365)
            registration_dates = pd.date_range(start=start_date, end=end_date, periods=500)
            
            # Define customer segments and their probabilities
            segments = ['New', 'Regular', 'VIP', 'Inactive', 'Wholesale']
            segment_probs = [0.2, 0.4, 0.1, 0.2, 0.1]
            
            # Define possible countries with US being more common
            countries = ['USA', 'Canada', 'UK', 'Australia', 'Germany', 'France', 'Japan', 'Mexico', 'Brazil']
            country_probs = [0.7, 0.05, 0.05, 0.03, 0.03, 0.03, 0.03, 0.04, 0.04]
            
            # Generate customer data
            for i in range(500):
                customer_id = f"CUST-{1000 + i}"
                email = f"customer_{1000 + i}@example.com"
                
                # Generate random name
                first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'James', 'Jennifer', 'Robert', 'Lisa']
                last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
                first_name = np.random.choice(first_names)
                last_name = np.random.choice(last_names)
                name = f"{first_name} {last_name}"
                
                # Use realistic phone number format
                phone = f"({np.random.randint(100, 999)}) {np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}"
                
                # Registration date and demographics
                registration_date = registration_dates[i]
                segment = np.random.choice(segments, p=segment_probs)
                country = np.random.choice(countries, p=country_probs)
                
                # Address information
                if country == 'USA':
                    states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
                    state = np.random.choice(states)
                    zip_code = f"{np.random.randint(10000, 99999)}"
                else:
                    state = ''
                    zip_code = f"{np.random.randint(10000, 99999)}" if country in ['Canada', 'USA'] else f"{np.random.randint(10, 999)}-{np.random.randint(100, 9999)}"
                
                # Purchase history
                total_orders = np.random.randint(0, 50) if segment != 'New' else 0
                if segment == 'VIP':
                    total_orders = np.random.randint(20, 100)
                elif segment == 'Regular':
                    total_orders = np.random.randint(5, 30)
                elif segment == 'Inactive':
                    total_orders = np.random.randint(1, 5)
                elif segment == 'Wholesale':
                    total_orders = np.random.randint(10, 50)
                
                # Calculate average order value based on segment
                if segment == 'VIP':
                    aov = np.random.uniform(100, 500)
                elif segment == 'Regular':
                    aov = np.random.uniform(50, 150)
                elif segment == 'Inactive':
                    aov = np.random.uniform(20, 80)
                elif segment == 'Wholesale':
                    aov = np.random.uniform(300, 1000)
                else:  # New
                    aov = 0
                
                total_spent = round(total_orders * aov, 2) if total_orders > 0 else 0
                
                # Last purchase date
                if total_orders > 0:
                    days_since_registration = (end_date - registration_date).days
                    max_days = min(days_since_registration, 365)  # Cap at 1 year or registration period
                    if segment == 'Inactive':
                        # Inactive customers haven't purchased recently
                        last_purchase_days_ago = np.random.randint(180, max(max_days, 181))
                    else:
                        # More recent purchases for active customers
                        last_purchase_days_ago = np.random.randint(1, min(90, max_days))
                    
                    last_purchase_date = end_date - pd.Timedelta(days=last_purchase_days_ago)
                else:
                    last_purchase_date = None
                
                # Customer preferences
                preferred_categories = np.random.choice(['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys'], 
                                                       size=np.random.randint(1, 4), 
                                                       replace=False)
                preferred_payment = np.random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery'])
                
                # Add to customers list
                customers.append({
                    'customer_id': customer_id,
                    'name': name,
                    'email': email,
                    'phone': phone,
                    'registration_date': registration_date,
                    'last_purchase_date': last_purchase_date,
                    'segment': segment,
                    'country': country,
                    'state': state,
                    'zip_code': zip_code,
                    'total_orders': total_orders,
                    'total_spent': total_spent,
                    'avg_order_value': round(aov, 2),
                    'preferred_categories': ', '.join(preferred_categories),
                    'preferred_payment': preferred_payment,
                    'is_subscribed': np.random.choice([True, False], p=[0.7, 0.3]),
                    'has_returned_items': np.random.choice([True, False], p=[0.1, 0.9]),
                })
            
            # Create a DataFrame
            df_customers = pd.DataFrame(customers)
            
            # Create a second sheet with customer feedback
            feedback = []
            
            # Generate feedback for a subset of customers
            for i in range(150):
                customer_idx = np.random.randint(0, len(customers))
                customer = customers[customer_idx]
                
                # Feedback data
                feedback_date = customer['last_purchase_date']
                if feedback_date is not None:
                    feedback_date += pd.Timedelta(days=np.random.randint(1, 14))
                else:
                    continue  # Skip customers with no purchases
                
                # Rating on 1-5 scale
                if customer['segment'] in ['VIP', 'Regular']:
                    # Satisfied customers are more likely to be VIPs or Regulars
                    rating = np.random.choice([3, 4, 5], p=[0.1, 0.3, 0.6])
                else:
                    # More variation in other segments
                    rating = np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.1, 0.2, 0.35, 0.3])
                
                # Feedback text based on rating
                if rating >= 4:
                    comments = np.random.choice([
                        "Great experience shopping on your site!",
                        "Products arrived quickly and in perfect condition.",
                        "Excellent customer service and product quality.",
                        "Very satisfied with my purchase.",
                        "Will definitely buy from you again."
                    ])
                elif rating == 3:
                    comments = np.random.choice([
                        "Decent experience but room for improvement.",
                        "Product was okay but shipping was slow.",
                        "Satisfactory overall but nothing exceptional.",
                        "Product met expectations but pricing was high.",
                        "Average service, may or may not return."
                    ])
                else:
                    comments = np.random.choice([
                        "Disappointed with product quality.",
                        "Shipping took too long.",
                        "Customer service was unhelpful.",
                        "Product did not match description.",
                        "Had issues with my order."
                    ])
                
                # Add to feedback list
                feedback.append({
                    'customer_id': customer['customer_id'],
                    'feedback_date': feedback_date,
                    'rating': rating,
                    'comments': comments,
                    'product_category': np.random.choice(customer['preferred_categories'].split(', ')),
                    'would_recommend': rating >= 4,
                    'feedback_source': np.random.choice(['Email', 'Website', 'App', 'Phone']),
                })
            
            # Create a DataFrame for feedback
            df_feedback = pd.DataFrame(feedback)
            
            # Create a third sheet with customer segments analysis
            segment_analysis = []
            for segment in segments:
                segment_customers = [c for c in customers if c['segment'] == segment]
                if not segment_customers:
                    continue
                    
                avg_orders = np.mean([c['total_orders'] for c in segment_customers])
                avg_spent = np.mean([c['total_spent'] for c in segment_customers])
                count = len(segment_customers)
                
                # Count customers by country
                countries_count = {}
                for c in segment_customers:
                    country = c['country']
                    countries_count[country] = countries_count.get(country, 0) + 1
                
                # Find most common country
                most_common_country = max(countries_count.items(), key=lambda x: x[1])[0] if countries_count else 'N/A'
                
                # Calculate retention rate (customers with more than 1 order)
                retention_rate = len([c for c in segment_customers if c['total_orders'] > 1]) / count if count > 0 else 0
                
                segment_analysis.append({
                    'segment': segment,
                    'customer_count': count,
                    'avg_orders_per_customer': round(avg_orders, 2),
                    'avg_lifetime_value': round(avg_spent, 2),
                    'most_common_country': most_common_country,
                    'retention_rate': round(retention_rate, 2),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                    'growth_opportunity': np.random.choice(['High', 'Medium', 'Low']),
                    'recommended_actions': np.random.choice([
                        "Targeted email campaign",
                        "Loyalty program enhancements",
                        "Re-engagement offers",
                        "Premium support services",
                        "Bulk order discounts"
                    ])
                })
            
            # Create a DataFrame for segment analysis
            df_segment_analysis = pd.DataFrame(segment_analysis)
            
            # Create Excel writer with multiple sheets
            with pd.ExcelWriter(self.source_file, engine='openpyxl') as writer:
                df_customers.to_excel(writer, sheet_name='Customers', index=False)
                df_feedback.to_excel(writer, sheet_name='Customer Feedback', index=False)
                df_segment_analysis.to_excel(writer, sheet_name='Segment Analysis', index=False)
            
            logger.info(f"Created sample Excel file with customer data: {self.source_file}")
            
        except ImportError:
            logger.error("Failed to create sample Excel file due to missing dependencies. Install with: pip install openpyxl")
            
            # Create a directory if it doesn't exist
            os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
            
            # Create an empty file as a placeholder
            with open(self.source_file, 'w') as f:
                f.write("Sample Excel placeholder")
        except Exception as e:
            logger.error(f"Error creating sample Excel file: {str(e)}")
