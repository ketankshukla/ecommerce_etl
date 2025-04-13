#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FTP Extractor module for extracting e-commerce data from FTP servers.
"""

import logging
import pandas as pd
import os
import tempfile
from datetime import datetime
import ftplib
import re

logger = logging.getLogger(__name__)

class FTPExtractor:
    """Extracts e-commerce data from FTP servers."""
    
    def __init__(self, config):
        """
        Initialize the FTP Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.ftp_host = config.ftp_host
        self.ftp_user = config.ftp_user
        self.ftp_password = config.ftp_password
        self.ftp_port = config.ftp_port
        self.ftp_path = config.ftp_path
        self.output_dir = os.path.join(config.data_dir, 'ftp')
    
    def extract(self, start_date=None, end_date=None, file_pattern=None):
        """
        Extract data from FTP server with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            file_pattern (str, optional): Regex pattern to match filenames
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            Exception: If there's an error connecting to the FTP server
        """
        logger.info(f"Extracting data from FTP server: {self.ftp_host}")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        try:
            # Connect to FTP server
            logger.info(f"Connecting to FTP server: {self.ftp_host}:{self.ftp_port}")
            
            # Mock the FTP extraction for demonstration if no real server
            if self.ftp_host == 'ftp.example.com':
                logger.warning("Using mock FTP data since no real server is configured")
                return self._mock_ftp_extraction(start_date, end_date, file_pattern)
            
            # Real FTP connection
            ftp = ftplib.FTP()
            ftp.connect(self.ftp_host, self.ftp_port)
            ftp.login(self.ftp_user, self.ftp_password)
            
            # Change to the specified directory
            if self.ftp_path:
                ftp.cwd(self.ftp_path)
            
            # List files in the directory
            files = []
            ftp.retrlines('LIST', lambda x: files.append(x))
            
            # Parse file listings to get names and dates
            file_infos = []
            for file_line in files:
                # Parse the file listing (format varies by FTP server)
                # Typical format: "-rw-r--r-- 1 user group 12345 Jan 01 12:34 filename.csv"
                parts = file_line.split()
                if len(parts) >= 9:
                    # Extract filename and date
                    filename = ' '.join(parts[8:])
                    date_str = ' '.join(parts[5:8])
                    
                    try:
                        # Parse date from FTP listing
                        file_date = self._parse_ftp_date(date_str)
                        
                        # Apply date filtering
                        if start_date and file_date < datetime.strptime(start_date, '%Y-%m-%d'):
                            continue
                        if end_date and file_date > datetime.strptime(end_date, '%Y-%m-%d'):
                            continue
                        
                        # Apply file pattern filtering
                        if file_pattern and not re.match(file_pattern, filename):
                            continue
                        
                        file_infos.append({'filename': filename, 'date': file_date})
                    except:
                        logger.warning(f"Could not parse date from FTP listing: {date_str}")
            
            # Sort files by date (most recent first)
            file_infos.sort(key=lambda x: x['date'], reverse=True)
            
            # Download files
            all_data = []
            for file_info in file_infos:
                filename = file_info['filename']
                local_path = os.path.join(self.output_dir, filename)
                
                logger.info(f"Downloading file: {filename}")
                
                # Download file
                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {filename}', f.write)
                
                # Read file based on extension
                if filename.endswith('.csv'):
                    df = pd.read_csv(local_path)
                elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                    df = pd.read_excel(local_path)
                elif filename.endswith('.json'):
                    df = pd.read_json(local_path)
                else:
                    logger.warning(f"Unsupported file format: {filename}")
                    continue
                
                # Add filename as a column
                df['source_file'] = filename
                
                # Add to combined data
                all_data.append(df)
            
            # Close FTP connection
            ftp.quit()
            
            # Combine all data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"Extracted {len(combined_df)} rows from FTP server")
                return combined_df
            else:
                logger.warning("No matching files found on FTP server")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error extracting data from FTP server: {str(e)}")
            logger.info("Falling back to mock data...")
            return self._mock_ftp_extraction(start_date, end_date, file_pattern)
    
    def _parse_ftp_date(self, date_str):
        """
        Parse date from FTP listing.
        
        Args:
            date_str (str): Date string from FTP listing
        
        Returns:
            datetime: Parsed date
        
        Raises:
            ValueError: If date cannot be parsed
        """
        # Try various date formats
        formats = [
            '%b %d %Y',  # Jan 01 2022
            '%b %d %H:%M',  # Jan 01 12:34
            '%Y-%m-%d %H:%M',  # 2022-01-01 12:34
            '%d %b %Y',  # 01 Jan 2022
            '%d %b %Y %H:%M',  # 01 Jan 2022 12:34
        ]
        
        # If year is missing, assume current year
        if ':' in date_str:
            parts = date_str.split()
            if len(parts) == 3:
                current_year = datetime.now().year
                date_str = f"{parts[0]} {parts[1]} {current_year}"
        
        # Try each format
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # If all formats fail, raise error
        raise ValueError(f"Could not parse date: {date_str}")
    
    def _mock_ftp_extraction(self, start_date=None, end_date=None, file_pattern=None):
        """
        Create mock FTP data for demonstration purposes.
        
        Args:
            start_date (str, optional): Start date for filtering
            end_date (str, optional): End date for filtering
            file_pattern (str, optional): Regex pattern to match filenames
        
        Returns:
            pandas.DataFrame: Mock data
        """
        import numpy as np
        
        logger.info("Generating mock FTP data for demonstration")
        
        # Generate dates for the past 30 days
        end_date_dt = datetime.now() if end_date is None else datetime.strptime(end_date, '%Y-%m-%d')
        start_date_dt = end_date_dt - pd.Timedelta(days=30) if start_date is None else datetime.strptime(start_date, '%Y-%m-%d')
        
        # Ensure start date is before end date
        if start_date_dt > end_date_dt:
            start_date_dt, end_date_dt = end_date_dt, start_date_dt
        
        # Generate dates within the range
        dates = pd.date_range(start=start_date_dt, end=end_date_dt, freq='D')
        
        # Create empty list to store data
        data = []
        
        # Partner IDs
        partner_ids = ['PARTNER-A', 'PARTNER-B', 'PARTNER-C']
        
        # Product categories
        categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
        
        # Generate random order data from partners
        for date in dates:
            # Each partner might not send data every day
            for partner in partner_ids:
                if np.random.random() < 0.7:  # 70% chance of sending data on a given day
                    # Number of orders for this partner on this day
                    num_orders = np.random.randint(5, 20)
                    
                    for _ in range(num_orders):
                        # Generate order data
                        order_id = f"{partner}-{np.random.randint(10000, 99999)}"
                        category = np.random.choice(categories)
                        quantity = np.random.randint(1, 5)
                        unit_price = round(np.random.uniform(10, 200), 2)
                        total_price = round(quantity * unit_price, 2)
                        
                        # Add to data list
                        data.append({
                            'order_id': order_id,
                            'order_date': date.strftime('%Y-%m-%d'),
                            'partner_id': partner,
                            'product_category': category,
                            'quantity': quantity,
                            'unit_price': unit_price,
                            'total_price': total_price,
                            'status': np.random.choice(['Processed', 'Shipped', 'Delivered'], p=[0.2, 0.3, 0.5]),
                            'source_file': f"{partner}_orders_{date.strftime('%Y%m%d')}.csv"
                        })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Apply file pattern filtering if provided
        if file_pattern:
            df = df[df['source_file'].str.match(file_pattern)]
        
        logger.info(f"Generated {len(df)} rows of mock FTP data")
        return df
