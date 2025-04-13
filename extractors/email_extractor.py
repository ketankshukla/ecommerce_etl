#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Email Extractor module for extracting e-commerce data from email messages.
"""

import logging
import pandas as pd
import os
import re
import email
import imaplib
from datetime import datetime
import tempfile
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

class EmailExtractor:
    """Extracts e-commerce data from email messages."""
    
    def __init__(self, config):
        """
        Initialize the Email Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.email_server = config.email_server
        self.email_user = config.email_user
        self.email_password = config.email_password
        self.output_dir = os.path.join(config.data_dir, 'email')
    
    def extract(self, start_date=None, end_date=None, subject_pattern=None):
        """
        Extract data from emails with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
            subject_pattern (str, optional): Regex pattern to match email subjects
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            Exception: If there's an error connecting to the email server
        """
        logger.info(f"Extracting data from email server: {self.email_server}")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        try:
            # Connect to IMAP server
            logger.info(f"Connecting to email server: {self.email_server}")
            
            # Mock the email extraction for demonstration if no real server
            if self.email_server == 'imap.example.com':
                logger.warning("Using mock email data since no real server is configured")
                return self._mock_email_extraction(start_date, end_date, subject_pattern)
            
            # Real IMAP connection
            mail = imaplib.IMAP4_SSL(self.email_server)
            mail.login(self.email_user, self.email_password)
            mail.select('inbox')
            
            # Prepare search criteria
            search_criteria = []
            
            # Add date criteria
            if start_date:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                search_criteria.append(f'SINCE {start_date_obj.strftime("%d-%b-%Y")}')
            
            if end_date:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                search_criteria.append(f'BEFORE {end_date_obj.strftime("%d-%b-%Y")}')
            
            # Add subject criteria if provided
            if subject_pattern:
                search_criteria.append(f'SUBJECT "{subject_pattern}"')
            
            # Execute search
            if search_criteria:
                search_query = ' '.join(search_criteria)
                logger.info(f"Searching emails with criteria: {search_query}")
                result, data = mail.search(None, search_query)
            else:
                # If no criteria, search for all emails (limit to recent)
                logger.info("Searching for recent emails")
                result, data = mail.search(None, 'RECENT')
            
            # Get email IDs
            email_ids = data[0].split()
            logger.info(f"Found {len(email_ids)} matching emails")
            
            # Extract data from emails
            all_data = []
            for email_id in email_ids:
                result, data = mail.fetch(email_id, '(RFC822)')
                raw_email = data[0][1]
                
                # Parse email
                msg = email.message_from_bytes(raw_email)
                subject = msg['subject']
                sender = msg['from']
                date = parsedate_to_datetime(msg['date'])
                
                # Apply subject pattern filtering if not applied in search
                if subject_pattern and not re.search(subject_pattern, subject, re.IGNORECASE):
                    continue
                
                # Process email content
                if msg.is_multipart():
                    # Handle multipart emails
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        content_disposition = str(part.get("Content-Disposition"))
                        
                        # Extract attachments
                        if "attachment" in content_disposition:
                            filename = part.get_filename()
                            if filename:
                                # Save attachment
                                local_path = os.path.join(self.output_dir, filename)
                                with open(local_path, 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                                
                                # Process attachment based on file type
                                if filename.endswith('.csv'):
                                    df = pd.read_csv(local_path)
                                    df['source_email'] = subject
                                    df['email_date'] = date
                                    all_data.append(df)
                                elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                                    df = pd.read_excel(local_path)
                                    df['source_email'] = subject
                                    df['email_date'] = date
                                    all_data.append(df)
                        
                        # Extract text content
                        elif content_type == "text/plain":
                            body = part.get_payload(decode=True).decode()
                            
                            # Try to extract structured data from text
                            extracted = self._extract_data_from_text(body, subject, date)
                            if extracted is not None:
                                all_data.append(extracted)
                else:
                    # Handle non-multipart emails (text only)
                    body = msg.get_payload(decode=True).decode()
                    
                    # Try to extract structured data from text
                    extracted = self._extract_data_from_text(body, subject, date)
                    if extracted is not None:
                        all_data.append(extracted)
            
            # Close connection
            mail.close()
            mail.logout()
            
            # Combine all data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"Extracted {len(combined_df)} rows from email server")
                return combined_df
            else:
                logger.warning("No structured data found in emails")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error extracting data from email server: {str(e)}")
            logger.info("Falling back to mock data...")
            return self._mock_email_extraction(start_date, end_date, subject_pattern)
    
    def _extract_data_from_text(self, text, subject, date):
        """
        Extract structured data from email text content.
        
        Args:
            text (str): Email text content
            subject (str): Email subject
            date (datetime): Email date
        
        Returns:
            pandas.DataFrame: Extracted data or None if no data found
        """
        # Look for common e-commerce patterns in the text
        
        # Check for order confirmation
        if "order confirmation" in subject.lower() or "your order" in subject.lower():
            # Try to extract order information
            order_id_match = re.search(r'order\s*(?:number|#|id)?\s*[:#]?\s*(\w+[-\w]*)', text, re.IGNORECASE)
            order_id = order_id_match.group(1) if order_id_match else None
            
            # Look for product information in the email
            products = []
            
            # Pattern for product listings (name, quantity, price)
            # This pattern will need to be adjusted based on actual email formats
            product_pattern = r'(\d+)\s*x\s*([\w\s\-\&]+?)\s*\$?(\d+\.\d{2})'
            for match in re.finditer(product_pattern, text):
                quantity = int(match.group(1))
                product_name = match.group(2).strip()
                price = float(match.group(3))
                
                products.append({
                    'order_id': order_id,
                    'product_name': product_name,
                    'quantity': quantity,
                    'price': price,
                    'total': quantity * price,
                    'order_date': date,
                    'source_email': subject
                })
            
            if products:
                return pd.DataFrame(products)
        
        # Check for shipping confirmation
        elif "shipping confirmation" in subject.lower() or "shipped" in subject.lower():
            # Try to extract shipping information
            order_id_match = re.search(r'order\s*(?:number|#|id)?\s*[:#]?\s*(\w+[-\w]*)', text, re.IGNORECASE)
            order_id = order_id_match.group(1) if order_id_match else None
            
            tracking_match = re.search(r'tracking\s*(?:number|#|id)?\s*[:#]?\s*(\w+[-\w]*)', text, re.IGNORECASE)
            tracking_number = tracking_match.group(1) if tracking_match else None
            
            carrier_match = re.search(r'(?:shipped|carrier|shipping)\s*(?:via|with|using)?\s*[:#]?\s*([\w\s]+)', text, re.IGNORECASE)
            carrier = carrier_match.group(1).strip() if carrier_match else None
            
            if order_id or tracking_number:
                return pd.DataFrame([{
                    'order_id': order_id,
                    'tracking_number': tracking_number,
                    'carrier': carrier,
                    'ship_date': date,
                    'source_email': subject,
                    'status': 'Shipped'
                }])
        
        # No structured data found
        return None
    
    def _mock_email_extraction(self, start_date=None, end_date=None, subject_pattern=None):
        """
        Create mock email data for demonstration purposes.
        
        Args:
            start_date (str, optional): Start date for filtering
            end_date (str, optional): End date for filtering
            subject_pattern (str, optional): Regex pattern to match email subjects
        
        Returns:
            pandas.DataFrame: Mock data
        """
        import numpy as np
        
        logger.info("Generating mock email data for demonstration")
        
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
        
        # Product categories
        categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
        
        # Email types
        email_types = ['Order Confirmation', 'Shipping Confirmation', 'Delivery Confirmation', 'Return Confirmation']
        
        # Generate random email data
        for date in dates:
            # Number of emails on this day
            num_emails = np.random.randint(1, 5)
            
            for _ in range(num_emails):
                # Generate random order ID
                order_id = f"ORD-{np.random.randint(100000, 999999)}"
                
                # Select random email type
                email_type = np.random.choice(email_types, p=[0.4, 0.3, 0.2, 0.1])
                
                if email_type == 'Order Confirmation':
                    subject = f"Your order {order_id} has been confirmed"
                    
                    # Generate order items
                    num_items = np.random.randint(1, 4)
                    for i in range(num_items):
                        category = np.random.choice(categories)
                        product_name = f"Sample {category} Product {i+1}"
                        quantity = np.random.randint(1, 3)
                        price = round(np.random.uniform(10, 200), 2)
                        
                        data.append({
                            'order_id': order_id,
                            'product_name': product_name,
                            'product_category': category,
                            'quantity': quantity,
                            'price': price,
                            'total': quantity * price,
                            'order_date': pd.Timestamp(date).to_pydatetime(),
                            'source_email': subject,
                            'email_type': email_type,
                            'status': 'Confirmed'
                        })
                
                elif email_type == 'Shipping Confirmation':
                    subject = f"Your order {order_id} has shipped"
                    tracking_number = f"TRK-{np.random.randint(1000000, 9999999)}"
                    carrier = np.random.choice(['USPS', 'FedEx', 'UPS', 'DHL'])
                    
                    data.append({
                        'order_id': order_id,
                        'tracking_number': tracking_number,
                        'carrier': carrier,
                        'ship_date': pd.Timestamp(date).to_pydatetime(),
                        'source_email': subject,
                        'email_type': email_type,
                        'status': 'Shipped'
                    })
                
                elif email_type == 'Delivery Confirmation':
                    subject = f"Your order {order_id} has been delivered"
                    delivery_date = date
                    
                    data.append({
                        'order_id': order_id,
                        'delivery_date': pd.Timestamp(delivery_date).to_pydatetime(),
                        'source_email': subject,
                        'email_type': email_type,
                        'status': 'Delivered'
                    })
                
                else:  # Return Confirmation
                    subject = f"Return for order {order_id} confirmed"
                    return_reason = np.random.choice(['Wrong size', 'Damaged', 'Not as described', 'Changed mind'])
                    
                    data.append({
                        'order_id': order_id,
                        'return_reason': return_reason,
                        'return_date': pd.Timestamp(date).to_pydatetime(),
                        'source_email': subject,
                        'email_type': email_type,
                        'status': 'Returned'
                    })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Apply subject pattern filtering if provided
        if subject_pattern:
            df = df[df['source_email'].str.contains(subject_pattern, case=False)]
        
        logger.info(f"Generated {len(df)} rows of mock email data")
        return df
