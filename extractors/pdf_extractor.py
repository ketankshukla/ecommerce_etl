#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PDF Extractor module for extracting e-commerce invoice data from PDF files.
"""

import logging
import pandas as pd
import os
import re
from datetime import datetime
import tempfile

logger = logging.getLogger(__name__)

class PDFExtractor:
    """Extracts e-commerce invoice data from PDF files."""
    
    def __init__(self, config):
        """
        Initialize the PDF Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.invoices_pdf
    
    def extract(self, start_date=None, end_date=None):
        """
        Extract data from PDF invoices with optional date filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the PDF file doesn't exist
        """
        logger.info(f"Extracting data from PDF file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"PDF file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Import required packages here to avoid dependencies when not using PDF
            import PyPDF2
            import tabula
            
            # Extract tables from PDF
            logger.info("Extracting tables from PDF")
            tables = tabula.read_pdf(self.source_file, pages='all', multiple_tables=True)
            
            # If no tables found, try to extract text and parse
            if not tables or all(df.empty for df in tables):
                logger.info("No tables found in PDF, attempting text extraction")
                
                # Extract text from PDF
                with open(self.source_file, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    text = ""
                    for page_num in range(len(pdf_reader.pages)):
                        text += pdf_reader.pages[page_num].extract_text()
                
                # Parse invoice data from text using regular expressions
                invoice_data = self._parse_invoice_text(text)
                
                # Convert to DataFrame
                if invoice_data:
                    df = pd.DataFrame(invoice_data)
                else:
                    logger.warning("Unable to extract data from PDF text")
                    return pd.DataFrame()
            else:
                # Combine all tables into one DataFrame
                df = pd.concat(tables, ignore_index=True)
                
                # Rename columns if needed
                if not df.empty:
                    # Try to identify column types based on content
                    column_mapping = {}
                    for col in df.columns:
                        col_str = str(col).lower()
                        if any(term in col_str for term in ['date', 'time']):
                            column_mapping[col] = 'invoice_date'
                        elif any(term in col_str for term in ['id', 'number', 'no.', '#']):
                            column_mapping[col] = 'invoice_id'
                        elif any(term in col_str for term in ['item', 'product', 'description']):
                            column_mapping[col] = 'product_description'
                        elif any(term in col_str for term in ['qty', 'quantity']):
                            column_mapping[col] = 'quantity'
                        elif any(term in col_str for term in ['unit', 'price']):
                            column_mapping[col] = 'unit_price'
                        elif any(term in col_str for term in ['total', 'amount']):
                            column_mapping[col] = 'total_price'
                    
                    # Rename columns if we found mappings
                    if column_mapping:
                        df = df.rename(columns=column_mapping)
            
            # Convert date columns to datetime
            date_columns = [col for col in df.columns if 'date' in str(col).lower()]
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Apply date filtering if provided
            if start_date and date_columns:
                start_date = pd.to_datetime(start_date)
                for col in date_columns:
                    df = df[df[col] >= start_date]
            
            if end_date and date_columns:
                end_date = pd.to_datetime(end_date)
                for col in date_columns:
                    df = df[df[col] <= end_date]
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from PDF")
            if not df.empty:
                logger.info(f"Columns in PDF data: {', '.join(df.columns)}")
            
            return df
            
        except ImportError as e:
            logger.error(f"Missing PDF dependencies: {str(e)}. Install with: pip install PyPDF2 tabula-py")
            raise
        except Exception as e:
            logger.error(f"Error extracting data from PDF file: {str(e)}")
            raise
    
    def _parse_invoice_text(self, text):
        """
        Parse invoice data from extracted text.
        
        Args:
            text (str): Extracted text from PDF
        
        Returns:
            list: List of dictionaries containing invoice data
        """
        # List to store extracted invoice items
        invoice_items = []
        
        # Try to extract invoice number
        invoice_match = re.search(r'Invoice\s*#?\s*:?\s*(\w+[-/]?\w+)', text, re.IGNORECASE)
        invoice_id = invoice_match.group(1) if invoice_match else None
        
        # Try to extract date
        date_match = re.search(r'Date\s*:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{2,4}[-/]\d{1,2}[-/]\d{1,2})', text, re.IGNORECASE)
        invoice_date = date_match.group(1) if date_match else None
        
        # Try to convert date to standard format
        if invoice_date:
            try:
                invoice_date = pd.to_datetime(invoice_date).strftime('%Y-%m-%d')
            except:
                pass
        
        # Try to extract customer information
        customer_match = re.search(r'(?:Customer|Client|Bill To)\s*:?\s*([A-Za-z0-9\s]+)', text, re.IGNORECASE)
        customer = customer_match.group(1).strip() if customer_match else None
        
        # Look for product entries - patterns vary widely between invoice formats
        # This is a simple pattern that may need to be adjusted based on actual invoice formats
        product_pattern = r'(\d+)\s+([A-Za-z0-9\s\-]+?)\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)'
        product_matches = re.finditer(product_pattern, text)
        
        for match in product_matches:
            try:
                quantity = int(match.group(1))
                product = match.group(2).strip()
                unit_price = float(match.group(3))
                total_price = float(match.group(4))
                
                invoice_items.append({
                    'invoice_id': invoice_id,
                    'invoice_date': invoice_date,
                    'customer': customer,
                    'product_description': product,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'total_price': total_price
                })
            except (ValueError, IndexError):
                continue
        
        return invoice_items
    
    def _create_sample_data(self):
        """Create a sample PDF invoice for demonstration purposes."""
        try:
            # Import required packages
            import PyPDF2
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
            from reportlab.lib import colors
            from reportlab.platypus import Table, TableStyle
            import numpy as np
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
            
            # Create a temporary PDF file
            temp_pdf = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
            pdf_path = temp_pdf.name
            temp_pdf.close()
            
            # Get current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Generate random invoice data
            invoice_id = f"INV-{np.random.randint(10000, 99999)}"
            customer_id = f"CUST-{np.random.randint(1000, 9999)}"
            customer_name = f"Customer {customer_id}"
            
            # Product data
            products = [
                ("Smartphone", np.random.randint(1, 3), round(np.random.uniform(500, 1000), 2)),
                ("Laptop", np.random.randint(1, 2), round(np.random.uniform(800, 2000), 2)),
                ("Headphones", np.random.randint(1, 4), round(np.random.uniform(50, 300), 2)),
                ("Smart Watch", np.random.randint(1, 3), round(np.random.uniform(100, 500), 2)),
                ("External Hard Drive", np.random.randint(1, 2), round(np.random.uniform(80, 200), 2))
            ]
            
            # Calculate totals
            subtotal = sum(qty * price for _, qty, price in products)
            tax_rate = 0.08
            tax = round(subtotal * tax_rate, 2)
            shipping = round(np.random.uniform(10, 30), 2)
            total = subtotal + tax + shipping
            
            # Create PDF with ReportLab
            c = canvas.Canvas(pdf_path, pagesize=letter)
            width, height = letter
            
            # Title
            c.setFont("Helvetica-Bold", 24)
            c.drawString(50, height - 50, "E-Commerce Invoice")
            
            # Invoice details
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, height - 80, "Invoice Details:")
            c.setFont("Helvetica", 12)
            c.drawString(50, height - 100, f"Invoice #: {invoice_id}")
            c.drawString(50, height - 120, f"Date: {current_date}")
            c.drawString(50, height - 140, f"Customer ID: {customer_id}")
            c.drawString(50, height - 160, f"Customer Name: {customer_name}")
            
            # Create table data
            table_data = [["Item", "Quantity", "Unit Price", "Total"]]
            for product, qty, price in products:
                table_data.append([product, str(qty), f"${price:.2f}", f"${qty * price:.2f}"])
            
            # Create table
            table = Table(table_data, colWidths=[200, 100, 100, 100])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            # Draw table
            table.wrapOn(c, width, height)
            table.drawOn(c, 50, height - 350)
            
            # Summary
            c.setFont("Helvetica-Bold", 12)
            c.drawString(350, height - 380, f"Subtotal: ${subtotal:.2f}")
            c.drawString(350, height - 400, f"Tax (8%): ${tax:.2f}")
            c.drawString(350, height - 420, f"Shipping: ${shipping:.2f}")
            c.drawString(350, height - 450, f"Total: ${total:.2f}")
            
            # Footer
            c.setFont("Helvetica", 10)
            c.drawString(50, 50, "Thank you for your purchase!")
            c.drawString(50, 30, f"For questions, contact: support@example.com")
            
            # Save the PDF
            c.save()
            
            # Copy the temporary PDF to the target location
            import shutil
            shutil.copy(pdf_path, self.source_file)
            
            # Clean up the temporary file
            os.unlink(pdf_path)
            
            logger.info(f"Created sample invoice PDF: {self.source_file}")
            
        except ImportError:
            logger.error("Failed to create sample PDF due to missing dependencies. Install with: pip install reportlab PyPDF2")
            # Create an empty file as a placeholder
            with open(self.source_file, 'w') as f:
                f.write("Sample PDF placeholder")
        except Exception as e:
            logger.error(f"Error creating sample PDF: {str(e)}")
