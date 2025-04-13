#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
XML Extractor module for extracting e-commerce inventory data from XML files.
"""

import logging
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import xmltodict

logger = logging.getLogger(__name__)

class XMLExtractor:
    """Extracts e-commerce inventory data from XML files."""
    
    def __init__(self, config):
        """
        Initialize the XML Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.inventory_xml
    
    def extract(self, start_date=None, end_date=None):
        """
        Extract data from XML file with optional filtering.
        
        Args:
            start_date (str, optional): Start date for filtering in YYYY-MM-DD format
            end_date (str, optional): End date for filtering in YYYY-MM-DD format
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the XML file doesn't exist
        """
        logger.info(f"Extracting data from XML file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"XML file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Parse XML file
            with open(self.source_file, 'r') as f:
                xml_data = f.read()
            
            # Convert XML to dictionary
            data_dict = xmltodict.parse(xml_data)
            
            # Determine the root structure and extract items
            if 'inventory' in data_dict:
                items = data_dict['inventory'].get('item', [])
            elif 'products' in data_dict:
                items = data_dict['products'].get('product', [])
            elif 'catalog' in data_dict:
                items = data_dict['catalog'].get('product', [])
            elif 'items' in data_dict:
                items = data_dict['items'].get('item', [])
            else:
                logger.warning("Unknown XML structure. Cannot locate items.")
                return pd.DataFrame()
            
            # Ensure items is a list
            if not isinstance(items, list):
                items = [items]
            
            # Flatten nested dictionaries
            flattened_items = []
            for item in items:
                flat_item = self._flatten_dict(item)
                flattened_items.append(flat_item)
            
            # Convert to DataFrame
            df = pd.DataFrame(flattened_items)
            
            # Convert date columns to datetime
            date_columns = [col for col in df.columns if 'date' in col.lower()]
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
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
            logger.info(f"Extracted {len(df)} items from XML file")
            if not df.empty:
                logger.info(f"Columns in XML data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from XML file: {str(e)}")
            raise
    
    def _flatten_dict(self, d, parent_key='', sep='_'):
        """
        Flatten a nested dictionary.
        
        Args:
            d (dict): Dictionary to flatten
            parent_key (str): Parent key for nested dictionaries
            sep (str): Separator for nested keys
        
        Returns:
            dict: Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # For list values, join them as comma-separated string
                if all(isinstance(x, str) for x in v):
                    items.append((new_key, ', '.join(v)))
                elif all(isinstance(x, dict) for x in v):
                    # If it's a list of dictionaries, we might want to handle differently
                    # For now, just take the first one or create a nested structure
                    if v:  # If the list is not empty
                        items.extend(self._flatten_dict(v[0], new_key, sep=sep).items())
                else:
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _create_sample_data(self):
        """Create sample e-commerce inventory data in XML format."""
        try:
            import numpy as np
            import xml.dom.minidom as minidom
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
            
            # Create root element
            root = ET.Element("inventory")
            root.set("generated", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            root.set("source", "E-commerce ETL Project")
            
            # Sample product categories and warehouses
            categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
            warehouses = ['Main Warehouse', 'East Coast Fulfillment', 'West Coast Fulfillment', 'International']
            
            # Generate 100 inventory items
            for i in range(100):
                # Create item element
                item = ET.SubElement(root, "item")
                
                # Basic item information
                ET.SubElement(item, "id").text = f"PROD-{1000 + i}"
                ET.SubElement(item, "sku").text = f"SKU{10000 + i}"
                
                category = np.random.choice(categories)
                ET.SubElement(item, "category").text = category
                
                # Generate product name based on category
                if category == 'Electronics':
                    names = ['Smartphone', 'Laptop', 'Headphones', 'Tablet', 'Smart Watch']
                    name = f"{np.random.choice(['Apple', 'Samsung', 'Sony', 'Dell'])} {np.random.choice(names)}"
                elif category == 'Clothing':
                    names = ['T-shirt', 'Jeans', 'Dress', 'Jacket', 'Socks']
                    name = f"{np.random.choice(['Nike', 'Adidas', 'Zara', 'H&M'])} {np.random.choice(names)}"
                elif category == 'Home & Kitchen':
                    names = ['Blender', 'Coffee Maker', 'Toaster', 'Cookware Set', 'Knife Set']
                    name = f"{np.random.choice(['KitchenAid', 'Cuisinart', 'Ninja', 'OXO'])} {np.random.choice(names)}"
                elif category == 'Books':
                    genres = ['Fiction', 'Biography', 'Cooking', 'Self-Help', 'History']
                    name = f"{np.random.choice(genres)} Book"
                else:  # Toys
                    names = ['Action Figure', 'Board Game', 'Puzzle', 'Stuffed Animal', 'Building Blocks']
                    name = f"{np.random.choice(['LEGO', 'Hasbro', 'Mattel', 'Fisher-Price'])} {np.random.choice(names)}"
                
                ET.SubElement(item, "name").text = name
                ET.SubElement(item, "description").text = f"This is a {name} from the {category} category."
                
                # Pricing information
                price_elem = ET.SubElement(item, "pricing")
                base_price = np.random.uniform(10, 500)
                ET.SubElement(price_elem, "base_price").text = f"{base_price:.2f}"
                ET.SubElement(price_elem, "currency").text = "USD"
                
                # Apply discount to some items
                if np.random.random() < 0.3:  # 30% chance of discount
                    discount_rate = np.random.uniform(0.05, 0.30)  # 5% to 30% discount
                    discount_amount = base_price * discount_rate
                    ET.SubElement(price_elem, "discount").text = f"{discount_amount:.2f}"
                    ET.SubElement(price_elem, "discount_percentage").text = f"{discount_rate*100:.0f}%"
                    ET.SubElement(price_elem, "sale_price").text = f"{base_price - discount_amount:.2f}"
                
                # Inventory information
                inventory_elem = ET.SubElement(item, "inventory")
                
                # Generate stock levels and locations
                total_stock = 0
                for warehouse in warehouses:
                    if np.random.random() < 0.7:  # 70% chance of having stock in this warehouse
                        location = ET.SubElement(inventory_elem, "location")
                        ET.SubElement(location, "warehouse").text = warehouse
                        stock = np.random.randint(0, 100)
                        ET.SubElement(location, "quantity").text = str(stock)
                        total_stock += stock
                
                ET.SubElement(inventory_elem, "total_stock").text = str(total_stock)
                ET.SubElement(inventory_elem, "stock_status").text = "In Stock" if total_stock > 0 else "Out of Stock"
                ET.SubElement(inventory_elem, "reorder_level").text = str(np.random.randint(5, 20))
                
                # Supplier information
                supplier_elem = ET.SubElement(item, "supplier")
                ET.SubElement(supplier_elem, "name").text = f"Supplier {np.random.randint(1, 10)}"
                ET.SubElement(supplier_elem, "lead_time_days").text = str(np.random.randint(3, 30))
                
                # Dates
                dates_elem = ET.SubElement(item, "dates")
                
                # Generate a created date within the last year
                days_ago = np.random.randint(1, 365)
                created_date = (datetime.now() - pd.Timedelta(days=days_ago)).strftime('%Y-%m-%d')
                ET.SubElement(dates_elem, "created_date").text = created_date
                
                # Last updated date is more recent than created date
                updated_days_ago = np.random.randint(0, days_ago)
                updated_date = (datetime.now() - pd.Timedelta(days=updated_days_ago)).strftime('%Y-%m-%d')
                ET.SubElement(dates_elem, "last_updated").text = updated_date
                
                # Last inventory check date is even more recent
                check_days_ago = np.random.randint(0, updated_days_ago)
                check_date = (datetime.now() - pd.Timedelta(days=check_days_ago)).strftime('%Y-%m-%d')
                ET.SubElement(dates_elem, "last_inventory_check").text = check_date
                
                # Product dimensions and shipping information
                dimensions_elem = ET.SubElement(item, "dimensions")
                ET.SubElement(dimensions_elem, "weight").text = f"{np.random.uniform(0.1, 10.0):.2f}"
                ET.SubElement(dimensions_elem, "weight_unit").text = "kg"
                ET.SubElement(dimensions_elem, "length").text = f"{np.random.uniform(5, 100):.1f}"
                ET.SubElement(dimensions_elem, "width").text = f"{np.random.uniform(5, 100):.1f}"
                ET.SubElement(dimensions_elem, "height").text = f"{np.random.uniform(5, 100):.1f}"
                ET.SubElement(dimensions_elem, "dimension_unit").text = "cm"
                
                # Additional attributes based on category
                attributes_elem = ET.SubElement(item, "attributes")
                if category == 'Electronics':
                    ET.SubElement(attributes_elem, "brand").text = name.split()[0]
                    ET.SubElement(attributes_elem, "color").text = np.random.choice(["Black", "Silver", "White", "Gold"])
                    ET.SubElement(attributes_elem, "warranty").text = f"{np.random.choice([1, 2, 3])} years"
                elif category == 'Clothing':
                    ET.SubElement(attributes_elem, "brand").text = name.split()[0]
                    ET.SubElement(attributes_elem, "size").text = np.random.choice(["XS", "S", "M", "L", "XL"])
                    ET.SubElement(attributes_elem, "color").text = np.random.choice(["Black", "White", "Blue", "Red", "Green"])
                    ET.SubElement(attributes_elem, "material").text = np.random.choice(["Cotton", "Polyester", "Wool", "Denim"])
                elif category == 'Home & Kitchen':
                    ET.SubElement(attributes_elem, "brand").text = name.split()[0]
                    ET.SubElement(attributes_elem, "color").text = np.random.choice(["Black", "Silver", "White", "Red"])
                    ET.SubElement(attributes_elem, "material").text = np.random.choice(["Stainless Steel", "Plastic", "Glass", "Ceramic"])
                elif category == 'Books':
                    ET.SubElement(attributes_elem, "author").text = f"Author {np.random.randint(1, 100)}"
                    ET.SubElement(attributes_elem, "publisher").text = f"Publisher {np.random.randint(1, 10)}"
                    ET.SubElement(attributes_elem, "language").text = "English"
                    ET.SubElement(attributes_elem, "pages").text = str(np.random.randint(100, 500))
                else:  # Toys
                    ET.SubElement(attributes_elem, "brand").text = name.split()[0]
                    ET.SubElement(attributes_elem, "age_range").text = np.random.choice(["0-2", "3-5", "6-9", "10-13", "14+"])
                    ET.SubElement(attributes_elem, "material").text = np.random.choice(["Plastic", "Wood", "Fabric", "Metal"])
            
            # Convert to string and pretty-print
            rough_string = ET.tostring(root, 'utf-8')
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")
            
            # Write to file
            with open(self.source_file, 'w') as f:
                f.write(pretty_xml)
            
            logger.info(f"Created sample inventory XML file: {self.source_file}")
            
        except Exception as e:
            logger.error(f"Error creating sample XML file: {str(e)}")
            # Create an empty XML file as a placeholder
            with open(self.source_file, 'w') as f:
                f.write("<inventory><item><id>SAMPLE</id><name>Sample Product</name></item></inventory>")
