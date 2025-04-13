#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File Loader module for exporting processed e-commerce data to various file formats.
"""

import logging
import pandas as pd
import os
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class FileLoader:
    """Exports processed e-commerce data to various file formats."""
    
    def __init__(self, config):
        """
        Initialize the File Loader.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.output_dir = config.processed_data_dir
    
    def export(self, data, formats=None, prefix=None):
        """
        Export data to various file formats.
        
        Args:
            data (pandas.DataFrame or dict): Data to export
            formats (list, optional): List of formats to export to ('csv', 'json', 'excel', 'parquet')
                                     Default is ['csv']
            prefix (str, optional): Prefix for output filenames
        
        Returns:
            dict: Dictionary mapping format to output file paths
        """
        if data is None:
            logger.warning("No data to export")
            return {}
        
        # Set default formats if not provided
        if formats is None:
            formats = ['csv']
        
        # Check if data is a dictionary of DataFrames
        if isinstance(data, dict):
            logger.info(f"Exporting {len(data)} dataframes to {', '.join(formats)}")
            
            # Initialize dictionary for output files
            output_files = {fmt: {} for fmt in formats}
            
            # Export each DataFrame
            for key, df in data.items():
                if df is not None and not df.empty:
                    # Generate prefix from key if not provided
                    df_prefix = prefix or key.lower().replace(' ', '_')
                    
                    # Export DataFrame
                    files = self._export_dataframe(df, formats, f"{df_prefix}_{key}")
                    
                    # Add to output files
                    for fmt, file_path in files.items():
                        output_files[fmt][key] = file_path
                else:
                    logger.warning(f"Empty dataframe for {key}, skipping")
            
            return output_files
        else:
            # Data is a single DataFrame
            if prefix is None:
                prefix = "exported_data"
            
            return self._export_dataframe(data, formats, prefix)
    
    def _export_dataframe(self, df, formats, prefix):
        """
        Export a single DataFrame to various file formats.
        
        Args:
            df (pandas.DataFrame): DataFrame to export
            formats (list): List of formats to export to
            prefix (str): Prefix for output filenames
        
        Returns:
            dict: Dictionary mapping format to output file path
        """
        if df is None or df.empty:
            logger.warning(f"No data to export for {prefix}")
            return {}
        
        logger.info(f"Exporting {len(df)} rows with prefix {prefix}")
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Get current timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize dictionary for output files
        output_files = {}
        
        try:
            # Export to each format
            for fmt in formats:
                if fmt.lower() == 'csv':
                    # Export to CSV
                    file_path = os.path.join(self.output_dir, f"{prefix}_{timestamp}.csv")
                    df.to_csv(file_path, index=False)
                    output_files['csv'] = file_path
                    logger.info(f"Exported to CSV: {file_path}")
                
                elif fmt.lower() == 'json':
                    # Export to JSON
                    file_path = os.path.join(self.output_dir, f"{prefix}_{timestamp}.json")
                    
                    # Convert datetime columns to string to handle JSON serialization
                    df_json = df.copy()
                    for col in df_json.select_dtypes(include=['datetime64']).columns:
                        df_json[col] = df_json[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Export to JSON
                    if len(df_json) > 1000:
                        # Use lines=True for large DataFrames to avoid memory issues
                        df_json.to_json(file_path, orient='records', lines=True)
                    else:
                        # Use prettier formatting for smaller DataFrames
                        with open(file_path, 'w') as f:
                            json.dump(df_json.to_dict(orient='records'), f, indent=2)
                    
                    output_files['json'] = file_path
                    logger.info(f"Exported to JSON: {file_path}")
                
                elif fmt.lower() == 'excel':
                    # Export to Excel
                    file_path = os.path.join(self.output_dir, f"{prefix}_{timestamp}.xlsx")
                    
                    # Check if DataFrame is too large for Excel
                    if len(df.columns) > 16384 or len(df) > 1048576:
                        logger.warning(f"DataFrame too large for Excel format: {len(df)} rows x {len(df.columns)} columns")
                        logger.warning("Excel limits: 1,048,576 rows x 16,384 columns")
                        continue
                    
                    # Export to Excel
                    df.to_excel(file_path, index=False, sheet_name=prefix[:31])  # Excel sheet name limited to 31 chars
                    output_files['excel'] = file_path
                    logger.info(f"Exported to Excel: {file_path}")
                
                elif fmt.lower() == 'parquet':
                    # Export to Parquet
                    try:
                        import pyarrow
                        file_path = os.path.join(self.output_dir, f"{prefix}_{timestamp}.parquet")
                        df.to_parquet(file_path, index=False)
                        output_files['parquet'] = file_path
                        logger.info(f"Exported to Parquet: {file_path}")
                    except ImportError:
                        logger.warning("Parquet export requires pyarrow. Install with: pip install pyarrow")
                
                elif fmt.lower() == 'html':
                    # Export to HTML
                    file_path = os.path.join(self.output_dir, f"{prefix}_{timestamp}.html")
                    
                    # Generate basic HTML with DataFrame
                    html_content = f"""<!DOCTYPE html>
                    <html>
                    <head>
                        <title>{prefix} - E-commerce ETL Export</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 20px; }}
                            h1 {{ color: #2c3e50; }}
                            table {{ border-collapse: collapse; width: 100%; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                        </style>
                    </head>
                    <body>
                        <h1>{prefix}</h1>
                        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        {df.to_html(index=False)}
                    </body>
                    </html>
                    """
                    
                    # Write HTML file
                    with open(file_path, 'w') as f:
                        f.write(html_content)
                    
                    output_files['html'] = file_path
                    logger.info(f"Exported to HTML: {file_path}")
                
                else:
                    logger.warning(f"Unsupported export format: {fmt}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Error exporting data: {str(e)}")
            return output_files
    
    def export_to_dir(self, data, output_dir=None, formats=None, prefix=None):
        """
        Export data to a specific directory.
        
        Args:
            data (pandas.DataFrame or dict): Data to export
            output_dir (str, optional): Output directory
            formats (list, optional): List of formats to export to
            prefix (str, optional): Prefix for output filenames
        
        Returns:
            dict: Dictionary mapping format to output file paths
        """
        # Save original output directory
        original_dir = self.output_dir
        
        # Set new output directory if provided
        if output_dir:
            self.output_dir = output_dir
            # Ensure directory exists
            os.makedirs(output_dir, exist_ok=True)
        
        # Export data
        result = self.export(data, formats, prefix)
        
        # Restore original output directory
        self.output_dir = original_dir
        
        return result
