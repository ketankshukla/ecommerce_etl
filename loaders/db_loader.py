#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Database Loader module for loading processed e-commerce data into databases.
"""

import logging
import pandas as pd
import os
import sqlite3
from datetime import datetime
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

class DBLoader:
    """Loads processed e-commerce data into databases."""
    
    def __init__(self, config):
        """
        Initialize the Database Loader.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.db_type = config.db_type
        self.db_connection_string = config.db_connection_string
    
    def load(self, data, table_name=None, if_exists='append'):
        """
        Load data into database.
        
        Args:
            data (pandas.DataFrame or dict): Data to load
            table_name (str, optional): Name of the table to load data into
            if_exists (str): Action to take if table exists ('fail', 'replace', 'append')
                             Default is 'append'
        
        Returns:
            bool: True if successful, False otherwise
        """
        if data is None:
            logger.warning("No data to load")
            return False
        
        # Check if data is a dictionary of DataFrames
        if isinstance(data, dict):
            logger.info(f"Loading {len(data)} dataframes into database")
            success = True
            
            # Load each DataFrame
            for key, df in data.items():
                if df is not None and not df.empty:
                    # Generate table name from key if not provided
                    table = table_name or f"{key.lower().replace(' ', '_')}"
                    
                    # Load DataFrame
                    if not self._load_dataframe(df, table, if_exists):
                        success = False
                else:
                    logger.warning(f"Empty dataframe for {key}, skipping")
            
            return success
        else:
            # Data is a single DataFrame
            if table_name is None:
                logger.warning("Table name is required for loading a single dataframe")
                return False
            
            return self._load_dataframe(data, table_name, if_exists)
    
    def _load_dataframe(self, df, table_name, if_exists='append'):
        """
        Load a single DataFrame into database.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            table_name (str): Name of the table to load data into
            if_exists (str): Action to take if table exists ('fail', 'replace', 'append')
        
        Returns:
            bool: True if successful, False otherwise
        """
        if df is None or df.empty:
            logger.warning(f"No data to load into table {table_name}")
            return False
        
        logger.info(f"Loading {len(df)} rows into table {table_name}")
        
        try:
            # Prepare data - convert date columns to string to avoid SQLite issues
            df_copy = df.copy()
            
            # Identify date columns
            date_columns = df_copy.select_dtypes(include=['datetime64']).columns
            
            # Convert date columns to strings
            for col in date_columns:
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Load data based on database type
            if self.db_type == 'sqlite':
                self._load_to_sqlite(df_copy, table_name, if_exists)
            else:
                self._load_to_sqlalchemy(df_copy, table_name, if_exists)
            
            logger.info(f"Successfully loaded data into table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data into table {table_name}: {str(e)}")
            return False
    
    def _load_to_sqlite(self, df, table_name, if_exists='append'):
        """
        Load data to SQLite database.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            table_name (str): Name of the table to load data into
            if_exists (str): Action to take if table exists ('fail', 'replace', 'append')
        """
        # Extract database path from connection string
        if 'sqlite:///' in self.db_connection_string:
            db_path = self.db_connection_string.replace('sqlite:///', '')
        else:
            db_path = self.config.db_path
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        
        # Load data
        df.to_sql(table_name, conn, index=False, if_exists=if_exists)
        
        # Create index on common columns
        cursor = conn.cursor()
        
        # Add index on id columns
        for col in df.columns:
            if col.endswith('_id') or col == 'id':
                index_name = f"idx_{table_name}_{col}"
                try:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col})")
                except Exception as e:
                    logger.warning(f"Could not create index {index_name}: {str(e)}")
        
        # Add index on date columns
        for col in df.columns:
            if 'date' in col.lower():
                index_name = f"idx_{table_name}_{col}"
                try:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col})")
                except Exception as e:
                    logger.warning(f"Could not create index {index_name}: {str(e)}")
        
        # Commit and close connection
        conn.commit()
        conn.close()
    
    def _load_to_sqlalchemy(self, df, table_name, if_exists='append'):
        """
        Load data using SQLAlchemy (for MySQL, PostgreSQL, etc.).
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            table_name (str): Name of the table to load data into
            if_exists (str): Action to take if table exists ('fail', 'replace', 'append')
        """
        # Create SQLAlchemy engine
        engine = create_engine(self.db_connection_string)
        
        # Load data
        df.to_sql(table_name, engine, index=False, if_exists=if_exists)
        
        # Close connection
        engine.dispose()
    
    def execute_query(self, query, params=None):
        """
        Execute SQL query on the database.
        
        Args:
            query (str): SQL query to execute
            params (tuple or dict, optional): Parameters for the query
        
        Returns:
            pandas.DataFrame: Query results as DataFrame
        """
        logger.info(f"Executing query: {query}")
        
        try:
            # Execute query based on database type
            if self.db_type == 'sqlite':
                # Extract database path from connection string
                if 'sqlite:///' in self.db_connection_string:
                    db_path = self.db_connection_string.replace('sqlite:///', '')
                else:
                    db_path = self.config.db_path
                
                # Connect to SQLite database
                conn = sqlite3.connect(db_path)
                
                # Execute query
                if params:
                    df = pd.read_sql_query(query, conn, params=params)
                else:
                    df = pd.read_sql_query(query, conn)
                
                # Close connection
                conn.close()
            else:
                # Create SQLAlchemy engine
                engine = create_engine(self.db_connection_string)
                
                # Execute query
                if params:
                    df = pd.read_sql_query(query, engine, params=params)
                else:
                    df = pd.read_sql_query(query, engine)
                
                # Close connection
                engine.dispose()
            
            logger.info(f"Query returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()
    
    def get_table_names(self):
        """
        Get list of tables in the database.
        
        Returns:
            list: List of table names
        """
        logger.info("Getting table names from database")
        
        try:
            # Get table names based on database type
            if self.db_type == 'sqlite':
                # Extract database path from connection string
                if 'sqlite:///' in self.db_connection_string:
                    db_path = self.db_connection_string.replace('sqlite:///', '')
                else:
                    db_path = self.config.db_path
                
                # Check if database file exists
                if not os.path.exists(db_path):
                    logger.warning(f"Database file does not exist: {db_path}")
                    return []
                
                # Connect to SQLite database
                conn = sqlite3.connect(db_path)
                
                # Get table names
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [table[0] for table in cursor.fetchall()]
                
                # Close connection
                conn.close()
            else:
                # Create SQLAlchemy engine
                engine = create_engine(self.db_connection_string)
                
                # Get table names using SQLAlchemy
                from sqlalchemy import inspect
                inspector = inspect(engine)
                tables = inspector.get_table_names()
                
                # Close connection
                engine.dispose()
            
            logger.info(f"Found {len(tables)} tables in database")
            return tables
            
        except Exception as e:
            logger.error(f"Error getting table names: {str(e)}")
            return []
