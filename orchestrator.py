#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Orchestrator module for the E-commerce Sales ETL pipeline.
Handles task scheduling and execution flow.
"""

import logging
import pandas as pd
from datetime import datetime

# Import extractors
from extractors.csv_extractor import CSVExtractor
from extractors.json_extractor import JSONExtractor
from extractors.excel_extractor import ExcelExtractor
from extractors.pdf_extractor import PDFExtractor
from extractors.db_extractor import DBExtractor
from extractors.xml_extractor import XMLExtractor
from extractors.ftp_extractor import FTPExtractor
from extractors.email_extractor import EmailExtractor
from extractors.api_extractor import APIExtractor

# Import transformers
from transformers.sales_transformer import SalesTransformer
from transformers.product_transformer import ProductTransformer
from transformers.customer_transformer import CustomerTransformer
from transformers.metrics_calculator import MetricsCalculator

# Import validators
from validators.data_validator import DataValidator

# Import loaders
from loaders.db_loader import DBLoader
from loaders.file_loader import FileLoader
from loaders.report_generator import ReportGenerator

logger = logging.getLogger(__name__)

class Task:
    """Represents a task in the ETL pipeline."""
    
    def __init__(self, name, func, dependencies=None):
        """
        Initialize a Task object.
        
        Args:
            name (str): Task name
            func (callable): Function to execute for this task
            dependencies (list): List of task names this task depends on
        """
        self.name = name
        self.func = func
        self.dependencies = dependencies or []
        self.completed = False
        self.result = None
        
    def execute(self, *args, **kwargs):
        """Execute the task function."""
        logger.info(f"Executing task: {self.name}")
        start_time = datetime.now()
        
        try:
            self.result = self.func(*args, **kwargs)
            self.completed = True
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Task {self.name} completed in {execution_time:.2f} seconds")
            return self.result
        except Exception as e:
            logger.error(f"Task {self.name} failed: {str(e)}")
            raise

class SimpleScheduler:
    """Simple task scheduler for the ETL pipeline."""
    
    def __init__(self):
        """Initialize the scheduler with an empty task dictionary."""
        self.tasks = {}
        
    def add_task(self, task):
        """Add a task to the scheduler."""
        self.tasks[task.name] = task
        
    def run(self, entry_point, *args, **kwargs):
        """
        Run tasks starting from the entry point.
        
        Args:
            entry_point (str): Name of the entry point task
            *args, **kwargs: Arguments to pass to tasks
        
        Returns:
            The result of the entry point task
        """
        if entry_point not in self.tasks:
            raise ValueError(f"Task {entry_point} not found")
        
        task = self.tasks[entry_point]
        
        # Check if dependencies are completed
        for dep_name in task.dependencies:
            if dep_name not in self.tasks:
                raise ValueError(f"Dependency {dep_name} not found")
            
            dep_task = self.tasks[dep_name]
            if not dep_task.completed:
                self.run(dep_name, *args, **kwargs)
        
        # Execute the task
        return task.execute(*args, **kwargs)

class Orchestrator:
    """Orchestrates the ETL pipeline execution."""
    
    def __init__(self, config):
        """
        Initialize the orchestrator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.scheduler = SimpleScheduler()
        
        # Initialize components
        # Extractors
        self.csv_extractor = CSVExtractor(config)
        self.json_extractor = JSONExtractor(config)
        self.excel_extractor = ExcelExtractor(config)
        self.pdf_extractor = PDFExtractor(config)
        self.db_extractor = DBExtractor(config)
        self.xml_extractor = XMLExtractor(config)
        self.ftp_extractor = FTPExtractor(config)
        self.email_extractor = EmailExtractor(config)
        self.api_extractor = APIExtractor(config)
        
        # Transformers
        self.sales_transformer = SalesTransformer(config)
        self.product_transformer = ProductTransformer(config)
        self.customer_transformer = CustomerTransformer(config)
        self.metrics_calculator = MetricsCalculator(config)
        
        # Validators
        self.validator = DataValidator(config)
        
        # Loaders
        self.db_loader = DBLoader(config)
        self.file_loader = FileLoader(config)
        self.report_generator = ReportGenerator(config)
        
        # Set up tasks
        self._setup_tasks()
        
    def _setup_tasks(self):
        """Set up tasks for the ETL pipeline."""
        # Extract tasks - define wrapper functions to handle parameters correctly
        self.scheduler.add_task(Task("extract_csv", 
            lambda *args, **kwargs: self.csv_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'product_category', 'customer_segment'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_json", 
            lambda *args, **kwargs: self.json_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'product_category'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_excel", 
            lambda *args, **kwargs: self.excel_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'sheet_name'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_pdf", 
            lambda *args, **kwargs: self.pdf_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_db", 
            lambda *args, **kwargs: self.db_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'query'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_xml", 
            lambda *args, **kwargs: self.xml_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_ftp", 
            lambda *args, **kwargs: self.ftp_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'file_pattern'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_email", 
            lambda *args, **kwargs: self.email_extractor.extract(**{k: kwargs[k] for k in ['start_date', 'end_date', 'subject_pattern'] if k in kwargs})))
        
        self.scheduler.add_task(Task("extract_api", 
            lambda *args, **kwargs: self.api_extractor.extract(**{k: kwargs[k] for k in ['platform', 'start_date', 'end_date', 'resource_type'] if k in kwargs})))
        
        # Transform tasks - use wrapper functions to handle parameters correctly
        self.scheduler.add_task(Task("transform_sales_data", 
            lambda *args, **kwargs: self.sales_transformer.transform(args[0] if args else None), 
            ["extract_csv", "extract_json", "extract_excel", "extract_pdf", "extract_db", "extract_xml", "extract_ftp", "extract_email", "extract_api"]))
        
        self.scheduler.add_task(Task("transform_product_data", 
            lambda *args, **kwargs: self.product_transformer.transform(args[0] if args else None), 
            ["transform_sales_data"]))
        
        self.scheduler.add_task(Task("transform_customer_data", 
            lambda *args, **kwargs: self.customer_transformer.transform(args[0] if args else None), 
            ["transform_sales_data"]))
        
        # Calculate metrics - use a wrapper to handle multiple dataframes
        self.scheduler.add_task(Task("calculate_metrics", 
            lambda *args, **kwargs: self.metrics_calculator.calculate(**{k: kwargs[k] for k in kwargs if k in ['sales_data', 'product_data', 'customer_data']}), 
            ["transform_sales_data", "transform_product_data", "transform_customer_data"]))
        
        # Validate tasks
        self.scheduler.add_task(Task("validate_data", 
            lambda *args, **kwargs: self.validator.validate(args[0] if args else None), 
            ["calculate_metrics"]))
        
        # Load tasks
        self.scheduler.add_task(Task("load_to_db", 
            lambda *args, **kwargs: self.db_loader.load(args[0] if args else None, **{k: kwargs[k] for k in ['table_name', 'if_exists'] if k in kwargs}), 
            ["validate_data"]))
        
        self.scheduler.add_task(Task("export_to_files", 
            lambda *args, **kwargs: self.file_loader.export(args[0] if args else None, **{k: kwargs[k] for k in ['formats', 'prefix'] if k in kwargs}), 
            ["validate_data"]))
        
        # Report tasks
        self.scheduler.add_task(Task("generate_report", 
            lambda *args, **kwargs: self.report_generator.generate(args[0] if args else None, **{k: kwargs[k] for k in ['report_type', 'output_dir'] if k in kwargs}), 
            ["validate_data"]))
    
    def run_etl(self, source, platform=None, start_date=None, end_date=None, product_category=None, customer_segment=None):
        """
        Run the ETL pipeline for the specified source.
        
        Args:
            source (str): Data source ('csv', 'json', 'excel', etc.)
            platform (str, optional): E-commerce platform for API extraction
            start_date (str, optional): Start date for data filtering
            end_date (str, optional): End date for data filtering
            product_category (str, optional): Product category filter
            customer_segment (str, optional): Customer segment filter
        """
        logger.info(f"Running ETL pipeline for {source} source")
        
        # Initialize empty dataframes for each data type
        sales_data = pd.DataFrame()
        product_data = pd.DataFrame()
        customer_data = pd.DataFrame()
        
        # Extract parameters dictionary
        extract_params = {
            'start_date': start_date,
            'end_date': end_date,
            'product_category': product_category,
            'customer_segment': customer_segment
        }
        
        # Add platform parameter for API extraction
        if source == 'api' and platform:
            extract_params['platform'] = platform
        
        # Execute extraction based on source
        try:
            if source == 'csv':
                data = self.scheduler.run("extract_csv", **extract_params)
            elif source == 'json':
                data = self.scheduler.run("extract_json", **extract_params)
            elif source == 'excel':
                data = self.scheduler.run("extract_excel", **extract_params)
            elif source == 'pdf':
                data = self.scheduler.run("extract_pdf", **extract_params)
            elif source == 'db':
                data = self.scheduler.run("extract_db", **extract_params)
            elif source == 'xml':
                data = self.scheduler.run("extract_xml", **extract_params)
            elif source == 'ftp':
                data = self.scheduler.run("extract_ftp", **extract_params)
            elif source == 'email':
                data = self.scheduler.run("extract_email", **extract_params)
            elif source == 'api':
                data = self.scheduler.run("extract_api", **extract_params)
            else:
                logger.warning(f"Unknown source: {source}")
                return
                
            if data is not None and not data.empty:
                # Transform data
                sales_data = self.scheduler.run("transform_sales_data", data)
                product_data = self.scheduler.run("transform_product_data", sales_data)
                customer_data = self.scheduler.run("transform_customer_data", sales_data)
                
                # Calculate metrics
                metrics_data = self.scheduler.run("calculate_metrics", 
                                                sales_data=sales_data, 
                                                product_data=product_data, 
                                                customer_data=customer_data)
                
                # Validate data
                validated_data = self.scheduler.run("validate_data", metrics_data)
                
                # Load data to database
                self.scheduler.run("load_to_db", validated_data, 
                                  table_name=f"{source}_sales_data", 
                                  if_exists='append')
                
                # Export to files
                self.scheduler.run("export_to_files", validated_data, 
                                  formats=['csv', 'json', 'excel'], 
                                  prefix=source)
                
                logger.info(f"ETL pipeline for {source} source completed successfully")
                return validated_data
            else:
                logger.warning(f"No data extracted from {source} source")
                return None
                
        except Exception as e:
            logger.error(f"Error in ETL pipeline for {source} source: {str(e)}")
            raise
    
    def generate_reports(self):
        """Generate reports based on processed data."""
        try:
            # Get all data from database
            data = self.db_extractor.extract_all_tables()
            
            if data and not all(df.empty for df in data.values()):
                # Generate different types of reports
                for report_type in ['sales_summary', 'product_performance', 'customer_segmentation', 'inventory_status']:
                    self.scheduler.run("generate_report", data, 
                                      report_type=report_type, 
                                      output_dir=self.config.reports_dir)
                
                logger.info("Reports generated successfully")
                return True
            else:
                logger.warning("No data available for report generation")
                return False
                
        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")
            raise
