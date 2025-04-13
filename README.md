# E-commerce Sales ETL Pipeline

## 📊 Overview

This project is a comprehensive data pipeline for processing e-commerce sales data from multiple sources. It extracts data from various e-commerce platforms and file formats, transforms it to calculate relevant business metrics, and loads it into databases for analysis.

## 📁 Project Structure

- `main.py` - Entry point with command-line argument parsing
- `config.py` - Configuration settings with defaults for paths and database
- `orchestrator.py` - Pipeline execution logic with Task class and SimpleScheduler
- `extractors/` - Modules for different data sources
- `transformers/` - Data transformation modules for sales calculations
- `validators/` - Data validation
- `loaders/` - Database and file export functionality
- `.gitignore` - Specifies files and directories to exclude from version control

## 📋 Data Sources

- CSV: Order data, product catalogs, customer information
- JSON: API data from e-commerce platforms (Shopify, Amazon, etc.)
- Excel: Marketing campaign data, inventory reports
- PDF: Supplier invoices, shipping manifests
- SQLite DB: Historical sales data
- XML: Product feeds, order exports
- FTP/SFTP: Partner data feeds
- Email: Order confirmations, customer inquiries (text extraction)

## 🚀 Setup and Usage

1. Install dependencies:
```powershell
pip install -r requirements.txt
```

2. Run the ETL pipeline:
```powershell
python main.py --source all
```

Available source options: csv, json, excel, pdf, db, xml, ftp, email, all

## ✨ Features

- Extract e-commerce data from multiple sources
- Calculate business metrics (sales trends, customer LTV, inventory turnover, etc.)
- Validate data for consistency and completeness
- Load processed data into databases or export as various file formats
- Generate automated reports (text-based, no UI)

## 📚 Documentation

For detailed information about using and extending this ETL pipeline, refer to:

- [User Guide](USER_GUIDE.md) - Instructions for end users running the pipeline
- [Developer Guide](DEVELOPER_GUIDE.md) - Technical documentation for developers

## 🔒 Version Control

This project includes a `.gitignore` file configured to exclude:
- Python virtual environments and bytecode
- Log files and data directories
- IDE-specific files
- Environment variables and sensitive information
- OS-specific files and temporary data
