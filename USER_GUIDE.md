# E-commerce Sales ETL Pipeline - User Guide

## 📊 Overview

The E-commerce Sales ETL Pipeline is a comprehensive data processing system designed to extract, transform, and load data from multiple e-commerce sources. This pipeline enables you to consolidate sales, product, and customer data for analysis and reporting.

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Pip package manager

### Installation

1. Clone or download the repository to your local machine.

2. Install the required dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

## 📋 Data Sources

The pipeline supports the following data sources:

- **CSV files**: Order data, product catalogs, customer information
- **JSON files**: API data from e-commerce platforms
- **Excel files**: Marketing campaign data, inventory reports
- **PDF files**: Supplier invoices, shipping manifests
- **SQLite DB**: Historical sales data
- **XML files**: Product feeds, order exports
- **FTP/SFTP**: Partner data feeds
- **Email**: Order confirmations, customer inquiries
- **APIs**: Direct connections to platforms like Shopify, Amazon, etc.

## 🔄 Running the Pipeline

### Basic Usage

To run the complete ETL pipeline processing all data sources:

```powershell
python main.py --source all
```

### Processing Specific Data Sources

To process only specific data sources:

```powershell
python main.py --source csv
python main.py --source json
python main.py --source excel
python main.py --source pdf
python main.py --source db
python main.py --source xml
python main.py --source ftp
python main.py --source email
python main.py --source api
```

### Filtering Data

Filter data by date range:

```powershell
python main.py --source all --start-date 2023-01-01 --end-date 2023-12-31
```

Filter by product category:

```powershell
python main.py --source all --product-category Electronics
```

Filter by customer segment:

```powershell
python main.py --source all --customer-segment VIP
```

### Generating Reports

To run the pipeline and generate reports:

```powershell
python main.py --source all --report
```

## 📁 Output Files

The pipeline produces several output files:

- **Processed Data**: Located in `data/processed/`
  - CSV, JSON, and Excel versions of processed data
  
- **Reports**: Located in `data/processed/reports/`
  - Sales summary reports
  - Product performance reports
  - Customer segmentation analysis
  - Inventory status reports

- **Database**: Located in `data/ecommerce_sales.db`
  - SQLite database containing all processed data

## 🔌 Connecting to External Systems

### API Configuration

To connect to e-commerce platforms via API, configure your credentials in environment variables:

```powershell
$env:SHOPIFY_API_KEY = "your_api_key"
$env:SHOPIFY_API_SECRET = "your_api_secret"
$env:SHOPIFY_STORE = "your-store.myshopify.com"

$env:AMAZON_ACCESS_KEY = "your_access_key"
$env:AMAZON_SECRET_KEY = "your_secret_key"
$env:AMAZON_SELLER_ID = "your_seller_id"
```

### FTP Configuration

For FTP/SFTP connections:

```powershell
$env:FTP_HOST = "ftp.example.com"
$env:FTP_USER = "username"
$env:FTP_PASSWORD = "password"
$env:FTP_PORT = "21"
$env:FTP_PATH = "/exports/"
```

### Database Configuration

For external databases:

```powershell
$env:MYSQL_HOST = "localhost"
$env:MYSQL_PORT = "3306"
$env:MYSQL_USER = "root"
$env:MYSQL_PASSWORD = "password"
$env:MYSQL_DATABASE = "ecommerce_etl"
```

## 📊 Analyzing the Data

The pipeline calculates various business metrics:

- Sales performance metrics (revenue, orders, AOV)
- Product performance (top sellers, inventory turnover)
- Customer segmentation (LTV, retention rates)
- Time-based analysis (daily, weekly, monthly trends)

These metrics are available in both the generated reports and the database.

## 🔍 Troubleshooting

### Common Issues

- **Missing Data Sources**: The pipeline will create sample data if a source file doesn't exist
- **API Connection Errors**: Verify your API credentials in the environment variables
- **Database Connection Issues**: Ensure your database is running and accessible

### Logs

Log files are stored in the `logs` directory. Check these for detailed information about any errors or warnings.

## 🆘 Getting Help

If you encounter issues or have questions about using the E-commerce ETL Pipeline, please:

1. Check the log files for error messages
2. Review this User Guide for instructions
3. Consult the Developer Guide for technical details
4. Contact the maintainer if the issue persists
