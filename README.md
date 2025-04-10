# Retail Orders Documentation

This document serves as the complete technical documentation for the Retail Orders ETL project. It includes:

1. **ETL Process Design**
   - Data extraction from Kaggle using CLI and API authentication.
   - Data transformation using pandas: column normalization, missing value handling, new feature creation.
   - Data loading into MySQL using SQLAlchemy.

2. **System Requirements and Setup Instructions**
   - Python version, libraries, and environment setup.
   - MySQL server configuration.
   - Kaggle API setup for secure dataset access.

3. **Execution Guide**
   - How to run the script, expected outputs, and logics behind data modifications.

4. **SQL Query Catalog**
   - A list of business-relevant SQL queries used to derive insights from the data, with notes on what each query achieves.

5. **Best Practices**
   - Use of parameterized DB connections.
   - Handling repeated execution behavior (e.g., `if_exists='append'` in `to_sql`).

6. **Next Steps** (Optional Enhancements)
   - Integration with a dashboard (e.g., Power BI, Streamlit).
   - Automating the ETL using Airflow or cron jobs.
   - Logging and exception handling for production-readiness.

## License

This project is for educational and development purposes only.


## Section 1: Creation of ETL Pipeline for `retail_orders` Dataset

# Retail Orders ETL Script

This Python script performs a complete ETL (Extract, Transform, Load) process using a retail orders dataset from Kaggle. The script downloads the dataset, processes it by cleaning and transforming the data, and loads it into a MySQL database.

## Overview

- Downloads the dataset using the Kaggle CLI.
- Cleans and transforms the data using pandas.
- Calculates financial metrics such as discount, sale price, and profit.
- Loads the processed data into a MySQL database.
- Performs data analysis using SQL queries on the loaded dataset.

## Project Flow Diagram

Below is the architecture diagram of the ETL pipeline:

![Project Flow](project_flow.png)

### Description:
- **Kaggle API**: Used to download the dataset programmatically.
- **Python Script**: Handles dataset download, cleaning, transformation, and metric calculation.
- **Pandas**: Used for in-memory data cleaning and processing.
- **SQL Server**: Stores the cleaned dataset for analysis.
- **SQL Queries**: Used to derive analytical insights and perform business intelligence tasks.

## Requirements

- Python 3.7+
- MySQL Server (running locally or accessible remotely)
- Required Python packages:
  - pandas
  - sqlalchemy
  - mysql-connector-python
  - kaggle

You can install the required packages using:

```bash
pip install pandas sqlalchemy mysql-connector-python kaggle
```

## Kaggle API Setup

1. Go to your Kaggle account settings and generate an API token.
2. Place the downloaded `kaggle.json` file in the directory `~/.kaggle/`.
3. Ensure the following permissions:
   - The file should be readable only by the user: `chmod 600 ~/.kaggle/kaggle.json`.

Alternatively, you can set the credentials via environment variables:

```bash
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key
```

## MySQL Configuration

Ensure your MySQL server is running. Update the connection credentials in the script to match your setup:

```python
user="root",
password="*****",
host="localhost",
port=3306
```

The script creates a database named `retail_orders` if it does not already exist and inserts data into a table named `df_orders`.

## Running the Script

Run the script from your terminal:

```bash
python etl_retail_order.py
```

The script performs the following steps:

1. Downloads and unzips the dataset from Kaggle.
2. Loads the data into a pandas DataFrame.
3. Renames columns, cleans missing values, and derives new columns.
4. Drops unnecessary columns.
5. Connects to MySQL and creates the database (if needed).
6. Inserts the cleaned data into the `df_orders` table.

## Output

- MySQL Database: `retail_orders`
- Table: `df_orders`
- Columns: cleaned and transformed retail order data

## SQL Queries Executed on `df_orders`

The following SQL queries were used to analyze and extract insights from the `df_orders` table:

### 0. Top 10 Highest Revenue Generating Products
- Aggregated total revenue per product and selected the top 10 products with the highest revenue.

### 1. Month-over-Month (MoM) Sales Comparison for FY 2022 vs FY 2023
- Compared monthly sales revenue for each month between FY 2022 and FY 2023.
- Included YoY percentage change and trend indicators (↑, ↓, →).

### 2. Top 5 Products per Region by Revenue
- Used `ROW_NUMBER()` with `PARTITION BY region` to rank products based on total revenue.

### 3. MoM Revenue Growth Query Optimization
- Created a pivoted version of monthly revenue using `CASE` and `MAX()` to display FY 2022 and 2023 side-by-side.

### 4. Highest Sales Month per Category
- Identified the month with the highest sales for each product category using `ROW_NUMBER()`.

These queries provided key insights into seasonal trends, regional performance, and product-wise revenue contribution.

## Notes

- Repeated script execution will append data to the existing table. Modify the `if_exists` parameter in `to_sql()` if needed.
- Make sure the dataset and column names match the script's expectations if using a different source.



