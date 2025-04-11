# etl_pipeline.py

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging
import os
import subprocess
import chardet

# Download data from Kaggle

def download_from_kaggle(dataset: str, download_path: str):
    """Downloads and unzips the full dataset from Kaggle."""
    logging.info("Downloading full dataset from Kaggle")

    zip_name = f"{dataset.split('/')[-1]}.zip"
    zip_path = os.path.join(download_path, zip_name)

    if os.path.exists(zip_path):
        logging.info("Removing existing dataset zip file: %s", zip_path)
        os.remove(zip_path)

    try:
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", dataset,
            "-p", download_path
        ], check=True)

        logging.info("Unzipping downloaded dataset...")
        import zipfile
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)

        logging.info("Download and unzip completed.")
    except subprocess.CalledProcessError as e:
        logging.error("Kaggle dataset download failed: %s", e)
        raise RuntimeError(f"Kaggle download failed: {e}")

# Detect file encoding

def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
    return result['encoding']

# Extraction

def extract_data(file_path: str) -> pd.DataFrame:
    logging.info("Extracting data from %s", file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' was not found.")

    encoding = detect_encoding(file_path)
    if encoding is None:
        logging.warning("Encoding could not be detected. Falling back to ISO-8859-1.")
        encoding = 'ISO-8859-1'
    else:
        logging.info("Detected file encoding: %s", encoding)

    try:
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            na_values=['Not Available', 'unknown'],
            on_bad_lines='skip'  # skip problematic rows to prevent parser errors
        )
    except Exception as e:
        logging.error("Error reading CSV: %s", e)
        raise

    return df

# Transformation

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming data")
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df['discount'] = df['list_price'] * df['discount_percent'] * 0.01
    df['sale_price'] = df['list_price'] - df['discount']
    df['profit'] = df['sale_price'] - df['cost_price']
    df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")
    df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)
    return df

# Database Setup

def create_database_if_not_exists(user: str, password: str, host: str, db_name: str):
    logging.info("Ensuring database '%s' exists", db_name)
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.close()
    conn.close()

# Loading to MySQL

def load_to_mysql(df: pd.DataFrame, user: str, password: str, host: str, db_name: str, table_name: str):
    logging.info("Loading data to MySQL table '%s.%s'", db_name, table_name)
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{db_name}")
    with engine.connect() as conn:
        df.to_sql(name=table_name, con=conn, index=False, if_exists='append')

# Main Pipeline Execution

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("ETL process started")

    # Step 1: Download the dataset from Kaggle
    dataset_id = "ankitbansal06/retail-orders"
    file_name = "orders.csv"
    download_dir = "."

    download_from_kaggle(dataset_id, download_dir)

    # Step 2: Extract
    df_raw = extract_data(f"{download_dir}/{file_name}")

    # Step 3: Transform
    df_cleaned = transform_data(df_raw)

    # Step 4: Load to MySQL
    db_config = {
        "user": "root",
        "password": "yourpassword",
        "host": "localhost",
        "db_name": "retail_orders_data",
        "table_name": "df_orders"
    }

    create_database_if_not_exists(db_config['user'], db_config['password'], db_config['host'], db_config['db_name'])
    load_to_mysql(df_cleaned, **db_config)

    logging.info("ETL process completed successfully")

if __name__ == "__main__":
    main()
