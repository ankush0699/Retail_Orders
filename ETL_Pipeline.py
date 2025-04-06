# Retail_Etl_Mysql.py

import os
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# -------------------- 1. Setup Logging --------------------
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------- 2. Load Environment Variables --------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME", "retail_orders_db")

# -------------------- 3. Download Dataset from Kaggle --------------------
try:
    logging.info("Initializing Kaggle API...")
    api = KaggleApi()
    api.authenticate()

    logging.info("Downloading dataset...")
    api.dataset_download_files('ankitbansal06/retail-orders', path='data', unzip=True)
except Exception as e:
    logging.error(f"Kaggle dataset download failed: {e}")
    raise

# -------------------- 4. Load & Clean Dataset --------------------
try:
    logging.info("Reading CSV file...")
    df = pd.read_csv('data/orders.csv')

    # Debugging output to verify column names
    print("\n🔍 Available columns in dataset:", df.columns.tolist())
    logging.info(f"Columns found: {df.columns.tolist()}")

    logging.info("Cleaning data...")
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Safely calculate profit if both columns exist
    if 'selling_price' in df.columns and 'cost_price' in df.columns:
        df['profit'] = df['selling_price'] - df['cost_price']
    else:
        logging.warning("Skipping profit calculation. Columns missing: selling_price or cost_price")

    df.dropna(inplace=True)
except Exception as e:
    logging.error(f"Data cleaning failed: {e}")
    raise

# -------------------- 5. Backup CSV --------------------
try:
    backup_file = f"etl_backup_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(backup_file, index=False)
    logging.info(f"Backup saved as {backup_file}")
except Exception as e:
    logging.warning(f"Backup failed: {e}")

# -------------------- 6. Load to MySQL --------------------
try:
    logging.info("Connecting to MySQL...")

    # Connect without specifying database (for initial CREATE DATABASE)
    temp_conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    temp_cursor = temp_conn.cursor()
    temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    temp_conn.close()
    logging.info(f"Database '{DB_NAME}' ensured.")

    # Use SQLAlchemy for table creation and data insertion
    conn_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)

    df.to_sql(name='df_orders', con=engine, if_exists='replace', index=False)
    logging.info("Data loaded successfully into 'df_orders' table.")
except SQLAlchemyError as e:
    logging.error(f"MySQL insert failed: {e}")
    raise
except Exception as e:
    logging.error(f"MySQL connection failed: {e}")
    raise
