import os
import zipfile
import pandas as pd
import sqlalchemy as sal
import logging
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi

# Load environment variables
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Setup logging (to both console and file)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger()

def ensure_database_exists():
    log.info("Ensuring database exists...")
    engine = sal.create_engine(
        f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/'
    )
    conn = engine.connect()
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    conn.close()
    log.info("Database checked/created successfully.")

def download_dataset(dataset: str, filename: str):
    log.info("Downloading dataset...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset, file_name=filename)
    log.info("Download complete.")

def extract_zip(zip_file: str, extract_path: str = "."):
    log.info("Extracting zip file...")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    log.info("Extraction complete.")

def preprocess_data(file_path: str) -> pd.DataFrame:
    log.info("Reading and preprocessing data...")
    df = pd.read_csv(file_path, na_values=['Not Available', 'unknown'])

    # Clean column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Derive profit column
    df['profit'] = df['sale_price'] - df['cost_price']

    # Convert date column
    df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")

    # Drop unnecessary columns
    df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)

    log.info("Preprocessing complete.")
    return df

def load_to_sql(df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
    log.info(f"Loading data into MySQL (table: {table_name})...")
    engine = sal.create_engine(
        f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )
    conn = engine.connect()
    df.to_sql(table_name, con=conn, index=False, if_exists=if_exists)
    conn.close()
    log.info("Data load complete.")

def main():
    dataset = 'ankitbansal06/retail-orders'
    filename = 'orders.csv'
    zip_file = 'orders.csv.zip'
    csv_file = 'orders.csv'
    table_name = 'df_orders'

    ensure_database_exists()

    if not os.path.exists(csv_file):
        download_dataset(dataset, filename)
        extract_zip(zip_file)

    df = preprocess_data(csv_file)
    load_to_sql(df, table_name, if_exists='append')

if __name__ == "__main__":
    main()
