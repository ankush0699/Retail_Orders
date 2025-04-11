# main.py

import logging
from pipeline.download import download_from_kaggle
from pipeline.extract import extract_data
from pipeline.transform import transform_data
from pipeline.load import create_database_if_not_exists, load_to_mysql
from pipeline.config import CONFIG

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.info("🚀 ETL pipeline started")

    # Step 1: Download dataset from Kaggle
    download_from_kaggle(CONFIG["dataset_id"], CONFIG["download_dir"])

    # Step 2: Extract data from CSV
    file_path = f"{CONFIG['download_dir']}/{CONFIG['file_name']}"
    df_raw = extract_data(file_path)

    # Step 3: Transform data
    df_cleaned = transform_data(df_raw)

    # Step 4: Load to MySQL
    create_database_if_not_exists(
    user=CONFIG["mysql"]["user"],
    password=CONFIG["mysql"]["password"],
    host=CONFIG["mysql"]["host"],
    db_name=CONFIG["mysql"]["db_name"]
    )
    load_to_mysql(df_cleaned, **CONFIG["mysql"])

    logging.info("ETL pipeline completed successfully")

if __name__ == "__main__":
    main()
