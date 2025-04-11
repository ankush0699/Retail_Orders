# pipeline/config.py

from dotenv import load_dotenv
import os

load_dotenv()

CONFIG = {
    "dataset_id": "ankitbansal06/retail-orders",
    "file_name": "orders.csv",
    "download_dir": "dataset",
    "mysql": {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "host": os.getenv("MYSQL_HOST"),
        "db_name": os.getenv("MYSQL_DB"),
        "table_name": os.getenv("MYSQL_TABLE")
    }
}
