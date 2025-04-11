# pipeline/load.py

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging

def create_database_if_not_exists(user: str, password: str, host: str, db_name: str):
    logging.info("Ensuring database '%s' exists", db_name)
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.close()
    conn.close()

def load_to_mysql(df: pd.DataFrame, user: str, password: str, host: str, db_name: str, table_name: str):
    logging.info("Loading data to MySQL table '%s.%s'", db_name, table_name)
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{db_name}")
    with engine.connect() as conn:
        df.to_sql(name=table_name, con=conn, index=False, if_exists='append')
