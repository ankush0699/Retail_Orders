# pipeline/transform.py

import pandas as pd
import logging

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming data")

    # Standardize column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Add derived columns
    df['discount'] = df['list_price'] * df['discount_percent'] * 0.01
    df['sale_price'] = df['list_price'] - df['discount']
    df['profit'] = df['sale_price'] - df['cost_price']

    # Fix data types
    df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")

    # Drop columns not needed further
    df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)

    return df
