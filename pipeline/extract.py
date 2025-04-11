# pipeline/extract.py

import pandas as pd
import os
import logging
import chardet

def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
    return result['encoding']

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
            on_bad_lines='skip'
        )
    except Exception as e:
        logging.error("Error reading CSV: %s", e)
        raise

    return df
