import pandas as pd
import logging

def extract_from_csv(file_path: str):
    logging.info(f"Extracting data from {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Extracted {len(df)} rows")
    return df