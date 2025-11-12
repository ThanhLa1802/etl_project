import logging
from extract import extract_from_csv
from transform import transform_data
from load import load_to_postgres

logging.basicConfig(
    filename="/logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_etl():
    logging.info("ETL job started")

    csv_path = "/data/customers-100000.csv"
    conn_string = "postgresql://user:pass@db:5432/mydb"

    df = extract_from_csv(csv_path)
    df = transform_data(df)
    load_to_postgres(df, conn_string, "users_cleaned")

    logging.info("ETL job finished successfully")

if __name__ == "__main__":
    run_etl()
