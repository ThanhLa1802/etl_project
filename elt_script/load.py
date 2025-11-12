from sqlalchemy import create_engine
import logging

def load_to_postgres(df, conn_string, table_name):
    logging.info(f"Loading data to {table_name}")
    engine = create_engine(conn_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logging.info("Load completed successfully")