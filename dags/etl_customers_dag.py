from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import subprocess

def extract():
    df = pd.read_csv("/opt/airflow/data/customers.csv")
    df.to_csv("/opt/airflow/data/customers_stage.csv", index=False)

def transform_and_load():
    df = pd.read_csv("/opt/airflow/data/customers_stage.csv")

    # Làm sạch số điện thoại
    df["Phone 1"] = df["Phone 1"].str.replace(r"x\\d+", "", regex=True)
    df["Phone 2"] = df["Phone 2"].str.replace(r"x\\d+", "", regex=True)

    # Ghép họ tên
    df["Full Name"] = df["First Name"] + " " + df["Last Name"]

    # Chuẩn hóa ngày
    df["Subscription Date"] = pd.to_datetime(df["Subscription Date"], errors="coerce")

    # Load vào PostgreSQL, dùng TRUNCATE + append để giữ view
    engine = create_engine("postgresql+psycopg2://user:pass@postgres:5432/mydb")

    with engine.begin() as conn:
        # Truncate table trước khi insert dữ liệu mới
        conn.execute(text("TRUNCATE TABLE customers_raw RESTART IDENTITY;"))

    # Append dữ liệu mới
    df.to_sql("customers_raw", engine, if_exists="append", index=False)

def transform_with_dbt():
    subprocess.run(
        ["dbt", "run", "--profiles-dir", "/opt/airflow/dbt_project"],
        cwd="/opt/airflow/dbt_project",
        check=True
    )

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="etl_customers_dbt",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)
    t3 = PythonOperator(task_id="transform_with_dbt", python_callable=transform_with_dbt)

    t1 >> t2 >> t3
