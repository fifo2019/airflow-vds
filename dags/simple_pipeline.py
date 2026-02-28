from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from data_quality_dags_utils.dq_operator_config import get_dq_config
from data_quality_dags_utils.custosdqoperator import CustosDQOperator
import requests
import io
import pandas as pd
from airflow.models import Variable
from utils.sensors import ParquetFileSensor
from utils.connections import get_psql_engine


def get_green_trip(**context):

    date_str = context['execution_date'].strftime('%Y-%m')

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{date_str}.parquet'

    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()

    df = pd.read_parquet(io.BytesIO(r.content))
    print(f"Loaded from {url}")

    print(df.head())

    engine = get_psql_engine()
    df.to_sql(
        name="green_trip",
        con=engine,
        schema="public",
        if_exists="append"
    )

    print("Inserted")


with DAG(
    dag_id="custom_dag",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 12, 1),
    # schedule="@monthly",   # запуск каждый день
    schedule="@once",   # запуск каждый день
    catchup=True,
    tags=["example"],
) as dag:

    check_file = ParquetFileSensor(
        task_id="check_parquet",
        url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{{ data_interval_start.strftime('%Y-%m') }}.parquet",
        poke_interval=3600,  # проверять раз в час
        timeout=86400 * 7,  # ждать до 7 дней
        mode="reschedule",  # освобождать слот между проверками
    )

    get_green_trip = PythonOperator(
        task_id="get_green_trip",
        python_callable=get_green_trip,
    )

    # # ----- For data quality -----
    dq_config = get_dq_config()
    dq_config['src_table'] = "green_trip"
    data_str = "{{ data_interval_start.strftime('%Y-%m') }}"
    dq_config["src_condition"] = f"WHERE TO_CHAR(lpep_pickup_datetime, 'YYYY-MM') = '{data_str}'"
    dq_app = CustosDQOperator(dq_config=dq_config, dag=dag)
    # ----------- End data quality -------------------

    check_file >> get_green_trip >> dq_app