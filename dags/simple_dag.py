from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello, Airflow!")

with DAG(
    dag_id="simple_dag!!!!",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # запуск каждый день
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello,
    )

    task_hello
