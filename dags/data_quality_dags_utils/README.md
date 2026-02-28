
Пример добавления в дагах

```python

from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from beeline.airflow.hashicorp_vault.VaultOperator import VaultOperator


local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 18, 10, 0, tzinfo=local_tz),
    'email': ['PDudkov@srt.beeline.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    f'Custos-dq',
    default_args=default_args,
    description='daily datamart details numbers HLR',
    schedule_interval="@once",  # 00 08 * * *
    catchup=True,
    tags=['prod'],
    max_active_runs=1,
    on_success_callback=VaultOperator.cleanup_xcom,
    on_failure_callback=VaultOperator.cleanup_xcom
)

# --------------- CUSTOS DQ ------------------------------  

from data_quality_dags_utils.dq_operator_config import get_dq_config
from data_quality_dags_utils.dq_operator import CustosDQOperator

dq_config = get_dq_config()
dq_config['src_table'] = "rep_20_n8_1_details_num_hlr"

dq_app = CustosDQOperator(dq_config=dq_config, dag=dag)

# ------------------------------------------------------


dq_app
```
