from airflow import DAG
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

#with DAG(dag_id='minha_simples_dag', schedule_interval="@hourly", start_date=datetime(2023,1,14)) as dag:
with DAG(dag_id='minha_simples_dag', schedule_interval=timedelta(days=30), start_date=datetime(2023,1,14)) as dag:
    task_1 = DummyOperator(
        task_id = 'task_1'
    )
