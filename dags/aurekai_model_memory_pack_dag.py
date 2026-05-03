from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("aurekai_model_memory_pack", start_date=datetime(2026, 5, 1),
         schedule=None, catchup=False, tags=["aurekai"]) as dag:
    run = BashOperator(task_id="run", bash_command="akai pack --tag latest --json")
