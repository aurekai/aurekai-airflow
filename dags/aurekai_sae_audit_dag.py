from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("aurekai_sae_audit", start_date=datetime(2026, 5, 1),
         schedule=None, catchup=False, tags=["aurekai"]) as dag:
    run = BashOperator(task_id="run", bash_command="akai sae audit --model default --json")
