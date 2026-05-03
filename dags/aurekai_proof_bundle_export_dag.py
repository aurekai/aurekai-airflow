from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("aurekai_proof_bundle_export", start_date=datetime(2026, 5, 1),
         schedule=None, catchup=False, tags=["aurekai"]) as dag:
    run = BashOperator(task_id="run", bash_command="akai proof export --output /tmp/aurekai-proof.tar.gz")
