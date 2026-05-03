from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="aurekai_release_gate",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["aurekai", "release"],
) as dag:
    doctor_deep = BashOperator(
        task_id="doctor_deep",
        bash_command="akai doctor --deep",
    )

    manifest_verify = BashOperator(
        task_id="manifest_verify",
        bash_command="test -f aurekai.manifest.json && test -f bonfyre.manifest.json",
    )

    release_gate = BashOperator(
        task_id="release_gate",
        bash_command="echo release gate passed",
    )

    doctor_deep >> manifest_verify >> release_gate
