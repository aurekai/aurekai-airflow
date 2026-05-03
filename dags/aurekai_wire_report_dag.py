"""
aurekai_wire_report_dag.py
Wire/telephony pipeline: pcap ingest → wire probe → wire report → proof → meter
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

from aurekai_operator import (
    AurekaiWireReportOperator,
    AurekaiProofBundleOperator,
    AurekaiMeterRecordOperator,
)

with DAG(
    dag_id="aurekai_wire_report",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["aurekai", "wire"],
    params={
        "pcap_path": {"type": "string", "default": "/data/captures/latest.pcap"},
        "capture_id": {"type": "string", "default": ""},
    },
) as dag:

    @task
    def wire_ingest(pcap_path: str):
        from aurekai_operator import _run_akai
        result = _run_akai(["wire", "ingest-pcap", "--input", pcap_path])
        return result.artifact_id

    @task
    def wire_probe(capture_id: str):
        from aurekai_operator import _run_akai
        return _run_akai(["wire", "probe", "--capture", capture_id]).json_output

    capture_id = wire_ingest("{{ params.pcap_path }}")

    probe = wire_probe(capture_id)

    report = AurekaiWireReportOperator(
        task_id="wire_report",
        capture_id="{{ ti.xcom_pull(task_ids='wire_ingest') }}",
        outlets=[Dataset("akai://artifacts/wire-report")],
    )

    proof = AurekaiProofBundleOperator(
        task_id="proof_bundle",
        outlets=[Dataset("akai://artifacts/proof")],
    )

    meter = AurekaiMeterRecordOperator(
        task_id="meter_wire",
        event="wire-report",
        units=1.0,
    )

    probe >> report >> proof >> meter
