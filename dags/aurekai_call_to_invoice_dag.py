"""
aurekai_call_to_invoice_dag.py
Commerce pipeline: tel sim → transcribe → clean → brief → proof → meter → invoice → outreach
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

from aurekai_operator import (
    AurekaiTranscribeOperator,
    AurekaiTranscriptCleanOperator,
    AurekaiBriefOperator,
    AurekaiProofBundleOperator,
    AurekaiMeterRecordOperator,
    AurekaiInvoiceOperator,
)

with DAG(
    dag_id="aurekai_call_to_invoice",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["aurekai", "commerce", "wire"],
    params={
        "audio_path": {"type": "string", "default": "/data/calls/latest.wav"},
        "client_id": {"type": "string"},
        "language": {"type": "string", "default": "en"},
    },
) as dag:

    transcribe = AurekaiTranscribeOperator(
        task_id="transcribe_call",
        audio_path="{{ params.audio_path }}",
        language="{{ params.language }}",
    )

    clean = AurekaiTranscriptCleanOperator(
        task_id="clean_transcript",
        transcript_id="{{ ti.xcom_pull(task_ids='transcribe_call', key='artifact_id') }}",
    )

    brief = AurekaiBriefOperator(
        task_id="generate_brief",
        artifact_id="{{ ti.xcom_pull(task_ids='clean_transcript', key='artifact_id') }}",
    )

    proof = AurekaiProofBundleOperator(task_id="proof_bundle")

    meter = AurekaiMeterRecordOperator(
        task_id="meter_usage",
        event="call-intake",
        units=1.0,
        client_id="{{ params.client_id }}",
    )

    invoice = AurekaiInvoiceOperator(
        task_id="generate_invoice",
        client_id="{{ params.client_id }}",
        period="current",
        outlets=[Dataset("akai://artifacts/invoice")],
    )

    @task
    def outreach_followup():
        from aurekai_operator import _run_akai
        _run_akai(["outreach", "followup", "--client", "{{ params.client_id }}"])

    transcribe >> clean >> brief >> proof >> meter >> invoice >> outreach_followup()
