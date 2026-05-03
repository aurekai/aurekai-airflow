"""
aurekai_audio_to_brief_dag.py
Full intake pipeline: ingest audio → transcribe → clean → brief → proof bundle → meter
Uses AurekaiDataset URIs for data-aware scheduling.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.operators.python import get_current_context

from aurekai_operator import (
    AurekaiTranscribeOperator,
    AurekaiTranscriptCleanOperator,
    AurekaiBriefOperator,
    AurekaiProofBundleOperator,
    AurekaiMeterRecordOperator,
)

AUDIO_DATASET = Dataset("akai://artifacts/audio-intake")
BRIEF_DATASET = Dataset("akai://artifacts/brief")
PROOF_DATASET = Dataset("akai://artifacts/proof")

with DAG(
    dag_id="aurekai_audio_to_brief",
    start_date=datetime(2026, 5, 1),
    schedule=[AUDIO_DATASET],
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["aurekai", "intake", "proof"],
    params={
        "audio_path": {"type": "string", "default": "/data/intake/latest.wav"},
        "client_id": {"type": "string", "default": ""},
        "language": {"type": "string", "default": "en"},
    },
) as dag:

    transcribe = AurekaiTranscribeOperator(
        task_id="transcribe",
        audio_path="{{ params.audio_path }}",
        language="{{ params.language }}",
        outlets=[Dataset("akai://artifacts/transcript")],
    )

    clean = AurekaiTranscriptCleanOperator(
        task_id="clean_transcript",
        transcript_id="{{ ti.xcom_pull(task_ids='transcribe', key='artifact_id') }}",
        outlets=[Dataset("akai://artifacts/clean-transcript")],
    )

    brief = AurekaiBriefOperator(
        task_id="generate_brief",
        artifact_id="{{ ti.xcom_pull(task_ids='clean_transcript', key='artifact_id') }}",
        outlets=[BRIEF_DATASET],
    )

    proof = AurekaiProofBundleOperator(
        task_id="proof_bundle",
        outlets=[PROOF_DATASET],
    )

    @task(outlets=[Dataset("akai://metering/audio-brief")])
    def meter_usage():
        ctx = get_current_context()
        client_id = ctx["params"].get("client_id", "")
        from aurekai_operator import _run_akai
        _run_akai(["meter", "record", "--event", "audio-to-brief",
                   "--units", "1", "--client", client_id] if client_id else
                  ["meter", "record", "--event", "audio-to-brief", "--units", "1"])

    transcribe >> clean >> brief >> proof >> meter_usage()
