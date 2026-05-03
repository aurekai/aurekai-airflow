"""
aurekai_model_memory_build_dag.py
Memory pipeline: model pull → FPQ compress → FPQx align → SLI auto-run → SAE activate → proof
Uses Dynamic Task Mapping for multi-model FPQ matrix jobs.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from aurekai_operator import (
    AurekaiFPQCompressOperator,
    AurekaiFPQRoundtripOperator,
    AurekaiFPQxAlignOperator,
    AurekaiSLIAutoRunOperator,
    AurekaiProofBundleOperator,
)

MODEL_MATRIX = [
    {"model": "qwen3-8b", "bits": 8},
    {"model": "qwen3-8b", "bits": 4},
    {"model": "qwen3-32b", "bits": 8},
]

with DAG(
    dag_id="aurekai_model_memory_build",
    start_date=datetime(2026, 5, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["aurekai", "memory", "fpq"],
    params={
        "model_tag": {"type": "string", "default": "qwen3-8b"},
        "bits": {"type": "integer", "default": 8},
    },
) as dag:

    compress = AurekaiFPQCompressOperator(
        task_id="fpq_compress",
        model_tag="{{ params.model_tag }}",
        bits="{{ params.bits | int }}",
        outlets=[Dataset("akai://artifacts/fpq-model")],
    )

    roundtrip = AurekaiFPQRoundtripOperator(
        task_id="fpq_roundtrip",
        model_tag="{{ params.model_tag }}",
    )

    align = AurekaiFPQxAlignOperator(
        task_id="fpqx_align",
        model_tag="{{ params.model_tag }}",
        outlets=[Dataset("akai://artifacts/fpqx-alignment")],
    )

    sli = AurekaiSLIAutoRunOperator(
        task_id="sli_auto_run",
        outlets=[Dataset("akai://artifacts/sli-report")],
    )

    proof = AurekaiProofBundleOperator(
        task_id="proof_bundle",
        outlets=[Dataset("akai://artifacts/proof")],
    )

    compress >> roundtrip >> align >> sli >> proof
