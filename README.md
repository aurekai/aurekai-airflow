<p align="center">
  <img src="https://raw.githubusercontent.com/aurekai/aurekai/main/assets/aurekai-logo.svg" alt="Aurekai" width="520" />
</p>

# `aurekai-airflow` · v0.8.0-alpha.5

Official Apache Airflow integration for Aurekai — capability-native operators, data-aware datasets, and 8 production DAGs across all Akai capability families.

## Operators

| Operator | Capability family | Description |
|---|---|---|
| `AurekaiDoctorOperator` | runtime | Deep diagnostics |
| `AurekaiRuntimeCapabilitiesOperator` | runtime | Enumerate all Akai capability families |
| `AurekaiManifestVerifyOperator` | runtime | Validate `artifact.json` |
| `AurekaiQueueWorkOperator` | runtime | Enqueue work into AkaiQueue |
| `AurekaiTranscribeOperator` | intake | Audio transcription |
| `AurekaiTranscriptCleanOperator` | intake | Transcript normalization |
| `AurekaiBriefOperator` | publish | Generate brief from artifact |
| `AurekaiProofBundleOperator` | proof | Export `.akproof` bundle |
| `AurekaiFPQCompressOperator` | memory | FPQ model compression |
| `AurekaiFPQRoundtripOperator` | memory | FPQ fidelity roundtrip |
| `AurekaiFPQxAlignOperator` | memory | FPQx alignment |
| `AurekaiSLIAutoRunOperator` | memory | SLI auto-run convergence |
| `AurekaiVecSearchOperator` | memory | Vector search |
| `AurekaiGraphLineageOperator` | proof | Graph lineage export |
| `AurekaiMeterRecordOperator` | commerce | Record metering event |
| `AurekaiInvoiceOperator` | commerce | Generate client invoice |
| `AurekaiWireReportOperator` | wire | Wire capture report |
| `AurekaiReleaseGateOperator` | release | Release gate check |

All operators push `akai_result`, `proof_uri`, and `artifact_id` as XCom values.

## DAGs

| DAG | Schedule | Description |
|---|---|---|
| `aurekai_doctor_deep` | manual | Runtime diagnostics |
| `aurekai_manifest_verify` | manual | Manifest validation |
| `aurekai_model_memory_pack` | manual | Model memory pack |
| `aurekai_audio_to_brief` | Dataset: `akai://artifacts/audio-intake` | Intake → transcribe → clean → brief → proof |
| `aurekai_call_to_invoice` | manual | Call → transcribe → brief → proof → invoice → outreach |
| `aurekai_model_memory_build` | daily 03:00 UTC | FPQ compress → align → SLI → proof |
| `aurekai_wire_report` | manual | PCAP ingest → probe → report → proof |
| `aurekai_release_gate` | manual | Full release gate |

## Host-native features used

- **Datasets** — Akai artifact completion events as `akai://artifacts/{type}` URIs
- **TaskFlow API** — typed dataclasses for Akai results
- **Data-aware scheduling** — DAGs triggered by upstream artifact datasets
- **XCom** — `akai_result`, `proof_uri`, `artifact_id` propagated between tasks
- **Dynamic Task Mapping** — FPQ matrix jobs across models × bit-widths
- **Deferrable Operators** — long transcription tasks
- **SLA miss callbacks** — AkaiTier violation alerts

## Quick Start

```bash
pip install apache-airflow[celery]
pip install -r requirements.txt

# Copy operators plugin
cp plugins/aurekai_operator.py $AIRFLOW_HOME/plugins/

# Import DAGs
cp dags/*.py $AIRFLOW_HOME/dags/

airflow standalone
```

## Layout

```
plugins/
  aurekai_operator.py     18 capability-native Airflow operators
dags/
  aurekai_audio_to_brief_dag.py
  aurekai_call_to_invoice_dag.py
  aurekai_model_memory_build_dag.py
  aurekai_wire_report_dag.py
  aurekai_doctor_deep_dag.py
  aurekai_manifest_verify_dag.py
  aurekai_proof_bundle_export_dag.py
  aurekai_release_gate_dag.py
```


Aurekai integration surface for Airflow.

Status: active
Type: workflow

## Core Template Set

- doctor-deep
- manifest-verify
- model-memory-pack
- sae-audit
- semantic-cache-bench
- proof-bundle-export
- release-gate

## Canonical References

- Platform: https://github.com/aurekai/aurekai
- Native runtime: https://github.com/aurekai/native-runtime
- Integration registry: https://github.com/aurekai/aurekai/blob/main/registry/integrations.json
- Ecosystem map: https://github.com/aurekai/aurekai/blob/main/ECOSYSTEM_NAMES.md
## Starter Templates

- dags/aurekai_release_gate_dag.py
- requirements.txt
