# Quickstart — aurekai-airflow

Run Aurekai pipeline templates as Apache Airflow DAGs.

## Requirements

- Apache Airflow >= 2.8.0
- `akai` CLI on `PATH`

## DAGs

| DAG | Description |
|---|---|
| `aurekai_doctor_deep` | Runs `akai doctor --deep` |
| `aurekai_manifest_verify` | Verifies Aurekai manifest |
| `aurekai_model_memory_pack` | Packs model memory artifacts |
| `aurekai_sae_audit` | SAE audit |
| `aurekai_semantic_cache_bench` | Semantic cache benchmark |
| `aurekai_proof_bundle_export` | Exports proof bundle |
| `aurekai_release_gate` | Release gate check |

## Validate

```bash
bash tests/validate-schemas.sh
bash tests/validate-scripts.sh
```
