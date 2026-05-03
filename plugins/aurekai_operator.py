"""Aurekai Airflow custom operators — capability-native adapters over the Akai runtime."""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context


@dataclass
class AkaiResult:
    operator: str
    exit_code: int
    json_output: dict[str, Any] = field(default_factory=dict)
    proof_uri: str = ""
    artifact_id: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def _run_akai(args: list[str], timeout: int = 300) -> AkaiResult:
    operator = args[0] if args else "unknown"
    result = subprocess.run(
        ["akai", *args, "--json"],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    try:
        parsed = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        parsed = {"raw": result.stdout, "error": result.stderr}
    return AkaiResult(
        operator=operator,
        exit_code=result.returncode,
        json_output=parsed,
        proof_uri=parsed.get("proof_uri", ""),
        artifact_id=parsed.get("artifact_id", ""),
    )


class AurekaiBaseOperator(BaseOperator):
    """Base class for all Aurekai operators. Pushes AkaiResult as XCom."""

    ui_color = "#1a1a2e"
    ui_fgcolor = "#e0e0ff"

    def _run(self, args: list[str], context: Context, timeout: int = 300) -> AkaiResult:
        result = _run_akai(args, timeout=timeout)
        context["task_instance"].xcom_push("akai_result", result.json_output)
        context["task_instance"].xcom_push("proof_uri", result.proof_uri)
        context["task_instance"].xcom_push("artifact_id", result.artifact_id)
        if not result.ok and getattr(self, "fail_on_error", True):
            raise RuntimeError(f"akai {args[0]} failed (exit {result.exit_code}): {result.json_output}")
        return result


class AurekaiDoctorOperator(AurekaiBaseOperator):
    """Run akai doctor --deep — runtime diagnostics."""
    def __init__(self, *, deep: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.deep = deep

    def execute(self, context: Context):
        args = ["doctor", "--deep"] if self.deep else ["doctor"]
        return self._run(args, context).json_output


class AurekaiRuntimeCapabilitiesOperator(AurekaiBaseOperator):
    """Enumerate all Akai capability families at runtime."""
    def execute(self, context: Context):
        return self._run(["runtime", "capabilities"], context).json_output


class AurekaiManifestVerifyOperator(AurekaiBaseOperator):
    def __init__(self, *, manifest: str = "artifact.json", **kwargs):
        super().__init__(**kwargs)
        self.manifest = manifest

    def execute(self, context: Context):
        return self._run(["verify", "--manifest", self.manifest], context).json_output


class AurekaiQueueWorkOperator(AurekaiBaseOperator):
    def __init__(self, *, queue: str = "default", payload: dict | None = None, **kwargs):
        super().__init__(**kwargs)
        self.queue = queue
        self.payload = payload or {}

    def execute(self, context: Context):
        args = ["queue", "enqueue", "--queue", self.queue]
        if self.payload:
            import tempfile, os
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w")
            json.dump(self.payload, tmp)
            tmp.close()
            args += ["--payload", tmp.name]
        return self._run(args, context).json_output


class AurekaiTranscribeOperator(AurekaiBaseOperator):
    def __init__(self, *, audio_path: str, language: str = "en", **kwargs):
        super().__init__(**kwargs)
        self.audio_path = audio_path
        self.language = language

    def execute(self, context: Context):
        return self._run(
            ["transcribe", "audio", "--input", self.audio_path, "--language", self.language],
            context, timeout=600
        ).json_output


class AurekaiTranscriptCleanOperator(AurekaiBaseOperator):
    def __init__(self, *, transcript_id: str, **kwargs):
        super().__init__(**kwargs)
        self.transcript_id = transcript_id

    def execute(self, context: Context):
        return self._run(["transcript", "clean", "--id", self.transcript_id], context).json_output


class AurekaiBriefOperator(AurekaiBaseOperator):
    def __init__(self, *, artifact_id: str, **kwargs):
        super().__init__(**kwargs)
        self.artifact_id = artifact_id

    def execute(self, context: Context):
        return self._run(["brief", "generate", "--artifact", self.artifact_id], context).json_output


class AurekaiProofBundleOperator(AurekaiBaseOperator):
    def __init__(self, *, run_id: str = "", **kwargs):
        super().__init__(**kwargs)
        self.run_id = run_id

    def execute(self, context: Context):
        run_id = self.run_id or context["run_id"]
        return self._run(["proof", "bundle", "--run-id", run_id], context).json_output


class AurekaiFPQCompressOperator(AurekaiBaseOperator):
    def __init__(self, *, model_tag: str, bits: int = 8, **kwargs):
        super().__init__(**kwargs)
        self.model_tag = model_tag
        self.bits = bits

    def execute(self, context: Context):
        return self._run(
            ["fpq", "compress", "--model", self.model_tag, "--bits", str(self.bits)],
            context, timeout=600
        ).json_output


class AurekaiFPQRoundtripOperator(AurekaiBaseOperator):
    def __init__(self, *, model_tag: str, **kwargs):
        super().__init__(**kwargs)
        self.model_tag = model_tag

    def execute(self, context: Context):
        return self._run(["fpq", "roundtrip", "--model", self.model_tag], context, timeout=900).json_output


class AurekaiFPQxAlignOperator(AurekaiBaseOperator):
    def __init__(self, *, model_tag: str, **kwargs):
        super().__init__(**kwargs)
        self.model_tag = model_tag

    def execute(self, context: Context):
        return self._run(["fpqx", "align", "--model", self.model_tag], context, timeout=600).json_output


class AurekaiSLIAutoRunOperator(AurekaiBaseOperator):
    def execute(self, context: Context):
        return self._run(["sli", "auto-run"], context, timeout=600).json_output


class AurekaiVecSearchOperator(AurekaiBaseOperator):
    def __init__(self, *, query: str, top_k: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.top_k = top_k

    def execute(self, context: Context):
        return self._run(
            ["vec", "search", "--query", self.query, "--top-k", str(self.top_k)],
            context
        ).json_output


class AurekaiGraphLineageOperator(AurekaiBaseOperator):
    def __init__(self, *, artifact_id: str, **kwargs):
        super().__init__(**kwargs)
        self.artifact_id = artifact_id

    def execute(self, context: Context):
        return self._run(["graph", "lineage", "--artifact", self.artifact_id], context).json_output


class AurekaiMeterRecordOperator(AurekaiBaseOperator):
    def __init__(self, *, event: str, units: float = 1.0, client_id: str = "", **kwargs):
        super().__init__(**kwargs)
        self.event = event
        self.units = units
        self.client_id = client_id

    def execute(self, context: Context):
        args = ["meter", "record", "--event", self.event, "--units", str(self.units)]
        if self.client_id:
            args += ["--client", self.client_id]
        return self._run(args, context).json_output


class AurekaiInvoiceOperator(AurekaiBaseOperator):
    def __init__(self, *, client_id: str, period: str = "current", **kwargs):
        super().__init__(**kwargs)
        self.client_id = client_id
        self.period = period

    def execute(self, context: Context):
        return self._run(
            ["pay", "invoice", "--client", self.client_id, "--period", self.period],
            context
        ).json_output


class AurekaiWireReportOperator(AurekaiBaseOperator):
    def __init__(self, *, capture_id: str, **kwargs):
        super().__init__(**kwargs)
        self.capture_id = capture_id

    def execute(self, context: Context):
        return self._run(["wire", "report", "--capture", self.capture_id], context).json_output


class AurekaiReleaseGateOperator(AurekaiBaseOperator):
    def __init__(self, *, version: str, release_tag: str = "", **kwargs):
        super().__init__(**kwargs)
        self.version = version
        self.release_tag = release_tag

    def execute(self, context: Context):
        args = ["release", "gate", "--version", self.version]
        if self.release_tag:
            args += ["--tag", self.release_tag]
        return self._run(args, context).json_output
