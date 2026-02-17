"""
OpenTelemetry setup and lightweight metrics helpers.

This module keeps observability optional and safe to import even when
OpenTelemetry dependencies are not installed.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

try:
    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.metrics import Observation
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    metrics = None
    trace = None
    Observation = None
    OTLPMetricExporter = None
    OTLPSpanExporter = None
    MeterProvider = None
    PeriodicExportingMetricReader = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None


def _is_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "true").lower() in {"1", "true", "yes"}


def _otlp_endpoint(signal: str) -> str:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4320").rstrip("/")
    if endpoint.endswith(f"/v1/{signal}"):
        return endpoint
    return f"{endpoint}/v1/{signal}"


def _resource_attributes(service_name: str) -> dict[str, str]:
    return {
        "service.name": os.getenv("OTEL_SERVICE_NAME", service_name),
        "service.namespace": os.getenv("OTEL_SERVICE_NAMESPACE", "open-data-platform"),
        "deployment.environment": os.getenv("ENVIRONMENT", "local"),
        "service.instance.id": os.getenv("HOSTNAME", "local"),
    }


_OBS_CACHE: dict[str, "Observability"] = {}
_OBS_CONFIGURED = False


@dataclass
class PipelineMetrics:
    runs_total: object
    duration_seconds: object
    rows_processed_total: object
    _last_success: Dict[str, float] = field(default_factory=dict)
    _last_failure: Dict[str, float] = field(default_factory=dict)

    def record_success(self, pipeline: str, duration: float, rows: Optional[int]) -> None:
        self.runs_total.add(1, {"pipeline": pipeline, "status": "success"})
        self.duration_seconds.record(duration, {"pipeline": pipeline})
        if rows is not None:
            self.rows_processed_total.add(rows, {"pipeline": pipeline})
        self._last_success[pipeline] = time.time()

    def record_failure(self, pipeline: str, duration: float) -> None:
        self.runs_total.add(1, {"pipeline": pipeline, "status": "failure"})
        self.duration_seconds.record(duration, {"pipeline": pipeline})
        self._last_failure[pipeline] = time.time()


@dataclass
class DataQualityMetrics:
    checks_total: object
    run_duration_seconds: object
    _last_run: Dict[str, float] = field(default_factory=dict)

    def record_check(self, dataset: str, check: str, severity: str, passed: bool) -> None:
        status = "pass" if passed else "fail"
        self.checks_total.add(
            1,
            {
                "dataset": dataset,
                "check": check,
                "severity": severity,
                "status": status,
            },
        )

    def record_dataset_run(self, dataset: str, duration: float) -> None:
        self.run_duration_seconds.record(duration, {"dataset": dataset})
        self._last_run[dataset] = time.time()


@dataclass
class Observability:
    tracer: Optional[object] = None
    meter: Optional[object] = None
    pipeline_metrics: Optional[PipelineMetrics] = None
    data_quality_metrics: Optional[DataQualityMetrics] = None


def get_observability(service_name: str) -> Observability:
    if service_name in _OBS_CACHE:
        return _OBS_CACHE[service_name]

    obs = _configure_observability(service_name)
    _OBS_CACHE[service_name] = obs
    return obs


def _configure_observability(service_name: str) -> Observability:
    if not _is_enabled() or metrics is None or trace is None:
        return Observability()

    global _OBS_CONFIGURED
    if not _OBS_CONFIGURED:
        resource = Resource.create(_resource_attributes(service_name))

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=_otlp_endpoint("traces")))
        )
        trace.set_tracer_provider(tracer_provider)

        export_interval = int(os.getenv("OTEL_METRICS_EXPORT_INTERVAL", "60000"))
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=_otlp_endpoint("metrics")),
            export_interval_millis=export_interval,
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        _OBS_CONFIGURED = True

    tracer = trace.get_tracer(service_name)
    meter = metrics.get_meter(service_name)

    pipeline_metrics = _build_pipeline_metrics(meter)
    data_quality_metrics = _build_data_quality_metrics(meter)

    return Observability(
        tracer=tracer,
        meter=meter,
        pipeline_metrics=pipeline_metrics,
        data_quality_metrics=data_quality_metrics,
    )


def _build_pipeline_metrics(meter: object) -> PipelineMetrics:
    runs_total = meter.create_counter(
        "pipeline_runs_total",
        description="Total pipeline runs",
    )
    duration_seconds = meter.create_histogram(
        "pipeline_duration_seconds",
        unit="s",
        description="Pipeline run duration",
    )
    rows_processed_total = meter.create_counter(
        "pipeline_rows_processed_total",
        unit="rows",
        description="Rows processed by pipelines",
    )
    metrics_obj = PipelineMetrics(
        runs_total=runs_total,
        duration_seconds=duration_seconds,
        rows_processed_total=rows_processed_total,
    )

    if Observation is not None:
        def _success_cb(_options: object) -> Iterable[Observation]:
            return [
                Observation(value=value, attributes={"pipeline": name})
                for name, value in metrics_obj._last_success.items()
            ]

        def _failure_cb(_options: object) -> Iterable[Observation]:
            return [
                Observation(value=value, attributes={"pipeline": name})
                for name, value in metrics_obj._last_failure.items()
            ]

        meter.create_observable_gauge(
            "pipeline_last_success_timestamp_seconds",
            callbacks=[_success_cb],
            description="Unix timestamp of last successful pipeline run",
        )
        meter.create_observable_gauge(
            "pipeline_last_failure_timestamp_seconds",
            callbacks=[_failure_cb],
            description="Unix timestamp of last failed pipeline run",
        )

    return metrics_obj


def _build_data_quality_metrics(meter: object) -> DataQualityMetrics:
    checks_total = meter.create_counter(
        "dq_checks_total",
        description="Total data quality checks",
    )
    run_duration_seconds = meter.create_histogram(
        "dq_run_duration_seconds",
        unit="s",
        description="Data quality run duration",
    )
    metrics_obj = DataQualityMetrics(
        checks_total=checks_total,
        run_duration_seconds=run_duration_seconds,
    )

    if Observation is not None:
        def _dq_last_run_cb(_options: object) -> Iterable[Observation]:
            return [
                Observation(value=value, attributes={"dataset": name})
                for name, value in metrics_obj._last_run.items()
            ]

        meter.create_observable_gauge(
            "dq_last_run_timestamp_seconds",
            callbacks=[_dq_last_run_cb],
            description="Unix timestamp of last data quality run per dataset",
        )

    return metrics_obj
