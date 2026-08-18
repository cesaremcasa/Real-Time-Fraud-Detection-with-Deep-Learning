"""Characterize Kafka, metrics, Docker, and Compose wiring without E2E."""

from __future__ import annotations

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_kafka_settings_and_metrics_names_are_frozen():
    config = (ROOT / "src/utils/config.py").read_text(encoding="utf-8")
    api = (ROOT / "src/api/main.py").read_text(encoding="utf-8")
    worker = (ROOT / "src/worker/main.py").read_text(encoding="utf-8")
    assert 'KAFKA_TOPIC_TRANSACTIONS_RAW: str = "transactions_raw"' in config
    assert 'WORKER_CONSUMER_GROUP: str = "fraud-worker-group"' in config
    assert "'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS" in config
    assert "'group.id': group_id" in config
    for name in (
        "api_requests_total",
        "api_request_duration_seconds",
        "api_active_requests",
        "kafka_messages_sent_total",
        "kafka_produce_errors_total",
    ):
        assert name in api
    for name in (
        "worker_messages_received_total",
        "worker_messages_processed_total",
        "worker_inference_latency_seconds",
        "worker_anomalies_detected_total",
        "worker_processing_errors_total",
    ):
        assert name in worker


def test_compose_services_ports_volumes_and_environment_are_characterized():
    yaml = pytest.importorskip("yaml")
    compose = yaml.safe_load((ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8"))
    local = yaml.safe_load((ROOT / "infra/docker-compose.local.yml").read_text(encoding="utf-8"))
    assert set(compose["services"]) == {"redpanda", "prometheus", "grafana", "api-producer", "ml-worker"}
    assert compose["services"]["redpanda"]["command"][-1] == "--advertise-kafka-addr PLAINTEXT://redpanda:29092"
    assert "redpanda_data:/var/lib/redpanda/data" in compose["services"]["redpanda"]["volumes"]
    assert "grafana_data:/var/lib/grafana" in compose["services"]["grafana"]["volumes"]
    assert any("FRAUD_PRODUCER_SECRETS_JSON" in value for value in compose["services"]["api-producer"]["environment"])
    assert compose["services"]["api-producer"]["healthcheck"]["test"][-1].endswith("/health")
    assert local["services"]["api-producer"]["ports"] == ["127.0.0.1:8000:8000"]
    assert local["services"]["prometheus"]["ports"] == ["127.0.0.1:9090:9090"]
    assert local["services"]["grafana"]["ports"] == ["127.0.0.1:3000:3000"]


def test_docker_entrypoints_and_current_known_mismatches_are_visible():
    api = (ROOT / "docker/api.dockerfile").read_text(encoding="utf-8")
    worker = (ROOT / "docker/worker.dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.10-slim" in api
    assert 'CMD ["uvicorn", "src.api.main:app"' in api
    assert "FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime" in worker
    # Characterization, not endorsement: this entrypoint currently names a
    # module absent from the repository and is a follow-up packaging finding.
    assert 'CMD ["python", "-m", "src.ml_worker"]' in worker
