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
    assert "@sha256:632ee5289856bd0d56612f8ab13266851834552ceb37511eea9f2401e3d4616f" in compose["services"]["redpanda"]["image"]
    assert "@sha256:b1935d181b6dd8e9c827705e89438815337e1b10ae35605126f05f44e5c6940f" in compose["services"]["prometheus"]["image"]
    assert "@sha256:8d938a1c52b018c60cb3583657e038054387aa18a74f09a865c99a522481f7ac" in compose["services"]["grafana"]["image"]
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
    assert "FROM python:3.11.11-slim@sha256:a8e0a3090316aed0b11037aac613aef32fb1747dcc1dcb5c0f6c727a0113a07f" in api
    assert 'CMD ["uvicorn", "src.api.main:app"' in api
    assert "FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime@sha256:77f17f843507062875ce8be2a6f76aa6aa3df7f9ef1e31d9d7432f4b0f563dee" in worker
    # Characterization, not endorsement: this entrypoint currently names a
    # module absent from the repository and is a follow-up packaging finding.
    assert 'CMD ["python", "-m", "src.ml_worker"]' in worker
