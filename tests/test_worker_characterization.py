"""PR1 worker/model characterization with synthetic data and fakes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.fakes import FakeKafkaMessage, FakeResultProducer, FakeScaler


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "synthetic_transaction.json"
KAFKA_FIXTURE = ROOT / "tests" / "fixtures" / "synthetic_kafka_message.json"


def _worker_or_skip():
    for dependency in (
        "torch",
        "numpy",
        "pandas",
        "confluent_kafka",
        "structlog",
        "joblib",
        "prometheus_client",
        "pydantic_settings",
    ):
        pytest.importorskip(dependency)
    import src.worker.main as worker

    return worker


def test_worker_runtime_contract_and_artifact_boundary():
    source = (ROOT / "src/worker/main.py").read_text(encoding="utf-8")
    assert "class Autoencoder" in source
    assert "def create_features_online" in source
    assert "def load_artifacts" in source
    assert "start_http_server" in source
    assert "settings.KAFKA_TOPIC_TRANSACTIONS_RAW" in source
    assert "worker_messages_processed_total" in source
    assert (ROOT / "artifacts/autoencoder.pt").is_file()
    assert (ROOT / "artifacts/scaler.pkl").is_file()
    assert (ROOT / "artifacts/iforest.pkl").is_file()
    thresholds = json.loads((ROOT / "artifacts/thresholds.json").read_text(encoding="utf-8"))
    assert thresholds["autoencoder_mse_threshold"] > 0
    assert thresholds["percentile_used"] == 95


def test_feature_engineering_shape_and_values_with_synthetic_fixture():
    worker = _worker_or_skip()
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    features = worker.create_features_online(payload)
    assert features is not None
    assert features.shape == (1, 5)
    assert features[0].tolist() == pytest.approx([2.0, 2.5, 12.5, 15.0, 12.5 / 15.0])
    assert worker.create_features_online({**payload, "dropoff_datetime": "2026-01-15T14:29:00"}) is None


def test_autoencoder_contract_is_five_to_two_to_five():
    worker = _worker_or_skip()
    torch = pytest.importorskip("torch")
    model = worker.Autoencoder(input_dim=5, latent_dim=2)
    reconstructed, latent = model(torch.zeros((1, 5)))
    assert tuple(reconstructed.shape) == (1, 5)
    assert tuple(latent.shape) == (1, 2)


def test_worker_message_boundary_uses_fake_kafka_and_fake_model(monkeypatch: pytest.MonkeyPatch):
    worker = _worker_or_skip()
    envelope = json.loads(KAFKA_FIXTURE.read_text(encoding="utf-8"))
    assert envelope["topic"] == "transactions_raw"
    payload = envelope["value"]
    instance = worker.FraudDetectionWorker()
    instance.scaler = FakeScaler()
    instance.producer = FakeResultProducer()
    instance.iforest = None
    instance.thresholds = {"autoencoder_mse_threshold": 0.1}
    instance.model = object()
    instance.device = "cpu"
    monkeypatch.setattr(
        worker,
        "predict",
        lambda *args, **kwargs: (False, {"autoencoder_mse": 0.01}, 0.001),
    )

    assert instance.process_message(FakeKafkaMessage(payload)) is True
