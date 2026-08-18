"""PR1 characterization of the existing producer API with a fake broker."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from pathlib import Path

import pytest

from tests.fakes import FakeKafkaProducer


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "synthetic_transaction.json"


def _signature(*, secret: str, producer_id: str, timestamp: str, nonce: str, body: bytes) -> str:
    canonical = "\n".join(
        (
            "POST",
            "/api/v1/transaction",
            producer_id,
            timestamp,
            nonce,
            hashlib.sha256(body).hexdigest(),
        )
    )
    return hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()


def _api_dependencies_or_skip():
    for dependency in ("fastapi", "confluent_kafka", "slowapi", "structlog", "pydantic_settings", "httpx"):
        pytest.importorskip(dependency)
    from fastapi.testclient import TestClient

    return TestClient


def test_route_and_header_names_are_explicitly_characterized():
    source = (ROOT / "src/api/main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/v1/transaction"' in source
    assert 'request.headers.get("X-Producer-Id")' in source
    assert 'request.headers.get("X-Request-Timestamp")' in source
    assert 'request.headers.get("X-Request-Nonce")' in source
    assert 'request.headers.get("X-Request-Signature")' in source
    assert 'topic=settings.KAFKA_TOPIC_TRANSACTIONS_RAW' in source


def test_producer_endpoint_accepts_signed_fixture_with_fake_broker(monkeypatch: pytest.MonkeyPatch):
    TestClient = _api_dependencies_or_skip()
    monkeypatch.setenv("FRAUD_PRODUCER_SECRETS_JSON", '{"fixture-producer":"fixture-secret"}')

    import src.api.main as api

    fake = FakeKafkaProducer()
    monkeypatch.setattr(api.kafka_producer, "produce", fake.produce)
    monkeypatch.setattr(
        api.kafka_producer,
        "_connect",
        lambda: setattr(api.kafka_producer, "_connected", True),
    )

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    timestamp = str(int(time.time()))
    nonce = "fixture-nonce-00000001"
    headers = {
        "Content-Type": "application/json",
        "X-Producer-Id": "fixture-producer",
        "X-Request-Timestamp": timestamp,
        "X-Request-Nonce": nonce,
        "X-Request-Signature": _signature(
            secret="fixture-secret",
            producer_id="fixture-producer",
            timestamp=timestamp,
            nonce=nonce,
            body=body,
        ),
    }

    with TestClient(api.app) as client:
        response = client.post("/api/v1/transaction", content=body, headers=headers)
        health = client.get("/health")

    # Characterization: current decorator has no explicit status_code, so this
    # is 200 despite the endpoint docstring describing 202 Accepted.
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "accepted"
    assert result["transaction_id"] == payload["transaction_id"]
    assert health.status_code == 200
    assert health.json()["status"] == "healthy"
    assert health.json()["kafka_connected"] is True
    assert len(fake.messages) == 1
    assert fake.messages[0]["topic"] == "transactions_raw"
    assert fake.messages[0]["key"] == payload["transaction_id"]
    assert json.loads(fake.messages[0]["value"])["fare_amount"] == 12.5


def test_producer_endpoint_requires_all_hmac_headers(monkeypatch: pytest.MonkeyPatch):
    TestClient = _api_dependencies_or_skip()
    monkeypatch.setenv("FRAUD_PRODUCER_SECRETS_JSON", '{"fixture-producer":"fixture-secret"}')

    import src.api.main as api

    fake = FakeKafkaProducer()
    monkeypatch.setattr(api.kafka_producer, "produce", fake.produce)
    monkeypatch.setattr(
        api.kafka_producer,
        "_connect",
        lambda: setattr(api.kafka_producer, "_connected", True),
    )
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    with TestClient(api.app) as client:
        response = client.post("/api/v1/transaction", json=payload)

    assert response.status_code == 401
    assert response.json()["status"] == "error"
    assert fake.messages == []


def test_transaction_schema_boundary_is_preserved():
    pytest.importorskip("pydantic")
    from pydantic import ValidationError
    from src.api.models import TransactionRequest

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    request = TransactionRequest(**payload)
    assert request.passenger_count == 2
    assert request.dropoff_datetime.isoformat().startswith("2026-01-15T14:45:00")
    with pytest.raises(ValidationError):
        TransactionRequest(**{**payload, "dropoff_datetime": "2026-01-15T14:29:00"})
    with pytest.raises(ValidationError):
        TransactionRequest(**{**payload, "fare_amount": 0})
