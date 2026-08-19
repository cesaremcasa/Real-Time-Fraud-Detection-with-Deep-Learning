from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.security import ProducerAuthenticator


class FakeKafkaMessage:
    def topic(self):
        return "transactions_raw"

    def partition(self):
        return 0

    def offset(self):
        return 17


class FakeDeliveryError:
    def __str__(self):
        return "synthetic broker delivery error"


class FakeProducer:
    def __init__(self, mode: str):
        self.mode = mode
        self.callback = None
        self.polls = 0
        self.flushes = 0

    def produce(self, *, topic, value, key, callback):
        self.callback = callback
        if self.mode == "buffer":
            raise BufferError("synthetic full")

    def poll(self, _timeout):
        self.polls += 1
        if self.callback is None:
            return
        if self.mode == "immediate" or (self.mode == "delayed" and self.polls >= 3):
            callback, self.callback = self.callback, None
            callback(None, FakeKafkaMessage())
        elif self.mode == "error":
            callback, self.callback = self.callback, None
            callback(FakeDeliveryError(), FakeKafkaMessage())

    def flush(self, _timeout):
        self.flushes += 1
        if self.mode == "flush-undelivered":
            return 1
        return 0


def _wrapper(monkeypatch: pytest.MonkeyPatch, mode: str):
    pytest.importorskip("fastapi")
    import src.api.main as module

    wrapper = module.KafkaProducerWrapper()
    fake = FakeProducer(mode)
    wrapper._producer = fake
    wrapper._connected = True
    monkeypatch.setattr(module.settings, "KAFKA_PRODUCE_TIMEOUT_SECONDS", 0.03)
    return wrapper, fake


@pytest.mark.parametrize("mode", ["immediate", "delayed"])
def test_producer_accepts_only_after_delivery_ack(monkeypatch: pytest.MonkeyPatch, mode: str):
    wrapper, fake = _wrapper(monkeypatch, mode)
    assert wrapper.produce("transactions_raw", "{}", "tx-1") is True
    assert fake.polls >= (3 if mode == "delayed" else 1)


def test_callback_error_is_controlled_failure(monkeypatch: pytest.MonkeyPatch):
    wrapper, _fake = _wrapper(monkeypatch, "error")
    assert wrapper.produce("transactions_raw", "{}", "tx-2") is False


def test_timeout_and_undelivered_flush_are_controlled_failure(monkeypatch: pytest.MonkeyPatch):
    wrapper, fake = _wrapper(monkeypatch, "flush-undelivered")
    assert wrapper.produce("transactions_raw", "{}", "tx-3", timeout_seconds=0.001) is False
    assert fake.flushes >= 1


def test_buffer_full_is_controlled_failure(monkeypatch: pytest.MonkeyPatch):
    wrapper, _fake = _wrapper(monkeypatch, "buffer")
    assert wrapper.produce("transactions_raw", "{}", "tx-4") is False


def test_config_forces_all_replica_ack(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi")
    from src.utils.config import get_kafka_producer_config

    monkeypatch.setattr("src.utils.config.settings.KAFKA_ACKS", "1")
    assert get_kafka_producer_config()["acks"] == "all"


def _signed_headers(secret: str, body: bytes, nonce: str) -> dict[str, str]:
    timestamp = str(int(time.time()))
    canonical = "\n".join(
        ("POST", "/api/v1/transaction", "fixture", timestamp, nonce, hashlib.sha256(body).hexdigest())
    )
    return {
        "X-Producer-Id": "fixture",
        "X-Request-Timestamp": timestamp,
        "X-Request-Nonce": nonce,
        "X-Request-Signature": hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest(),
    }


def test_endpoint_returns_503_without_ack_and_no_sensitive_detail(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    import src.api.main as module

    module.settings.FRAUD_PRODUCER_SECRETS_JSON = '{"fixture":"fixture-secret"}'
    module.producer_authenticator = ProducerAuthenticator({"fixture": "fixture-secret"})
    monkeypatch.setattr(module.kafka_producer, "produce", lambda **_kwargs: False)
    monkeypatch.setattr(module.kafka_producer, "_connect", lambda: setattr(module.kafka_producer, "_connected", True))
    body = json.dumps(
        {
            "pickup_datetime": "2026-01-15T14:30:00",
            "dropoff_datetime": "2026-01-15T14:45:00",
            "passenger_count": 1,
            "trip_distance": 1.0,
            "fare_amount": 5.0,
        },
        separators=(",", ":"),
    ).encode()
    headers = _signed_headers("fixture-secret", body, "ack-endpoint-nonce-001")
    with TestClient(module.app) as client:
        response = client.post("/api/v1/transaction", content=body, headers=headers)
    assert response.status_code == 503
    assert response.json()["message"] == "Transaction queue temporarily unavailable"
    assert "fixture-secret" not in response.text
