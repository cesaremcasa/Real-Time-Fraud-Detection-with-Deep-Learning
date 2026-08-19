from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _worker_module():
    for dependency in ("torch", "numpy", "pandas", "confluent_kafka", "structlog", "joblib"):
        pytest.importorskip(dependency)
    import src.worker.main as worker

    return worker


class FakeResultProducer:
    def __init__(self, mode: str = "ack"):
        self.mode = mode
        self.callback = None
        self.payloads: list[dict] = []
        self.flushes = 0

    def produce(self, *, topic, key, value, callback):
        self.topic = topic
        self.key = key
        self.payloads.append(json.loads(value))
        self.callback = callback

    def poll(self, _timeout):
        if self.callback is None:
            return
        callback, self.callback = self.callback, None
        callback(None if self.mode == "ack" else RuntimeError("result delivery failed"), object())

    def flush(self, _timeout):
        self.flushes += 1
        return 0


class FakeMessage:
    def __init__(self, payload: dict):
        self._value = json.dumps(payload).encode()

    def value(self):
        return self._value

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.commits: list[tuple[object, bool]] = []
        self.closed = False

    def poll(self, _timeout):
        if self.messages:
            return self.messages.pop(0)
        raise KeyboardInterrupt

    def commit(self, *, message, asynchronous):
        self.commits.append((message, asynchronous))

    def close(self):
        self.closed = True


def _payload() -> dict:
    return {
        "transaction_id": "tx-1",
        "request_id": "req-1",
        "pickup_datetime": "2026-01-15T14:30:00",
        "dropoff_datetime": "2026-01-15T14:45:00",
        "passenger_count": 2,
        "trip_distance": 2.5,
        "fare_amount": 12.5,
    }


@pytest.mark.parametrize("is_anomaly", [False, True])
def test_result_schema_is_compact_for_normal_and_anomaly(monkeypatch: pytest.MonkeyPatch, is_anomaly: bool):
    worker = _worker_module()
    instance = worker.FraudDetectionWorker()
    producer = FakeResultProducer()
    instance.producer = producer
    monkeypatch.setattr(worker, "predict", lambda *args: (is_anomaly, {"autoencoder_mse": 0.2}, 0.01))

    assert instance.process_message(FakeMessage(_payload())) is True
    result = producer.payloads[0]
    assert set(result) == {
        "transaction_id",
        "request_id",
        "is_anomaly",
        "scores",
        "inference_time_seconds",
        "processed_at",
    }
    assert result["transaction_id"] == "tx-1"
    assert result["is_anomaly"] is is_anomaly
    assert "passenger_count" not in json.dumps(result)
    assert producer.topic == "fraud_predictions"


def test_publish_error_returns_failure_and_run_does_not_commit(monkeypatch: pytest.MonkeyPatch):
    worker = _worker_module()
    instance = worker.FraudDetectionWorker(max_messages=1)
    instance.producer = FakeResultProducer(mode="error")
    consumer = FakeConsumer([FakeMessage(_payload())])
    instance.consumer = consumer
    monkeypatch.setattr(worker, "predict", lambda *args: (False, {"autoencoder_mse": 0.01}, 0.01))

    instance.run()
    assert consumer.commits == []
    assert consumer.closed is True


def test_failure_on_message_a_fail_stops_before_queued_message_b(monkeypatch: pytest.MonkeyPatch):
    worker = _worker_module()
    instance = worker.FraudDetectionWorker()
    instance.producer = FakeResultProducer(mode="error")
    message_a = FakeMessage(_payload())
    message_b = FakeMessage({**_payload(), "transaction_id": "tx-b"})
    consumer = FakeConsumer([message_a, message_b])
    instance.consumer = consumer
    processed: list[str] = []

    def fail_a(raw, *_args):
        processed.append(raw["transaction_id"])
        return False, {"autoencoder_mse": 0.01}, 0.01

    monkeypatch.setattr(worker, "predict", fail_a)
    instance.run()
    assert processed == ["tx-1"]
    assert consumer.commits == []
    assert consumer.messages == [message_b]


def test_commit_happens_only_after_result_ack_and_max_messages_exits(monkeypatch: pytest.MonkeyPatch):
    worker = _worker_module()
    instance = worker.FraudDetectionWorker(max_messages=1)
    producer = FakeResultProducer(mode="ack")
    instance.producer = producer
    message = FakeMessage(_payload())
    consumer = FakeConsumer([message])
    instance.consumer = consumer
    monkeypatch.setattr(worker, "predict", lambda *args: (False, {"autoencoder_mse": 0.01}, 0.01))

    instance.run()
    assert len(consumer.commits) == 1
    assert consumer.commits[0] == (message, False)
    assert consumer.closed is True


def test_shutdown_closes_consumer_and_flushes_result_producer():
    worker = _worker_module()
    instance = worker.FraudDetectionWorker()
    producer = FakeResultProducer()
    consumer = FakeConsumer([])
    instance.producer = producer
    instance.consumer = consumer

    instance.shutdown()
    assert producer.flushes == 1
    assert consumer.closed is True


def test_consumer_config_disables_auto_commit_and_max_messages_is_configured():
    pytest.importorskip("pydantic_settings")
    from src.utils.config import get_kafka_consumer_config, settings

    assert get_kafka_consumer_config()["enable.auto.commit"] is False
    assert get_kafka_consumer_config()["enable.auto.offset.store"] is False
    assert settings.WORKER_MAX_MESSAGES == 0
