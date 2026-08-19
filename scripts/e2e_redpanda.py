#!/usr/bin/env python3
"""Real Redpanda/API/worker E2E proof for PR2.

This is intentionally opt-in and uses only localhost, synthetic credentials,
and checked-in model artifacts. Every started process/container is cleaned up
on success or failure.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "infra" / "docker-compose.e2e.yml"
COMPOSE_PROJECT = "fraud-pr2-e2e"
BROKERS = "127.0.0.1:19092"
RAW_TOPIC = "transactions_raw"
RESULT_TOPIC = "fraud_predictions"
API_URL = "http://127.0.0.1:18000"
METRICS_URL = "http://127.0.0.1:18001/metrics"
SIGNING_MATERIAL = hashlib.sha256(b"public PR2 E2E fixture signing material").hexdigest()
PRODUCER_ID = "e2e-producer"


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(command, cwd=ROOT, check=check, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stdout or "") + (exc.stderr or "")
        raise RuntimeError(f"command failed: {' '.join(command)}\n{detail}") from exc


def _http(url: str, *, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None):
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    return urllib.request.urlopen(request, timeout=5)


def _wait_http(url: str, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with _http(url) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError) as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {url}: {last_error}")


def _admin_topics() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    client = AdminClient({"bootstrap.servers": BROKERS})
    futures = client.create_topics(
        [NewTopic(RAW_TOPIC, num_partitions=1, replication_factor=1), NewTopic(RESULT_TOPIC, 1, 1)]
    )
    for future in futures.values():
        try:
            future.result(15)
        except Exception as exc:  # already-exists is a deterministic rerun success
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def _signed_body() -> tuple[bytes, str, str]:
    transaction_id = f"e2e-{uuid.uuid4()}"
    payload = {
        "transaction_id": transaction_id,
        "pickup_datetime": "2026-01-15T14:30:00",
        "dropoff_datetime": "2026-01-15T14:45:00",
        "passenger_count": 2,
        "trip_distance": 2.5,
        "fare_amount": 12.5,
        "payment_type": 1,
        "vendor_id": 1,
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    nonce = f"e2e-nonce-{uuid.uuid4()}"
    canonical = "\n".join(
        ("POST", "/api/v1/transaction", PRODUCER_ID, timestamp, nonce, hashlib.sha256(body).hexdigest())
    )
    signature = hmac.new(SIGNING_MATERIAL.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return body, transaction_id, json.dumps(
        {
            "Content-Type": "application/json",
            "X-Producer-Id": PRODUCER_ID,
            "X-Request-Timestamp": timestamp,
            "X-Request-Nonce": nonce,
            "X-Request-Signature": signature,
        }
    )


def _consume_result(transaction_id: str, timeout: float = 60.0) -> dict:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": f"e2e-result-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([RESULT_TOPIC])
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            result = json.loads(message.value().decode())
            if result.get("transaction_id") == transaction_id:
                return result
        raise RuntimeError("timed out consuming fraud result")
    finally:
        consumer.close()


def run() -> None:
    api = worker = None
    compose_started = False
    try:
        compose_started = True
        _run(["docker", "compose", "-p", COMPOSE_PROJECT, "-f", str(COMPOSE), "up", "-d", "--wait"])
        # AdminClient readiness is the broker wait; no external service is used.
        deadline = time.monotonic() + 45
        while True:
            try:
                _admin_topics()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

        env = os.environ.copy()
        env.update(
            {
                "FRAUD_PRODUCER_SECRETS_JSON": json.dumps({PRODUCER_ID: SIGNING_MATERIAL}),
                "KAFKA_BOOTSTRAP_SERVERS": BROKERS,
                "KAFKA_TOPIC_TRANSACTIONS_RAW": RAW_TOPIC,
                "KAFKA_TOPIC_FRAUD_PREDICTIONS": RESULT_TOPIC,
                "KAFKA_PRODUCE_TIMEOUT_SECONDS": "10",
                "PYTHONUNBUFFERED": "1",
                "WORKER_METRICS_PORT": "18001",
            }
        )
        api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "src.api.main:app", "--host", "127.0.0.1", "--port", "18000"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        _wait_http(f"{API_URL}/health")
        worker_code = (
            "import runpy,sys,time; "
            "sys.argv=['src.worker.main','--max-messages','1']; "
            "runpy.run_module('src.worker.main', run_name='__main__'); time.sleep(10)"
        )
        worker = subprocess.Popen(
            [sys.executable, "-c", worker_code],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        body, transaction_id, header_json = _signed_body()
        headers = json.loads(header_json)
        with _http(f"{API_URL}/api/v1/transaction", method="POST", body=body, headers=headers) as response:
            response_body = response.read().decode()
            assert response.status == 202, response_body
            accepted = json.loads(response_body)
        assert accepted["transaction_id"] == transaction_id
        result = _consume_result(transaction_id)
        assert result["transaction_id"] == transaction_id
        assert result["request_id"]
        assert isinstance(result["is_anomaly"], bool)
        assert set(result) == {
            "transaction_id",
            "request_id",
            "is_anomaly",
            "scores",
            "inference_time_seconds",
            "processed_at",
        }
        _wait_http(METRICS_URL, timeout=10)
        with _http(METRICS_URL) as response:
            metrics = response.read().decode()
        assert "worker_messages_processed_total" in metrics
        assert "worker_results_published_total" in metrics
        assert worker.wait(timeout=20) == 0
        print("E2E PASS: signed transaction -> API 202 -> acknowledged fraud result -> worker metrics")
    finally:
        for process in (worker, api):
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
        if compose_started:
            _run(["docker", "compose", "-p", COMPOSE_PROJECT, "-f", str(COMPOSE), "down", "-v", "--remove-orphans"], check=False)


if __name__ == "__main__":
    run()
