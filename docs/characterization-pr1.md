# PR1 characterization record

This document freezes the behavior observed at baseline commit
`92ffc170d1d9156f7a6bf457e151c6aa5b70761e`. The tests in `tests/` use a small
synthetic transaction fixture and fake Kafka/model boundaries; they do not
claim a live broker, model serving deployment, Docker startup, or end-to-end
flow.

## Preserved contracts

- Producer route: `POST /api/v1/transaction`.
- HMAC headers: `X-Producer-Id`, `X-Request-Timestamp`, `X-Request-Nonce`, and
  `X-Request-Signature`.
- Signature canonicalization: uppercase method, path, producer ID, timestamp,
  nonce, and SHA-256 body digest separated by newlines; `sha256=` prefixes are
  accepted by the verifier.
- Health route: `GET /health`, exposing `status`, `kafka_connected`, `version`,
  and `timestamp`.
- Kafka topic and consumer group: `transactions_raw` and
  `fraud-worker-group` by default.
- Worker feature vector: five values ending in duration and fare-per-minute;
  the autoencoder is characterized as 5 → 32 → 16 → 8 → 2 → 8 → 16 → 32 → 5.
- API and worker Prometheus metric names, Compose service names, local-only
  ports, read-only data/artifact mounts, and fake message/model boundaries.

## Known baseline differences/failures

- The API decorator has no explicit `status_code`, so the signed fixture
  currently returns HTTP 200 even though the function docstring says “202
  Accepted”.
- `docker/worker.dockerfile` currently invokes `src.ml_worker`, but the source
  tree contains `src/worker/main.py`; PR1 records this rather than repairing
  runtime behavior.
- Compose currently exports `REDPANDA_BROKERS` and `KAFKA_TOPIC` while the
  Python settings read `KAFKA_BOOTSTRAP_SERVERS` and
  `KAFKA_TOPIC_TRANSACTIONS_RAW`. This mismatch is intentionally characterized.
- Worker consumption has `enable.auto.commit=True` and its explicit commit is
  commented out. Acknowledgement, replay, and real Redpanda E2E behavior are
  PR2/PR3 scope and are not inferred from these tests.
- The baseline had only HMAC helper tests; these additions are contract tests,
  not a production E2E suite.
