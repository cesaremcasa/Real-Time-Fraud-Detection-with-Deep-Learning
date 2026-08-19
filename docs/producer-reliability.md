# Producer reliability boundary (PR2)

`POST /api/v1/transaction` keeps the existing HMAC headers and canonical
signature. A successful `202` response now means the Kafka delivery callback
completed without an error while the producer used `acks=all`.

The producer polls and then performs one bounded flush within
`KAFKA_PRODUCE_TIMEOUT_SECONDS` (default 5 seconds). Buffer-full errors,
callback errors, undelivered flushes, and timeout all return a controlled HTTP
`503` without broker details or credentials. The accepted payload remains
unchanged.

The result topic is `fraud_predictions`. A result is JSON with exactly these
fields: `transaction_id`, `request_id`, `is_anomaly` (boolean), `scores`,
`inference_time_seconds`, and `processed_at`. It intentionally excludes the
raw transaction payload. The worker commits the consumed raw offset only
after this result delivery callback succeeds.

Replay protection remains an in-process `(producer_id, nonce)` cache. It is
single-instance protection only: it does not coordinate multiple API replicas,
and PR2 does not add Redis or another shared nonce store. The E2E harness uses
only the pinned localhost Redpanda image; Linux/amd64 CI is authoritative when
ARM emulation exits early. No cloud, production, or release environment is
used.
