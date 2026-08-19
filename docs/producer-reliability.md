# Producer reliability boundary (PR2)

`POST /api/v1/transaction` keeps the existing HMAC headers and canonical
signature. A successful `202` response now means the Kafka delivery callback
completed without an error while the producer used `acks=all`.

The producer polls and then performs one bounded flush within
`KAFKA_PRODUCE_TIMEOUT_SECONDS` (default 5 seconds). Buffer-full errors,
callback errors, undelivered flushes, and timeout all return a controlled HTTP
`503` without broker details or credentials. The accepted payload remains
unchanged.

Replay protection remains an in-process `(producer_id, nonce)` cache. It is
single-instance protection only: it does not coordinate multiple API replicas,
and PR2 does not add Redis or another shared nonce store. Real broker/E2E and
worker acknowledgement semantics remain later scope.
