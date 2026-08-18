# Data and model provenance

This repository contains two kinds of data at the PR1 baseline:

1. `tests/fixtures/` contains tiny deterministic transaction/Kafka envelopes
   generated for characterization tests. They contain no real people,
   merchants, providers, or production events. They are dedicated to the
   public domain under CC0 1.0; see `tests/fixtures/README.md`.
2. `data/processed_sample.parquet` and `artifacts/` are pre-existing tracked
   baseline outputs from the upstream repository. Their training source is
   represented by `notebooks/01_prep.py` and `scripts/train_autoencoder.py`;
   PR1 does not retrain or claim a reproducible model run. Exact SHA-256 values
   are recorded in `artifacts/SHA256SUMS` and below.

## Reproduction and verification

The fixture hashes were generated with:

```bash
sha256sum tests/fixtures/synthetic_transaction.json \
  tests/fixtures/synthetic_kafka_message.json
```

The baseline artifact hashes were generated with:

```bash
sha256sum artifacts/autoencoder.pt artifacts/iforest.pkl \
  artifacts/scaler.pkl artifacts/thresholds.json
```

`scripts/check_provenance.py` verifies both manifests without downloading
data or contacting a broker. It is a provenance-integrity check, not a claim
that the historical training environment is fully reconstructible.

## License and scope

The repository code is MIT-licensed (`LICENSE`). Synthetic fixtures are CC0
1.0. No customer or production data may be added to this tree. Real Redpanda
E2E, acknowledgement/replay semantics, and model retraining belong to later
portfolio phases and are deliberately not represented by PR1 tests.
