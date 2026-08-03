# Real-Time Fraud Detection with Deep Learning

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka%20API-E14E2A?style=for-the-badge)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-3C9A5F?style=for-the-badge)

**Unsupervised anomaly detection on a streaming pipeline.**

A FastAPI service accepts taxi trip records and publishes them to Redpanda. A separate worker consumes the stream, scores each record with a trained autoencoder on GPU when one is available, and exports Prometheus metrics.

The two processes never talk directly. The broker is the only contract between them, which is what lets the worker restart, fall behind, or run on different hardware without the ingestion path caring.

---

## Why an autoencoder

Labeled fraud is scarce, and the fraud you have labels for is the fraud you already know about. This model is trained only on normal trips and learns to reconstruct them. A trip it reconstructs badly is a trip that does not look like anything it was trained on, which catches shapes nobody wrote a rule for.

The decision threshold is not a guess. It is the 95th percentile of reconstruction error measured over a validation set, written to `artifacts/thresholds.json` at training time and loaded by the worker at startup.

---

## Architecture

```
POST /api/v1/transaction
-> FastAPI producer, validates the payload and returns immediately
-> Redpanda topic transactions_raw
-> Worker, consumer group fraud-worker-group
-> Feature engineering, StandardScaler, autoencoder forward pass
-> Reconstruction error compared against the stored threshold
-> Prometheus metrics, API on 8000 and worker on 8001
```

The API imports no ML code and loads no model. A slow or failing model cannot block ingestion.

---

## What is in this repository

| Path | What it is |
| --- | --- |
| `src/api/main.py` | FastAPI service, Kafka producer, rate limiting, metrics middleware |
| `src/api/models.py` | Request and response models with validators |
| `src/worker/main.py` | Kafka consumer, autoencoder definition, inference loop |
| `src/utils/config.py` | Settings, Kafka producer and consumer configuration |
| `notebooks/01_prep.py` | Data preparation and feature engineering |
| `scripts/train_autoencoder.py` | Trains the autoencoder and the Isolation Forest, writes the artifacts |
| `scripts/check_environment.sh` | GPU and environment check |
| `data/processed_sample.parquet` | Processed sample used for training and validation |
| `artifacts/autoencoder.pt` | Trained checkpoint with weights, dimensions and scaler parameters |
| `artifacts/scaler.pkl` | Fitted StandardScaler over the five online features |
| `artifacts/iforest.pkl` | Trained Isolation Forest |
| `artifacts/thresholds.json` | Decision threshold and the validation statistics behind it |
| `infra/docker-compose.yml` | Redpanda, Prometheus, Grafana, API and worker |
| `infra/prometheus.yml` | Scrape configuration |
| `docker/api.dockerfile` | API image |
| `docker/worker.dockerfile` | Worker image, CUDA base |

---

## The model

Autoencoder, five inputs down to a two dimensional latent space and back:

```
5 -> 32 -> 16 -> 8 -> 2 -> 8 -> 16 -> 32 -> 5
```

Features computed per trip:

- `passenger_count`
- `trip_distance`
- `fare_amount`
- `trip_duration_min`, derived from the pickup and dropoff timestamps
- `fare_per_minute`, derived from the two above


Threshold, read straight from `artifacts/thresholds.json`:

| Value | Number |
| --- | --- |
| Reconstruction error threshold | 0.0172 |
| Mean error on validation | 0.0049 |
| Standard deviation | 0.0464 |
| Percentile used | 95 |
| Validation samples | 10767 |

An Isolation Forest is trained alongside the autoencoder and loaded by the worker. Its score is computed and logged, but it does not yet feed the decision, so the current anomaly flag comes from the autoencoder alone. Wiring the two into one ensemble score is the next step, and it is written here rather than implied by the diagram.

---

## API

`GET /health` returns service status and whether the Kafka producer is connected.

`POST /api/v1/transaction` accepts a trip and queues it for scoring.

```json
{
  "pickup_datetime": "2026-01-15T14:30:00",
  "dropoff_datetime": "2026-01-15T14:45:00",
  "passenger_count": 2,
  "trip_distance": 2.5,
  "fare_amount": 12.50,
  "payment_type": 1,
  "vendor_id": 1
}
```

```json
{
  "status": "accepted",
  "transaction_id": "a3f2c1b8-...",
  "message": "Transaction queued for processing"
}
```

Validation happens at the edge. Dropoff must be after pickup, distance and fare carry bounds, and a fare of zero on a trip longer than 0.1 miles is rejected before anything reaches the broker. Rate limiting is 100 requests per minute, with `/health` exempt.

Interactive documentation at `/docs`.

---

## Metrics

Exposed by the API at `/metrics`:

- `api_requests_total` by method, endpoint and status
- `api_request_duration_seconds`
- `api_active_requests`
- `kafka_messages_sent_total`
- `kafka_produce_errors_total`

Exposed by the worker on port 8001:

- `worker_messages_received_total`
- `worker_messages_processed_total` by outcome
- `worker_inference_latency_seconds`
- `worker_anomalies_detected_total`
- `worker_processing_errors_total` by error type

---

## Running it

Docker with the NVIDIA container runtime if you want the worker on GPU. It falls back to CPU on its own.

```bash
git clone https://github.com/cesaremcasa/Real-Time-Fraud-Detection-with-Deep-Learning.git
cd Real-Time-Fraud-Detection-with-Deep-Learning/infra
docker compose up -d
```

The compose file lives in `infra/`, not at the repository root.

```bash
curl http://localhost:8000/health
```

Prometheus comes up on 9090 and Grafana on 3000. Set `GF_SECURITY_ADMIN_PASSWORD` in your environment before exposing either one beyond localhost.

---

## What is not finished

Stated plainly, because a README that oversells is worse than one that undersells.

- The Isolation Forest is loaded and scored but does not affect the decision yet
- There are no automated tests
- No Grafana dashboards are provisioned, so Grafana starts empty
- The GPU memory gauge is declared and never populated
- There is no published benchmark, which is why no latency or throughput numbers appear anywhere in this file

- ---

## License

MIT. See [LICENSE](LICENSE).

---

**Cesar Augusto** · AI Systems Engineer, Mycellium Lab
[GitHub](https://github.com/cesaremcasa) · [LinkedIn](https://www.linkedin.com/in/cesar-augusto-22943a351/) · [korvo.dev](https://korvo.dev)
