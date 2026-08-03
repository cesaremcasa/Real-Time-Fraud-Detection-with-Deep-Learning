# Real-Time Fraud Detection with Deep Learning

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![PyTorch](https://img.shields.io/badge/PyTorch-2.0+-ee4c2c.svg)
![Docker](https://img.shields.io/badge/docker-compose-2496ed.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![CUDA](https://img.shields.io/badge/CUDA-enabled-76b900.svg)

## About

Traditional fraud detection systems process transactions in batch mode, often taking hours to identify fraudulent activity. By the time fraud is detected, significant financial damage has already occurred.

This project implements a real-time anomaly detection system for taxi transactions using streaming architecture and deep learning. The system ingests transaction data through a REST API, processes it via a Kafka streaming pipeline (Redpanda), and uses a PyTorch-based Autoencoder running on GPU to detect fraudulent patterns in milliseconds.

**The Problem:** Batch processing systems create detection delays of hours, enabling fraudulent transactions to compound losses before intervention.

**The Solution:** A streaming-first architecture that combines FastAPI for ingestion, Redpanda for message brokering, and GPU-accelerated deep learning inference to detect anomalies with sub-10ms latency.

### How It Works

The system uses an Autoencoder neural network trained on normal transaction patterns. When a new transaction arrives:

- **Normal transactions:** The model reconstructs the data with low reconstruction error
- **Fraudulent transactions:** Abnormal patterns (impossible distances, suspicious pricing) produce high reconstruction error, triggering real-time alerts

**Result:** Average detection latency of **2.5ms** per transaction (50x faster than the 100ms SLA requirement).

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────────┐      ┌──────────────┐
│   FastAPI   │─────>│   Redpanda   │─────>│  PyTorch Worker │─────>│   Grafana    │
│  Producer   │      │   (Kafka)    │      │  (GPU Inference)│      │  Dashboard   │
└─────────────┘      └──────────────┘      └─────────────────┘      └──────────────┘
                                                    │
                                                    v
                                            ┌──────────────┐
                                            │  Prometheus  │
                                            │   Metrics    │
                                            └──────────────┘
```

### Technology Stack

- **API Layer:** FastAPI (async request handling)
- **Message Broker:** Redpanda (Kafka-compatible streaming)
- **ML Framework:** PyTorch with CUDA acceleration
- **Model Architecture:** Autoencoder (anomaly detection via reconstruction error)
- **Monitoring:** Prometheus + Grafana
- **Infrastructure:** Docker Compose, NVIDIA L4 GPU
- **Dataset:** NYC Taxi Trip Records

## Performance Metrics

| Metric | Value | Target |
|--------|-------|--------|
| **Average Latency** | 2.5ms | <100ms |
| **P95 Latency** | 8.7ms | <150ms |
| **Throughput** | 5,000 txn/sec | 1,000 txn/sec |
| **Model Precision** | 94.2% | >90% |
| **False Positive Rate** | 3.1% | <5% |

## Prerequisites

- Docker 24.0+ and Docker Compose
- NVIDIA GPU with CUDA support (recommended: L4 or equivalent)
- Python 3.10+
- 8GB+ available RAM
- NVIDIA Container Toolkit (for GPU acceleration)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/cesaremcasa/Real-Time-Fraud-Detection-with-Deep-Learning.git
cd Real-Time-Fraud-Detection-with-Deep-Learning
```

### 2. Start the Infrastructure

```bash
docker-compose up -d
```

This will spin up:
- Redpanda (Kafka broker)
- FastAPI producer service
- PyTorch worker with GPU inference
- Prometheus and Grafana monitoring stack

### 3. Access the Services

- **API Documentation:** http://localhost:8000/docs
- **Grafana Dashboard:** http://localhost:3000 (credentials from GF_SECURITY_ADMIN_USER and GF_SECURITY_ADMIN_PASSWORD)
- **Prometheus Metrics:** http://localhost:9090

### 4. Send Test Transactions

```bash
curl -X POST http://localhost:8000/transaction \
  -H "Content-Type: application/json" \
  -d '{
    "trip_distance": 2.5,
    "fare_amount": 12.50,
    "duration_minutes": 15
  }'
```

### 5. Monitor Results

View real-time fraud detection metrics in Grafana at http://localhost:3000/d/fraud-detection

## Project Structure

```
Real-Time-Fraud-Detection-with-Deep-Learning/
├── src/
│   ├── api/
│   │   ├── main.py              # FastAPI producer
│   │   └── models.py            # Pydantic schemas
│   ├── ml/
│   │   ├── autoencoder.py       # PyTorch model definition
│   │   ├── inference.py         # GPU inference worker
│   │   └── preprocessor.py      # Feature engineering
│   └── config/
│       └── settings.py          # Configuration management
├── infra/
│   ├── docker-compose.yml       # Infrastructure orchestration
│   ├── prometheus.yml           # Metrics configuration
│   └── grafana/
│       └── dashboards/          # Pre-built dashboards
├── artifacts/
│   ├── model.pth               # Trained Autoencoder weights
│   └── scaler.pkl              # Feature scaling parameters
├── notebooks/
│   └── training.ipynb          # Model training pipeline
├── tests/
│   ├── test_api.py
│   └── test_inference.py
├── requirements.txt
├── Dockerfile
└── README.md
```

## License

MIT License

Copyright (c) 2025 Cesar Augusto

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
