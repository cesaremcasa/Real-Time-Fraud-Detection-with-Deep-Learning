"""
Configurações centralizadas para o projeto.
Usa Pydantic Settings para gerenciamento de variáveis de ambiente.
"""

from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any
import os
import json

class Settings(BaseSettings):
    # API Config
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 1
    API_DEBUG: bool = False
    
    # Kafka/Redpanda Config
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TRANSACTIONS_RAW: str = "transactions_raw"
    KAFKA_TOPIC_FRAUD_PREDICTIONS: str = "fraud_predictions"
    KAFKA_ACKS: str = "all"  # PR2 acceptance requires all replicas to ack.
    KAFKA_MAX_IN_FLIGHT: int = 5
    KAFKA_PRODUCE_TIMEOUT_SECONDS: float = 5.0
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 100

    # External ingestion is authenticated with one HMAC secret per producer.
    # This intentionally has no insecure default.
    FRAUD_PRODUCER_SECRETS_JSON: str = ""
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_JSON_FORMAT: bool = True
    
    # Monitoring
    PROMETHEUS_ENABLED: bool = True
    
    # Worker Config
    WORKER_METRICS_PORT: int = 8001
    WORKER_CONSUMER_GROUP: str = "fraud-worker-group"
    WORKER_POLL_TIMEOUT: float = 1.0
    WORKER_HEALTH_CHECK_INTERVAL: int = 30
    WORKER_MAX_MESSAGES: int = 0  # 0 means run indefinitely.
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignorar variáveis extras no .env

# Instância global de configurações
settings = Settings()


def get_producer_secrets() -> dict[str, str]:
    """Return the configured producer-id -> HMAC-secret mapping."""
    try:
        parsed = json.loads(settings.FRAUD_PRODUCER_SECRETS_JSON)
    except json.JSONDecodeError as exc:
        raise RuntimeError("FRAUD_PRODUCER_SECRETS_JSON must be a JSON object") from exc
    if not isinstance(parsed, dict) or not parsed or any(not isinstance(key, str) or not isinstance(value, str) or not value for key, value in parsed.items()):
        raise RuntimeError("FRAUD_PRODUCER_SECRETS_JSON must contain at least one producer ID and secret")
    return parsed

# Helper para configurações do Kafka Producer
def get_kafka_producer_config() -> dict:
    """Retorna configurações para o produtor Kafka."""
    return {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        # Never let an environment override weaken broker acknowledgement.
        'acks': 'all',
        'max.in.flight.requests.per.connection': settings.KAFKA_MAX_IN_FLIGHT,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 100,  # 100ms de buffer
        'batch.num.messages': 10000,
        'compression.type': 'none',  # 'snappy', 'gzip', 'lz4'
        'message.timeout.ms': 30000,  # 30 segundos
        'client.id': 'fraud-detection-api',
        'enable.idempotence': False,  # Desabilitado para performance
    }

def get_kafka_consumer_config(group_id: Optional[str] = None) -> dict:
    """Retorna configurações para o consumidor Kafka."""
    if group_id is None:
        group_id = settings.WORKER_CONSUMER_GROUP
    
    return {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'auto.commit.interval.ms': 5000,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 10000,
        'fetch.wait.max.ms': 500,
    }

def get_worker_config() -> Dict[str, Any]:
    """Retorna configurações específicas do worker."""
    return {
        'metrics_port': settings.WORKER_METRICS_PORT,
        'consumer_group': settings.WORKER_CONSUMER_GROUP,
        'poll_timeout': settings.WORKER_POLL_TIMEOUT,
        'health_check_interval': settings.WORKER_HEALTH_CHECK_INTERVAL,
        'max_messages': settings.WORKER_MAX_MESSAGES,
    }
