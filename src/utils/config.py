"""
Configurações centralizadas para o projeto.
Usa Pydantic Settings para gerenciamento de variáveis de ambiente.
"""

from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any
import os

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
    KAFKA_ACKS: str = "1"  # "0": não espera, "1": leader, "all": todos replicas
    KAFKA_MAX_IN_FLIGHT: int = 5
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 100
    
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
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignorar variáveis extras no .env

# Instância global de configurações
settings = Settings()

# Helper para configurações do Kafka Producer
def get_kafka_producer_config() -> dict:
    """Retorna configurações para o produtor Kafka."""
    return {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'acks': settings.KAFKA_ACKS,
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
        'enable.auto.commit': True,
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
    }
