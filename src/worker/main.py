#!/usr/bin/env python3
"""
Bloco 4: Worker de Inferência em Tempo Real
Consome mensagens do Kafka, processa com Autoencoder (GPU) e detecta anomalias.
"""

import json
import time
import signal
import sys
from datetime import datetime, timezone
from threading import Event
from typing import Dict, Any, Optional, Tuple
import structlog

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import joblib
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Configuração de paths
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.config import settings, get_kafka_consumer_config, get_kafka_producer_config

# ============================================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================================
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# ============================================================================
# MÉTRICAS PROMETHEUS
# ============================================================================
MESSAGES_PROCESSED = Counter(
    'worker_messages_processed_total',
    'Total messages processed by worker',
    ['status']
)

MESSAGES_RECEIVED = Counter(
    'worker_messages_received_total',
    'Total messages received from Kafka'
)

INFERENCE_LATENCY = Histogram(
    'worker_inference_latency_seconds',
    'Inference latency in seconds',
    ['model']
)

ANOMALIES_DETECTED = Counter(
    'worker_anomalies_detected_total',
    'Total anomalies detected',
    ['model']
)

GPU_MEMORY_USAGE = Gauge(
    'worker_gpu_memory_usage_bytes',
    'GPU memory usage in bytes'
)

PROCESSING_ERRORS = Counter(
    'worker_processing_errors_total',
    'Total processing errors',
    ['error_type']
)

RESULTS_PUBLISHED = Counter(
    'worker_results_published_total',
    'Total acknowledged fraud prediction results published',
)

# ============================================================================
# DEFINIÇÃO DO AUTOENCODER (mesma arquitetura do treinamento)
# ============================================================================
class Autoencoder(nn.Module):
    def __init__(self, input_dim: int = 5, latent_dim: int = 2):
        super(Autoencoder, self).__init__()
        # Encoder
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 8),
            nn.ReLU(),
            nn.Linear(8, latent_dim)
        )
        # Decoder
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 8),
            nn.ReLU(),
            nn.Linear(8, 16),
            nn.ReLU(),
            nn.Linear(16, 32),
            nn.ReLU(),
            nn.Linear(32, input_dim)
        )
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        latent = self.encoder(x)
        reconstructed = self.decoder(latent)
        return reconstructed, latent

# ============================================================================
# CARREGAMENTO DE MODELOS E ARTEFATOS
# ============================================================================
def load_artifacts() -> Tuple[Any, Any, Dict, nn.Module, torch.device]:
    """
    Carrega todos os artefatos necessários para inferência.
    Returns: (scaler, iforest, thresholds, model, device)
    """
    logger.info("Loading artifacts...")
    
    # Definir dispositivo (GPU se disponível)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logger.info(f"Using device: {device}")
    
    if torch.cuda.is_available():
        logger.info(f"GPU: {torch.cuda.get_device_name(0)}")
        logger.info(f"GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    
    # Caminhos dos artefatos
    artifacts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "artifacts")
    
    # 1. Carregar scaler
    scaler_path = os.path.join(artifacts_dir, "scaler.pkl")
    scaler = joblib.load(scaler_path)
    logger.info(f"✅ Scaler loaded from {scaler_path}")
    logger.info(f"   Features: {scaler.n_features_in_}")
    
    # 2. Carregar thresholds
    thresholds_path = os.path.join(artifacts_dir, "thresholds.json")
    with open(thresholds_path, 'r') as f:
        thresholds = json.load(f)
    logger.info(f"✅ Thresholds loaded from {thresholds_path}")
    logger.info(f"   Autoencoder threshold: {thresholds['autoencoder_mse_threshold']:.6f}")
    
    # 3. Carregar Isolation Forest (opcional)
    iforest_path = os.path.join(artifacts_dir, "iforest.pkl")
    iforest = None
    if os.path.exists(iforest_path):
        iforest = joblib.load(iforest_path)
        logger.info(f"✅ Isolation Forest loaded from {iforest_path}")
    else:
        logger.warning(f"Isolation Forest not found at {iforest_path}, using Autoencoder only")
    
    # 4. Carregar modelo PyTorch
    model_path = os.path.join(artifacts_dir, "autoencoder.pt")
    checkpoint = torch.load(model_path, map_location=device, weights_only=False)
    
    # Instanciar modelo
    model = Autoencoder(
        input_dim=checkpoint['input_dim'],
        latent_dim=checkpoint['latent_dim']
    )
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()  # Modo de avaliação
    logger.info(f"✅ Autoencoder loaded from {model_path}")
    logger.info(f"   Input dim: {checkpoint['input_dim']}")
    logger.info(f"   Latent dim: {checkpoint['latent_dim']}")
    logger.info(f"   Model moved to: {device}")
    
    # Verificar uso de GPU
    if torch.cuda.is_available():
        gpu_mem = torch.cuda.memory_allocated() / 1e6
        logger.info(f"   GPU memory allocated: {gpu_mem:.2f} MB")
    
    return scaler, iforest, thresholds, model, device

# ============================================================================
# ENGENHARIA DE FEATURES ONLINE
# ============================================================================
def create_features_online(raw_data: Dict[str, Any]) -> Optional[np.ndarray]:
    """
    Converte dados brutos em features numéricas.
    Retorna None se houver erro nos dados.
    """
    try:
        # Extrair dados
        passenger_count = float(raw_data.get('passenger_count', 1))
        trip_distance = float(raw_data.get('trip_distance', 0))
        fare_amount = float(raw_data.get('fare_amount', 0))
        
        # Converter timestamps
        pickup_str = raw_data.get('pickup_datetime', '')
        dropoff_str = raw_data.get('dropoff_datetime', '')
        
        if not pickup_str or not dropoff_str:
            logger.warning("Missing datetime fields", raw_data=raw_data)
            return None
        
        # Converter para datetime
        pickup_dt = pd.to_datetime(pickup_str)
        dropoff_dt = pd.to_datetime(dropoff_str)
        
        # Calcular duração em minutos
        trip_duration_min = (dropoff_dt - pickup_dt).total_seconds() / 60.0
        
        # Validar dados básicos
        if (passenger_count <= 0 or 
            trip_distance <= 0 or 
            fare_amount < 0 or 
            trip_duration_min <= 0):
            logger.warning("Invalid data values", 
                          passenger_count=passenger_count,
                          trip_distance=trip_distance,
                          fare_amount=fare_amount,
                          trip_duration_min=trip_duration_min)
            return None
        
        # Calcular fare_per_minute (com proteção contra divisão por zero)
        if trip_duration_min > 0:
            fare_per_minute = fare_amount / trip_duration_min
        else:
            logger.warning("Zero or negative trip duration", trip_duration_min=trip_duration_min)
            return None
        
        # Criar vetor de features
        features = np.array([
            passenger_count,
            trip_distance,
            fare_amount,
            trip_duration_min,
            fare_per_minute
        ]).reshape(1, -1)  # Shape: (1, 5)
        
        # Verificar NaNs ou infinitos
        if np.any(np.isnan(features)) or np.any(np.isinf(features)):
            logger.warning("NaN or Inf in features", features=features)
            return None
        
        return features
        
    except Exception as e:
        logger.error("Error creating features", error=str(e), raw_data=raw_data)
        PROCESSING_ERRORS.labels(error_type='feature_engineering').inc()
        return None

# ============================================================================
# INFERÊNCIA ENSEMBLE
# ============================================================================
def predict(
    raw_data: Dict[str, Any],
    scaler: Any,
    model: nn.Module,
    iforest: Any,
    thresholds: Dict[str, float],
    device: torch.device
) -> Tuple[bool, Dict[str, float], float]:
    """
    Executa inferência ensemble usando Autoencoder (GPU) e Isolation Forest (CPU).
    
    Returns:
        is_anomaly: bool
        scores: dict com scores de cada modelo
        inference_time: tempo total de inferência em segundos
    """
    start_time = time.time()
    
    # 1. Engenharia de features online
    features = create_features_online(raw_data)
    if features is None:
        return False, {'error': 'invalid_features'}, 0.0
    
    # 2. Aplicar scaling
    try:
        features_scaled = scaler.transform(features)
    except Exception as e:
        logger.error("Error in scaling", error=str(e))
        PROCESSING_ERRORS.labels(error_type='scaling').inc()
        return False, {'error': 'scaling_error'}, 0.0
    
    scores = {}
    
    # 3. Inferência Isolation Forest (CPU)
    iso_anomaly_score = None
    if iforest is not None:
        try:
            # Isolation Forest espera dados não escalados
            features_original = scaler.inverse_transform(features_scaled)
            iso_score = iforest.decision_function(features_original)[0]
            iso_anomaly = iso_score < 0  # Isolation Forest retorna -1 para anomalias
            
            scores['isolation_forest_score'] = float(iso_score)
            scores['isolation_forest_anomaly'] = bool(iso_anomaly)
            
        except Exception as e:
            logger.error("Isolation Forest inference error", error=str(e))
            PROCESSING_ERRORS.labels(error_type='isolation_forest').inc()
    
    # 4. Inferência Autoencoder (GPU)
    try:
        # Converter para tensor PyTorch
        features_tensor = torch.FloatTensor(features_scaled).to(device)
        
        # Inferência (sem calcular gradientes)
        with torch.no_grad():
            reconstructed, latent = model(features_tensor)
            
            # Calcular erro de reconstrução (MSE)
            reconstruction_error = torch.mean((reconstructed - features_tensor) ** 2).item()
            
            scores['autoencoder_mse'] = float(reconstruction_error)
            scores['autoencoder_latent'] = latent.cpu().numpy().tolist()
            
            # Verificar se é anomalia baseado no threshold
            ae_threshold = thresholds['autoencoder_mse_threshold']
            ae_anomaly = reconstruction_error > ae_threshold
            scores['autoencoder_anomaly'] = bool(ae_anomaly)
    
    except Exception as e:
        logger.error("Autoencoder inference error", error=str(e))
        PROCESSING_ERRORS.labels(error_type='autoencoder').inc()
        return False, {'error': 'autoencoder_error'}, 0.0
    
    # 5. Decisão ensemble
    # Priorizar Autoencoder para este MVP
    is_anomaly = ae_anomaly
    
    # Se Isolation Forest também está disponível, podemos combinar
    if iso_anomaly_score is not None:
        # Lógica simples de combinação: qualquer um detectar anomalia
        is_anomaly = ae_anomaly or iso_anomaly
    
    inference_time = time.time() - start_time
    INFERENCE_LATENCY.labels(model='ensemble').observe(inference_time)
    
    return is_anomaly, scores, inference_time

# ============================================================================
# CONSUMER KAFKA
# ============================================================================
class FraudDetectionWorker:
    def __init__(self, max_messages: int | None = None):
        self.running = True
        self.scaler = None
        self.iforest = None
        self.thresholds = None
        self.model = None
        self.device = None
        self.producer = None
        self.max_messages = settings.WORKER_MAX_MESSAGES if max_messages is None else max_messages
        
        # Configurar handlers de sinal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handler para sinais de shutdown."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def initialize(self):
        """Inicializa o worker carregando artefatos e configurando Kafka."""
        logger.info("Initializing Fraud Detection Worker...")
        
        # 1. Carregar artefatos
        self.scaler, self.iforest, self.thresholds, self.model, self.device = load_artifacts()
        
        # 2. Configurar consumer Kafka
        consumer_config = get_kafka_consumer_config()
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(get_kafka_producer_config())
        
        # 3. Subscrever tópicos
        topics = [settings.KAFKA_TOPIC_TRANSACTIONS_RAW]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
        # 4. Iniciar servidor de métricas
        metrics_port = settings.WORKER_METRICS_PORT
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics server started on port {metrics_port}")
        
        logger.info("✅ Worker initialization complete")
        logger.info(f"   Kafka: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"   Topics: {topics}")
        logger.info(f"   Device: {self.device}")
        logger.info(f"   Autoencoder threshold: {self.thresholds['autoencoder_mse_threshold']:.6f}")

    def publish_result(
        self,
        *,
        transaction_id: str,
        request_id: str,
        is_anomaly: bool,
        scores: Dict[str, Any],
        inference_time: float,
    ) -> bool:
        """Publish a compact result and wait for its delivery callback."""
        if self.producer is None:
            PROCESSING_ERRORS.labels(error_type='result_producer').inc()
            return False
        payload = {
            "transaction_id": transaction_id,
            "request_id": request_id,
            "is_anomaly": bool(is_anomaly),
            "scores": scores,
            "inference_time_seconds": float(inference_time),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        done = Event()
        delivery_error: list[object | None] = [None]

        def callback(error, _message):
            delivery_error[0] = error
            done.set()

        try:
            self.producer.produce(
                topic=settings.KAFKA_TOPIC_FRAUD_PREDICTIONS,
                key=transaction_id,
                value=json.dumps(payload, separators=(",", ":")),
                callback=callback,
            )
            deadline = time.monotonic() + settings.KAFKA_PRODUCE_TIMEOUT_SECONDS
            while not done.is_set() and time.monotonic() < deadline:
                self.producer.poll(min(0.1, deadline - time.monotonic()))
            if not done.is_set():
                self.producer.flush(max(0.0, deadline - time.monotonic()))
            if not done.is_set() or delivery_error[0] is not None:
                PROCESSING_ERRORS.labels(error_type='result_delivery').inc()
                return False
        except BufferError:
            PROCESSING_ERRORS.labels(error_type='result_buffer').inc()
            return False
        except KafkaException:
            PROCESSING_ERRORS.labels(error_type='result_publish').inc()
            return False
        RESULTS_PUBLISHED.inc()
        return True
    
    def process_message(self, message) -> bool:
        """Processa uma mensagem Kafka."""
        MESSAGES_RECEIVED.inc()
        
        try:
            # Desserializar JSON
            raw_data = json.loads(message.value().decode('utf-8'))
            
            transaction_id = raw_data.get('transaction_id', 'unknown')
            request_id = raw_data.get('request_id', 'unknown')
            
            logger.debug("Processing message", 
                        transaction_id=transaction_id,
                        request_id=request_id)
            
            # Executar inferência
            is_anomaly, scores, inference_time = predict(
                raw_data, 
                self.scaler, 
                self.model, 
                self.iforest, 
                self.thresholds, 
                self.device
            )

            if "error" in scores:
                PROCESSING_ERRORS.labels(error_type='inference').inc()
                MESSAGES_PROCESSED.labels(status='error').inc()
                return False
            if not self.publish_result(
                transaction_id=transaction_id,
                request_id=request_id,
                is_anomaly=is_anomaly,
                scores=scores,
                inference_time=inference_time,
            ):
                MESSAGES_PROCESSED.labels(status='error').inc()
                return False
            
            # Log baseado no resultado
            if is_anomaly:
                ANOMALIES_DETECTED.labels(model='autoencoder').inc()
                MESSAGES_PROCESSED.labels(status='anomaly').inc()
                
                logger.warning("🚨 ANOMALY DETECTED",
                              transaction_id=transaction_id,
                              request_id=request_id,
                              scores=scores,
                              inference_time=inference_time,
                              raw_data={k: v for k, v in raw_data.items() if k != 'received_at'})
            else:
                MESSAGES_PROCESSED.labels(status='normal').inc()
                
                logger.info("✅ Transaction processed",
                           transaction_id=transaction_id,
                           request_id=request_id,
                           inference_time=inference_time,
                           autoencoder_mse=scores.get('autoencoder_mse'),
                           is_normal=True)
            
            return True
            
        except json.JSONDecodeError as e:
            logger.error("JSON decode error", error=str(e), message=message.value())
            PROCESSING_ERRORS.labels(error_type='json_decode').inc()
            MESSAGES_PROCESSED.labels(status='error').inc()
            return False
            
        except Exception as e:
            logger.error("Unexpected error processing message", 
                        error=str(e), 
                        error_type=type(e).__name__)
            PROCESSING_ERRORS.labels(error_type='unexpected').inc()
            MESSAGES_PROCESSED.labels(status='error').inc()
            return False
    
    def run(self, max_messages: int | None = None):
        """Loop principal de consumo Kafka."""
        logger.info("Starting Kafka consumer loop...")
        poll_timeout = settings.WORKER_POLL_TIMEOUT
        limit = self.max_messages if max_messages is None else max_messages
        processed = 0
        
        while self.running:
            try:
                # Poll para nova mensagem
                msg = self.consumer.poll(poll_timeout)
                
                if msg is None:
                    # Timeout, continuar loop
                    continue
                
                if msg.error():
                    # Erro do Kafka
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fim da partição, normal
                        logger.debug("Reached end of partition", topic=msg.topic(), partition=msg.partition())
                    else:
                        logger.error("Kafka error", error=msg.error())
                    self.running = False
                    break
                
                # Processar mensagem
                success = self.process_message(msg)
                if not success:
                    logger.error("Worker fail-stop after message processing failure")
                    self.running = False
                    break
                try:
                    self.consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:  # noqa: BLE001 - do not read ahead after commit uncertainty
                    logger.error("Worker fail-stop after offset commit failure", error=str(exc))
                    self.running = False
                    break
                processed += 1
                if limit and processed >= limit:
                    self.running = False
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                self.running = False
                break
                
            except Exception as e:
                logger.error("Error in consumer loop", error=str(e))
                time.sleep(1)  # Pequena pausa antes de continuar
        
        # Cleanup
        self.shutdown()
    
    def shutdown(self):
        """Cleanup resources."""
        logger.info("Shutting down worker...")
        
        try:
            # Fechar consumer Kafka
            if hasattr(self, 'consumer'):
                self.consumer.close()
                logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error("Error closing Kafka consumer", error=str(e))

        try:
            if self.producer is not None:
                self.producer.flush(settings.KAFKA_PRODUCE_TIMEOUT_SECONDS)
                self.producer = None
                logger.info("Kafka result producer flushed")
        except Exception as e:
            logger.error("Error flushing result producer", error=str(e))
        
        # Limpar memória GPU
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            logger.info("GPU cache cleared")
        
        logger.info("Worker shutdown complete")

# ============================================================================
# MAIN
# ============================================================================
def main():
    """Função principal."""
    import argparse

    parser = argparse.ArgumentParser(description="Fraud detection worker")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="exit after this many acknowledged results (default: configured/unlimited)",
    )
    args = parser.parse_args()
    print("="*70)
    print("FRAUD DETECTION WORKER - BLOCO 4")
    print("="*70)
    
    worker = FraudDetectionWorker(max_messages=args.max_messages)
    
    try:
        worker.initialize()
        worker.run()
    except Exception as e:
        logger.critical("Fatal error in worker", error=str(e))
        worker.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main()
