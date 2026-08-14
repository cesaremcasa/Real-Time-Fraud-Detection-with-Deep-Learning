"""
API FastAPI principal para ingestão de transações de táxi.
Bloco 3: Ingestion Layer
"""

import uuid
import time
from datetime import datetime
from typing import Dict, Any

import structlog
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_client import make_asgi_app, Counter, Histogram, Gauge

from confluent_kafka import Producer, KafkaException

# Importações locais
from src.api.models import TransactionRequest, TransactionResponse, HealthResponse
from src.api.security import ProducerAuthenticationError, ProducerAuthenticator
from src.utils.config import settings, get_kafka_producer_config, get_producer_secrets

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
        structlog.processors.JSONRenderer() if settings.LOG_JSON_FORMAT else structlog.dev.ConsoleRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# ============================================================================
# CONFIGURAÇÃO DO PROMETHEUS
# ============================================================================
# Métricas
REQUESTS_TOTAL = Counter(
    'api_requests_total',
    'Total requests received',
    ['method', 'endpoint', 'status']
)

REQUEST_DURATION = Histogram(
    'api_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint']
)

ACTIVE_REQUESTS = Gauge(
    'api_active_requests',
    'Active requests currently being processed'
)

KAFKA_MESSAGES_SENT = Counter(
    'kafka_messages_sent_total',
    'Total messages sent to Kafka',
    ['topic', 'status']
)

KAFKA_PRODUCE_ERRORS = Counter(
    'kafka_produce_errors_total',
    'Total Kafka produce errors'
)

# ============================================================================
# CONFIGURAÇÃO DO KAFKA PRODUCER
# ============================================================================
class KafkaProducerWrapper:
    """Wrapper para o produtor Kafka com reconexão automática."""
    
    def __init__(self):
        self.config = get_kafka_producer_config()
        self._producer = None
        self._connected = False
        self.logger = structlog.get_logger(__name__)
    
    @property
    def producer(self) -> Producer:
        """Getter lazy para o produtor Kafka."""
        if self._producer is None or not self._connected:
            self._connect()
        return self._producer
    
    def _connect(self):
        """Conecta ao Kafka."""
        try:
            self.logger.info("Connecting to Kafka", servers=self.config['bootstrap.servers'])
            self._producer = Producer(self.config)
            
            # Testar conexão
            self._producer.list_topics(timeout=5)
            self._connected = True
            self.logger.info("Kafka connection successful")
            
        except Exception as e:
            self._connected = False
            self.logger.error("Kafka connection failed", error=str(e))
            raise
    
    def produce(self, topic: str, value: str, key: str = None):
        """Produz mensagem para o Kafka."""
        try:
            self.producer.produce(
                topic=topic,
                value=value.encode('utf-8') if isinstance(value, str) else value,
                key=key,
                callback=self._delivery_callback
            )
            # Poll para trigger callbacks e verificar erros
            self.producer.poll(0)
            KAFKA_MESSAGES_SENT.labels(topic=topic, status='success').inc()
            return True
            
        except BufferError as e:
            self.logger.warning("Kafka producer buffer full", error=str(e))
            KAFKA_PRODUCE_ERRORS.inc()
            return False
        except KafkaException as e:
            self.logger.error("Kafka produce error", error=str(e))
            KAFKA_PRODUCE_ERRORS.inc()
            self._connected = False
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback para entrega de mensagens Kafka."""
        if err:
            self.logger.error("Message delivery failed", error=str(err), topic=msg.topic())
            KAFKA_MESSAGES_SENT.labels(topic=msg.topic(), status='error').inc()
        else:
            self.logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
    
    def flush(self, timeout: float = 5.0):
        """Espera todas as mensagens serem entregues."""
        if self._producer:
            self._producer.flush(timeout)
    
    def close(self):
        """Fecha a conexão com o Kafka."""
        if self._producer:
            self.flush()
            self._producer = None
            self._connected = False
            self.logger.info("Kafka producer closed")

# Instância global do produtor
kafka_producer = KafkaProducerWrapper()
producer_authenticator: ProducerAuthenticator | None = None

# ============================================================================
# CONFIGURAÇÃO DO FASTAPI
# ============================================================================
# Rate Limiter
limiter = Limiter(key_func=get_remote_address, default_limits=[f"{settings.RATE_LIMIT_PER_MINUTE}/minute"])

# Criação da aplicação FastAPI
app = FastAPI(
    title="Fraud Detection API",
    description="API para ingestão de transações de táxi e detecção de fraudes",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Adicionar rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Middleware de métricas e logging
@app.middleware("http")
async def metrics_and_logging_middleware(request: Request, call_next):
    """Middleware para coletar métricas e logs."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.time()
    ACTIVE_REQUESTS.inc()
    
    # Log da requisição recebida
    logger.info(
        "Request received",
        request_id=request_id,
        method=request.method,
        url=str(request.url),
        client_ip=get_remote_address(request)
    )
    
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        
        # Registrar métricas
        REQUEST_DURATION.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)
        
        REQUESTS_TOTAL.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()
        
        # Log da resposta
        logger.info(
            "Request completed",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
            duration_ms=duration * 1000
        )
        
        return response
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "Request failed",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            error=str(e),
            duration_ms=duration * 1000
        )
        raise
    finally:
        ACTIVE_REQUESTS.dec()

# Adicionar app do Prometheus
if settings.PROMETHEUS_ENABLED:
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

# ============================================================================
# ENDPOINTS DA API
# ============================================================================
@app.get("/", include_in_schema=False)
async def root():
    """Endpoint raiz com redirecionamento para docs."""
    return {"message": "Fraud Detection API - See /docs for API documentation"}

@app.get("/health", response_model=HealthResponse)
@limiter.exempt
async def health_check(request: Request):
    """Health check do serviço."""
    kafka_status = kafka_producer._connected
    
    status = "healthy" if kafka_status else "degraded"
    
    return HealthResponse(
        status=status,
        kafka_connected=kafka_status,
        version="1.0.0"
    )


async def require_authenticated_producer(request: Request) -> str:
    """Require a fresh, signed request before it enters the Kafka pipeline."""
    if producer_authenticator is None:
        raise HTTPException(status_code=503, detail="transaction producer authentication is not configured")
    try:
        return producer_authenticator.verify(
            producer_id=request.headers.get("X-Producer-Id"),
            timestamp=request.headers.get("X-Request-Timestamp"),
            nonce=request.headers.get("X-Request-Nonce"),
            signature=request.headers.get("X-Request-Signature"),
            method=request.method,
            path=request.url.path,
            body=await request.body(),
        )
    except ProducerAuthenticationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

@app.post("/api/v1/transaction", response_model=TransactionResponse)
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def create_transaction(
    request: Request,
    transaction: TransactionRequest,
    _producer_id: str = Depends(require_authenticated_producer),
):
    """
    Recebe uma transação de táxi e a envia para processamento.
    
    - Valida os dados com Pydantic
    - Envia para o tópico Kafka transactions_raw
    - Retorna 202 Accepted se bem-sucedido
    """
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    logger.info(
        "Processing transaction",
        request_id=request_id,
        transaction_id=transaction.transaction_id,
        passenger_count=transaction.passenger_count,
        fare_amount=transaction.fare_amount
    )
    
    try:
        # Preparar mensagem para Kafka
        message = {
            "transaction_id": transaction.transaction_id,
            "pickup_datetime": transaction.pickup_datetime.isoformat(),
            "dropoff_datetime": transaction.dropoff_datetime.isoformat(),
            "passenger_count": transaction.passenger_count,
            "trip_distance": transaction.trip_distance,
            "fare_amount": transaction.fare_amount,
            "payment_type": transaction.payment_type,
            "vendor_id": transaction.vendor_id,
            "received_at": datetime.now().isoformat(),
            "request_id": request_id
        }
        
        import json
        message_json = json.dumps(message)
        
        # Enviar para Kafka
        success = kafka_producer.produce(
            topic=settings.KAFKA_TOPIC_TRANSACTIONS_RAW,
            value=message_json,
            key=transaction.transaction_id
        )
        
        if not success:
            logger.error(
                "Failed to send to Kafka",
                request_id=request_id,
                transaction_id=transaction.transaction_id
            )
            raise HTTPException(
                status_code=500,
                detail="Failed to queue transaction for processing"
            )
        
        logger.info(
            "Transaction queued successfully",
            request_id=request_id,
            transaction_id=transaction.transaction_id,
            kafka_topic=settings.KAFKA_TOPIC_TRANSACTIONS_RAW
        )
        
        return TransactionResponse(
            status="accepted",
            transaction_id=transaction.transaction_id,
            message="Transaction queued for processing"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Unexpected error processing transaction",
            request_id=request_id,
            transaction_id=transaction.transaction_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )

# ============================================================================
# EVENTOS DA APLICAÇÃO
# ============================================================================
@app.on_event("startup")
async def startup_event():
    """Evento de inicialização da aplicação."""
    logger.info("Starting Fraud Detection API")
    global producer_authenticator
    producer_authenticator = ProducerAuthenticator(get_producer_secrets())
    logger.info(
        "Configuration",
        api_host=settings.API_HOST,
        api_port=settings.API_PORT,
        kafka_bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        producer_count=len(get_producer_secrets()),
    )
    
    # Testar conexão com Kafka
    try:
        # Conectar ao Kafka
        kafka_producer._connect()
        logger.info("Kafka connection test successful")
    except Exception as e:
        logger.error("Kafka connection test failed", error=str(e))
        # Não falhamos o startup, mas logamos o erro

@app.on_event("shutdown")
async def shutdown_event():
    """Evento de desligamento da aplicação."""
    logger.info("Shutting down Fraud Detection API")
    
    # Fechar conexão com Kafka
    kafka_producer.close()
    logger.info("Kafka producer closed")

# ============================================================================
# MANIPULADORES DE EXCEÇÃO
# ============================================================================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Manipulador de exceções HTTP."""
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    logger.warning(
        "HTTP exception",
        request_id=request_id,
        status_code=exc.status_code,
        detail=exc.detail,
        url=str(request.url)
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "message": exc.detail,
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Manipulador de exceções genéricas."""
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    logger.error(
        "Unhandled exception",
        request_id=request_id,
        error_type=type(exc).__name__,
        error=str(exc),
        url=str(request.url)
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error",
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
    )

# ============================================================================
if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting Fraud Detection API server")
    
    uvicorn.run(
        "src.api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        workers=settings.API_WORKERS,
        reload=settings.API_DEBUG,
        log_config=None
    )
