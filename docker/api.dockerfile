# Dockerfile para API Producer
# Pinned linux/amd64 digest; update deliberately with a provenance entry.
FROM python:3.11.11-slim@sha256:a8e0a3090316aed0b11037aac613aef32fb1747dcc1dcb5c0f6c727a0113a07f

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y     gcc     g++     && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro para aproveitar cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY src/ ./src/
COPY data/ ./data/
COPY artifacts/ ./artifacts/

# Expor porta da API
EXPOSE 8000

# Comando para iniciar a API
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
