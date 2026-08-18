# Dockerfile para ML Worker com suporte a GPU
# Pinned linux/amd64 digest; the CUDA worker remains an explicit GPU image.
FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime@sha256:77f17f843507062875ce8be2a6f76aa6aa3df7f9ef1e31d9d7432f4b0f563dee

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y     gcc     g++     && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro para aproveitar cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte e artefatos
COPY src/ ./src/
COPY data/ ./data/
COPY artifacts/ ./artifacts/

# Comando para iniciar o worker
CMD ["python", "-m", "src.ml_worker"]
