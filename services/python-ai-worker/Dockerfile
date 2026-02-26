# Use a python 3.11 slim image
FROM python:3.11-slim

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV TEMPORAL_ADDRESS=host.docker.internal:7233

WORKDIR /app

# Install build tools in case some ML libraries need them before wheel cache
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies (vllm is heavy, so do this before copying source code)
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose the FastAPI port
EXPOSE 8080

CMD ["python", "main.py"]
