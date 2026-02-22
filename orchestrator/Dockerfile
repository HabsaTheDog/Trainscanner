FROM node:20-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    curl \
    jq \
    unzip \
    python3 \
    findutils \
    postgresql-client \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY frontend/package.json ./frontend/package.json
COPY frontend/package-lock.json ./frontend/package-lock.json
RUN cd frontend && npm ci
COPY frontend ./frontend
RUN cd frontend && npm run build
COPY orchestrator ./orchestrator
COPY scripts ./scripts
COPY db ./db

EXPOSE 3000
CMD ["node", "orchestrator/src/server.js"]
