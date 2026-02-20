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
COPY orchestrator ./orchestrator
COPY scripts ./scripts
COPY db ./db

EXPOSE 3000
CMD ["node", "orchestrator/src/server.js"]
