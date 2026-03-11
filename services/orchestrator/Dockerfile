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

COPY --chmod=0444 package.json ./package.json
COPY --chmod=0444 package-lock.json ./package-lock.json
COPY --chmod=0444 frontend/package.json ./frontend/package.json
COPY --chmod=0444 services/orchestrator/package.json ./services/orchestrator/package.json
COPY --chmod=0444 services/control-plane/package.json ./services/control-plane/package.json
RUN npm ci --workspaces --include-workspace-root --no-fund --no-audit

COPY --chmod=0555 frontend ./frontend
RUN npm run -w frontend build
COPY --chmod=0555 services/orchestrator ./services/orchestrator
COPY --chmod=0555 scripts ./scripts
COPY --chmod=0555 db ./db

RUN mkdir -p /app/config /app/data/artifacts /app/services/orchestrator/state \
  && chown -R node:node /app \
  && find /app -path /app/config -prune -o -path /app/data -prune -o -path /app/services/orchestrator/state -prune -o -type d -exec chmod 0555 {} + \
  && find /app -path /app/config -prune -o -path /app/data -prune -o -path /app/services/orchestrator/state -prune -o -type f -exec chmod 0444 {} + \
  && chown -R node:node /app/config /app/data /app/services/orchestrator/state \
  && chmod 0755 /app/config /app/data /app/data/artifacts /app/services/orchestrator/state

ENV CONFIG_DIR=/app/config \
    DATA_DIR=/app/data \
    FRONTEND_DIR=/app/frontend/dist \
    STATE_DIR=/app/services/orchestrator/state

USER node

EXPOSE 3000
CMD ["node", "services/orchestrator/src/server.js"]
