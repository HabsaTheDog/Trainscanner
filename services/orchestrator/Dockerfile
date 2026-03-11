FROM node:20-bookworm-slim AS build

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

COPY package.json ./package.json
COPY package-lock.json ./package-lock.json
COPY frontend/package.json ./frontend/package.json
COPY services/orchestrator/package.json ./services/orchestrator/package.json
COPY services/control-plane/package.json ./services/control-plane/package.json
RUN npm ci --workspaces --include-workspace-root --no-fund --no-audit

COPY frontend ./frontend
RUN npm run -w frontend build
COPY services/orchestrator ./services/orchestrator
COPY scripts ./scripts
COPY db ./db

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

COPY --from=build --chown=node:node --chmod=0444 /app/package.json ./package.json
COPY --from=build --chown=node:node --chmod=0444 /app/package-lock.json ./package-lock.json
COPY --from=build --chown=node:node --chmod=0555 /app/node_modules ./node_modules
COPY --from=build --chown=node:node --chmod=0555 /app/frontend/dist ./frontend/dist
COPY --from=build --chown=node:node --chmod=0555 /app/services/orchestrator ./services/orchestrator
COPY --from=build --chown=node:node --chmod=0555 /app/scripts ./scripts
COPY --from=build --chown=node:node --chmod=0555 /app/db ./db

RUN mkdir -p /app/config /app/data/artifacts /app/services/orchestrator/state \
  && chown -R node:node /app/config /app/data /app/services/orchestrator/state \
  && chmod 0755 /app/config /app/data /app/data/artifacts /app/services/orchestrator/state

ENV CONFIG_DIR=/app/config \
    DATA_DIR=/app/data \
    FRONTEND_DIR=/app/frontend/dist \
    STATE_DIR=/app/services/orchestrator/state

USER node

EXPOSE 3000
CMD ["node", "services/orchestrator/src/server.js"]
