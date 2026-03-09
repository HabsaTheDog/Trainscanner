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

COPY --chown=node:node package.json ./package.json
COPY --chown=node:node package-lock.json ./package-lock.json
COPY --chown=node:node frontend/package.json ./frontend/package.json
COPY --chown=node:node services/orchestrator/package.json ./services/orchestrator/package.json
COPY --chown=node:node services/control-plane/package.json ./services/control-plane/package.json
RUN npm ci --workspaces --include-workspace-root --no-fund --no-audit

COPY --chown=node:node frontend ./frontend
RUN npm run -w frontend build
COPY --chown=node:node services/orchestrator ./services/orchestrator
COPY --chown=node:node scripts ./scripts
COPY --chown=node:node db ./db
USER node

EXPOSE 3000
CMD ["node", "services/orchestrator/src/server.js"]
