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

RUN chown -R node:node /app
USER node

EXPOSE 3000
CMD ["node", "services/orchestrator/src/server.js"]
