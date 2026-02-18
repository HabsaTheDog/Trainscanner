# ---------- Stage 1: Build ----------
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --ignore-scripts 2>/dev/null || npm install

COPY tsconfig.json nest-cli.json ./
COPY src/ ./src/

RUN npx nest build

# ---------- Stage 2: Production ----------
FROM node:20-alpine AS production

RUN apk add --no-cache wget

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --ignore-scripts 2>/dev/null || npm install --omit=dev

COPY --from=builder /app/dist ./dist

# Data directory (mounted as volume)
RUN mkdir -p /app/data

EXPOSE 3000

CMD ["node", "dist/main"]
