FROM node:20-alpine

RUN apk add --no-cache unzip

WORKDIR /app
COPY package.json ./
COPY src ./src

EXPOSE 3000
CMD ["node", "src/server.js"]
