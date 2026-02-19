#!/usr/bin/env node

const fs = require('fs');
const http = require('http');
const path = require('path');

function parseArgs(argv) {
  const args = {
    host: '127.0.0.1',
    port: 18080,
    responseFile: path.resolve(__dirname, '..', 'samples', 'ojp-trip-response.mock.xml'),
  };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];

    switch (key) {
      case '--host':
        args.host = next || '';
        i += 1;
        break;
      case '--port':
        args.port = Number(next);
        i += 1;
        break;
      case '--response-file':
        args.responseFile = path.resolve(next || '');
        i += 1;
        break;
      case '-h':
      case '--help':
        process.stdout.write('Usage: node scripts/data/mock/mock-ojp-server.js [--host 127.0.0.1] [--port 18080] [--response-file FILE]\n');
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${key}`);
    }
  }

  if (!Number.isInteger(args.port) || args.port < 1 || args.port > 65535) {
    throw new Error('--port must be an integer between 1 and 65535');
  }

  if (!args.host) {
    throw new Error('--host must not be empty');
  }

  return args;
}

function main() {
  const args = parseArgs(process.argv);
  const responseXml = fs.readFileSync(args.responseFile, 'utf8');

  const server = http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end('{"ok":true}\n');
      return;
    }

    if (req.method === 'POST' && req.url === '/ojp') {
      let body = '';
      req.setEncoding('utf8');
      req.on('data', (chunk) => {
        body += chunk;
        if (body.length > 2_000_000) {
          req.socket.destroy();
        }
      });
      req.on('end', () => {
        if (!body.includes('<OJPTripRequest>')) {
          res.writeHead(400, { 'content-type': 'application/xml; charset=utf-8' });
          res.end('<?xml version="1.0" encoding="UTF-8"?><Error><ErrorText>Missing OJPTripRequest</ErrorText></Error>\n');
          return;
        }

        res.writeHead(200, { 'content-type': 'application/xml; charset=utf-8' });
        res.end(responseXml.endsWith('\n') ? responseXml : `${responseXml}\n`);
      });
      return;
    }

    res.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
    res.end('{"ok":false,"error":"not_found"}\n');
  });

  server.listen(args.port, args.host, () => {
    process.stdout.write(`[mock-ojp-server] listening on http://${args.host}:${args.port}\n`);
  });

  function shutdown(signal) {
    server.close(() => {
      process.stdout.write(`[mock-ojp-server] stopped (${signal})\n`);
      process.exit(0);
    });
  }

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

try {
  main();
} catch (err) {
  process.stderr.write(`[mock-ojp-server] ERROR: ${err.message}\n`);
  process.exit(1);
}
