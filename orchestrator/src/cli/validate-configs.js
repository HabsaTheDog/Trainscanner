#!/usr/bin/env node
const fs = require('node:fs/promises');
const path = require('node:path');

const { validateGtfsProfilesConfig } = require('../domains/switch-runtime/contracts');
const { validateSourceDiscoveryConfig } = require('../domains/source-discovery/contracts');
const { validateOjpEndpointsConfig } = require('../domains/qa/ojp-contracts');

function parseArgs(argv) {
  const args = {
    root: process.cwd(),
    only: ''
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--root') {
      args.root = argv[i + 1] || '';
      i += 1;
      continue;
    }
    if (arg === '--only') {
      args.only = argv[i + 1] || '';
      i += 1;
      continue;
    }
    if (arg === '-h' || arg === '--help') {
      printHelp();
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function printHelp() {
  process.stdout.write(`Usage: node orchestrator/src/cli/validate-configs.js [options]\n\n`);
  process.stdout.write(`Options:\n`);
  process.stdout.write(`  --root <path>       Project root (default: cwd)\n`);
  process.stdout.write(`  --only <name>       One of: profiles,dach,ojp,ojp-mock\n`);
}

async function readJson(filePath) {
  return JSON.parse(await fs.readFile(filePath, 'utf8'));
}

async function validateEntry(name, filePath, validator) {
  const payload = await readJson(filePath);
  validator(payload);
  process.stdout.write(`[validate-configs] ok ${name} -> ${path.relative(process.cwd(), filePath)}\n`);
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const root = path.resolve(args.root);
  const configDir = path.join(root, 'config');

  const tasks = [
    {
      name: 'profiles',
      filePath: path.join(configDir, 'gtfs-profiles.json'),
      validator: validateGtfsProfilesConfig
    },
    {
      name: 'dach',
      filePath: path.join(configDir, 'dach-data-sources.json'),
      validator: validateSourceDiscoveryConfig
    },
    {
      name: 'ojp',
      filePath: path.join(configDir, 'ojp-endpoints.json'),
      validator: validateOjpEndpointsConfig
    },
    {
      name: 'ojp-mock',
      filePath: path.join(configDir, 'ojp-endpoints.mock.json'),
      validator: validateOjpEndpointsConfig
    }
  ];

  const selected = args.only ? tasks.filter((task) => task.name === args.only) : tasks;
  if (selected.length === 0) {
    throw new Error(`Unknown --only target '${args.only}'`);
  }

  for (const task of selected) {
    await validateEntry(task.name, task.filePath, task.validator);
  }
}

run().catch((err) => {
  process.stderr.write(`[validate-configs] ERROR: ${err.message}\n`);
  process.exit(1);
});
