const { AppError } = require('./errors');

function createCircuitBreaker(options = {}) {
  const name = String(options.name || 'circuit').trim();
  const failureThreshold = Number.isFinite(options.failureThreshold) ? Math.max(1, options.failureThreshold) : 3;
  const cooldownMs = Number.isFinite(options.cooldownMs) ? Math.max(100, options.cooldownMs) : 15_000;

  let failureCount = 0;
  let openUntil = 0;

  function isOpen() {
    return Date.now() < openUntil;
  }

  async function execute(fn) {
    if (isOpen()) {
      throw new AppError({
        code: 'CIRCUIT_OPEN',
        message: `Circuit '${name}' is open`,
        details: {
          circuit: name,
          openUntil: new Date(openUntil).toISOString(),
          failureThreshold,
          cooldownMs
        }
      });
    }

    try {
      const result = await fn();
      failureCount = 0;
      openUntil = 0;
      return result;
    } catch (err) {
      failureCount += 1;
      if (failureCount >= failureThreshold) {
        openUntil = Date.now() + cooldownMs;
      }
      throw err;
    }
  }

  return {
    execute,
    state() {
      return {
        name,
        failureCount,
        openUntil,
        isOpen: isOpen()
      };
    }
  };
}

module.exports = {
  createCircuitBreaker
};
