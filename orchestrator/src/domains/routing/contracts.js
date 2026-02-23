const { AppError } = require("../../core/errors");

function isValidDate(value) {
  return value instanceof Date && !Number.isNaN(value.getTime());
}

function validateRouteRequestBody(body) {
  const origin = body?.origin;
  const destination = body?.destination;
  const datetime = body?.datetime;

  if (!origin || !destination || !datetime) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Required fields: origin, destination, datetime",
    });
  }

  const requestDate = new Date(datetime);
  if (!isValidDate(requestDate)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "Invalid datetime. Use ISO-8601 format, e.g. 2026-02-19T12:00:00Z.",
    });
  }

  const now = new Date();
  const min = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const max = new Date(now.getTime() + 400 * 24 * 60 * 60 * 1000);
  if (requestDate < min || requestDate > max) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "Datetime outside supported range. Pick a time between now - 30 days and the next 400 days.",
      details: {
        requestDatetime: requestDate.toISOString(),
        min: min.toISOString(),
        max: max.toISOString(),
      },
    });
  }

  return {
    origin,
    destination,
    datetime,
    requestDate,
  };
}

module.exports = {
  validateRouteRequestBody,
};
