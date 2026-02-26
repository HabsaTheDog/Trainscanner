const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function isStrictIsoDate(value) {
  const input = String(value || "").trim();
  if (!ISO_DATE_RE.test(input)) {
    return false;
  }

  const [yearRaw, monthRaw, dayRaw] = input.split("-");
  const year = Number.parseInt(yearRaw, 10);
  const month = Number.parseInt(monthRaw, 10);
  const day = Number.parseInt(dayRaw, 10);
  if (
    !Number.isInteger(year) ||
    !Number.isInteger(month) ||
    !Number.isInteger(day)
  ) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  if (!Number.isFinite(date.getTime())) {
    return false;
  }

  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() + 1 === month &&
    date.getUTCDate() === day
  );
}

module.exports = {
  ISO_DATE_RE,
  isStrictIsoDate,
};
