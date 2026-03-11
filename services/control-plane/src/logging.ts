type LogLevel = "info" | "error";

export function formatLogLine(
  level: LogLevel,
  message: string,
  context?: Record<string, unknown>,
): string {
  const payload = {
    ts: new Date().toISOString(),
    level,
    service: "control-plane",
    message,
    ...(context && Object.keys(context).length > 0 ? { context } : {}),
  };
  return `${JSON.stringify(payload)}\n`;
}

function writeLog(
  stream: NodeJS.WriteStream,
  level: LogLevel,
  message: string,
  context?: Record<string, unknown>,
) {
  stream.write(formatLogLine(level, message, context));
}

export function logInfo(
  message: string,
  context?: Record<string, unknown>,
): void {
  writeLog(process.stdout, "info", message, context);
}

export function logError(
  message: string,
  context?: Record<string, unknown>,
): void {
  writeLog(process.stderr, "error", message, context);
}
