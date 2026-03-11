export function resolveTemporalAddress(
  env: NodeJS.ProcessEnv = process.env,
): string {
  return env.TEMPORAL_ADDRESS?.trim() || "localhost:7233";
}
