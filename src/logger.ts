// Structured JSON logger for mcp-grocy-api.
//
// All output goes to stderr. This is intentional and must not be changed:
// the stdio MCP transport uses stdout for JSON-RPC messages, and any
// non-RPC output on stdout breaks the protocol.
//
// Each log line is a JSON object with at least: time, level, msg.
// Additional context fields may be supplied as the second argument.
//
// Set LOG_LEVEL to suppress messages below a minimum severity.
// Accepted values: debug, info, warn, error, fatal. Defaults to info.

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 0,
  info:  1,
  warn:  2,
  error: 3,
  fatal: 4,
};

function parseMinLevel(): LogLevel {
  const raw = (process.env.LOG_LEVEL ?? 'info').toLowerCase();
  if (raw in LEVEL_ORDER) return raw as LogLevel;
  process.stderr.write(JSON.stringify({
    time: new Date().toISOString(),
    level: 'warn',
    msg: `Unknown LOG_LEVEL "${raw}", defaulting to "info"`,
  }) + '\n');
  return 'info';
}

const MIN_LEVEL = parseMinLevel();

// Normalise an Error object to a plain serialisable structure.
function serializeError(err: unknown): Record<string, unknown> {
  if (err instanceof Error) {
    return { message: err.message, stack: err.stack, name: err.name };
  }
  if (typeof err === 'object' && err !== null) {
    return err as Record<string, unknown>;
  }
  return { raw: String(err) };
}

function emit(level: LogLevel, msg: string, ctx?: Record<string, unknown>): void {
  if (LEVEL_ORDER[level] < LEVEL_ORDER[MIN_LEVEL]) return;
  const entry: Record<string, unknown> = {
    time: new Date().toISOString(),
    level,
    msg,
    ...ctx,
  };
  try {
    process.stderr.write(JSON.stringify(entry) + '\n');
  } catch (e) {
    process.stderr.write(JSON.stringify({ time: entry.time, level, msg, err: `log serialisation failed: ${String(e)}` }) + '\n');
  }
}

export const log = {
  debug(msg: string, ctx?: Record<string, unknown>): void { emit('debug', msg, ctx); },
  info(msg: string,  ctx?: Record<string, unknown>): void { emit('info',  msg, ctx); },
  warn(msg: string,  ctx?: Record<string, unknown>): void { emit('warn',  msg, ctx); },
  error(msg: string, ctx?: Record<string, unknown>): void { emit('error', msg, ctx); },
  fatal(msg: string, ctx?: Record<string, unknown>): void { emit('fatal', msg, ctx); },

  // Convenience: lift an Error into the context object automatically.
  err(level: LogLevel, msg: string, err: unknown, ctx?: Record<string, unknown>): void {
    emit(level, msg, { ...ctx, err: serializeError(err) });
  },
};
