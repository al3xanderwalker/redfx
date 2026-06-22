import { Duration } from "effect";

export interface RedisCommand {
  readonly name: string;
  readonly args: ReadonlyArray<string | Uint8Array>;
}

export const make = (name: string, ...args: ReadonlyArray<string | Uint8Array>): RedisCommand => ({
  name,
  args,
});

/** Whole seconds for `EX`/`EXPIRE`, floored at 1: `SET ... EX 0` errors, and `EXPIRE key 0` would delete the key. */
export const expirySeconds = (d: Duration.Input): string =>
  String(Math.max(1, Math.ceil(Duration.toSeconds(d))));

const expiryMillis = (d: Duration.Input): string =>
  String(Math.max(1, Math.ceil(Duration.toMillis(d))));

/** Whole millis floored at 0 — for `BLOCK`/`IDLE`/min-idle, where `0` is valid (block forever / no
 *  idle floor), unlike `expirySeconds` which floors at 1. */
const wholeMillis = (d: Duration.Input): string =>
  String(Math.max(0, Math.floor(Duration.toMillis(d))));

export interface TrimArgs {
  readonly maxLen: number;
  /** Opts into `MAXLEN ~`: Redis trims in whole macronode chunks, so length stays `>= maxLen`. */
  readonly approx?: boolean;
}

export const trimArgs = (opts: TrimArgs): ReadonlyArray<string> =>
  opts.approx ? ["MAXLEN", "~", String(opts.maxLen)] : ["MAXLEN", String(opts.maxLen)];

export interface XReadArgs {
  readonly count?: number;
  readonly block?: Duration.Input;
}

export interface SetCommandOptions {
  readonly ex?: Duration.Input;
  readonly px?: Duration.Input;
  readonly keepTtl?: boolean;
}

export const setArgs = (
  key: string,
  value: string,
  opts?: SetCommandOptions,
): ReadonlyArray<string> => {
  const args: Array<string> = [key, value];
  if (opts?.ex !== undefined) args.push("EX", expirySeconds(opts.ex));
  else if (opts?.px !== undefined) args.push("PX", expiryMillis(opts.px));
  else if (opts?.keepTtl) args.push("KEEPTTL");
  return args;
};

/** ZADD/ZINCRBY scores: `±Infinity` → Redis's `"inf"`/`"-inf"` (`String(Infinity)` is `"Infinity"`, which Redis rejects). */
export const formatScore = (n: number): string => {
  if (Number.isNaN(n)) throw new RangeError("redfx: a sorted-set score must not be NaN");
  if (n === Number.POSITIVE_INFINITY) return "inf";
  if (n === Number.NEGATIVE_INFINITY) return "-inf";
  return String(n);
};

export const hashEntriesArgs = (
  entries: Record<string, string> | ReadonlyArray<readonly [string, string]>,
): ReadonlyArray<string> => {
  const pairs = Array.isArray(entries) ? entries : Object.entries(entries);
  return pairs.flat();
};

export const zaddArgs = (
  entries: ReadonlyArray<readonly [number, string]>,
): ReadonlyArray<string> => entries.flatMap(([score, member]) => [formatScore(score), member]);

export const Cmd = {
  get: (key: string): RedisCommand => make("GET", key),
  set: (key: string, value: string, opts?: SetCommandOptions): RedisCommand => ({
    name: "SET",
    args: setArgs(key, value, opts),
  }),
  getDelete: (key: string): RedisCommand => make("GETDEL", key),
  del: (...keys: ReadonlyArray<string>): RedisCommand => ({ name: "DEL", args: keys }),
  exists: (...keys: ReadonlyArray<string>): RedisCommand => ({ name: "EXISTS", args: keys }),
  incr: (key: string): RedisCommand => make("INCR", key),
  decr: (key: string): RedisCommand => make("DECR", key),
  incrBy: (key: string, by: number): RedisCommand => make("INCRBY", key, String(by)),
  decrBy: (key: string, by: number): RedisCommand => make("DECRBY", key, String(by)),
  expire: (key: string, ttl: Duration.Input): RedisCommand =>
    make("EXPIRE", key, expirySeconds(ttl)),
  ttl: (key: string): RedisCommand => make("TTL", key),
  mget: (...keys: ReadonlyArray<string>): RedisCommand => ({ name: "MGET", args: keys }),
  publish: (channel: string, message: string): RedisCommand => make("PUBLISH", channel, message),
  hset: (
    key: string,
    entries: Record<string, string> | ReadonlyArray<readonly [string, string]>,
  ): RedisCommand => ({ name: "HSET", args: [key, ...hashEntriesArgs(entries)] }),
  hget: (key: string, field: string): RedisCommand => make("HGET", key, field),
  hgetAll: (key: string): RedisCommand => make("HGETALL", key),
  hdel: (key: string, ...fields: ReadonlyArray<string>): RedisCommand => ({
    name: "HDEL",
    args: [key, ...fields],
  }),
  hexists: (key: string, field: string): RedisCommand => make("HEXISTS", key, field),
  hincrBy: (key: string, field: string, by: number): RedisCommand =>
    make("HINCRBY", key, field, String(by)),
  hkeys: (key: string): RedisCommand => make("HKEYS", key),
  hvals: (key: string): RedisCommand => make("HVALS", key),
  hlen: (key: string): RedisCommand => make("HLEN", key),
  hmget: (key: string, ...fields: ReadonlyArray<string>): RedisCommand => ({
    name: "HMGET",
    args: [key, ...fields],
  }),
  sadd: (key: string, ...members: ReadonlyArray<string>): RedisCommand => ({
    name: "SADD",
    args: [key, ...members],
  }),
  srem: (key: string, ...members: ReadonlyArray<string>): RedisCommand => ({
    name: "SREM",
    args: [key, ...members],
  }),
  smembers: (key: string): RedisCommand => make("SMEMBERS", key),
  sismember: (key: string, member: string): RedisCommand => make("SISMEMBER", key, member),
  scard: (key: string): RedisCommand => make("SCARD", key),
  spop: (key: string): RedisCommand => make("SPOP", key),
  zadd: (key: string, entries: ReadonlyArray<readonly [number, string]>): RedisCommand => ({
    name: "ZADD",
    args: [key, ...zaddArgs(entries)],
  }),
  zrem: (key: string, ...members: ReadonlyArray<string>): RedisCommand => ({
    name: "ZREM",
    args: [key, ...members],
  }),
  zscore: (key: string, member: string): RedisCommand => make("ZSCORE", key, member),
  zincrBy: (key: string, by: number, member: string): RedisCommand =>
    make("ZINCRBY", key, formatScore(by), member),
  zcard: (key: string): RedisCommand => make("ZCARD", key),
  zrank: (key: string, member: string): RedisCommand => make("ZRANK", key, member),
  zrange: (key: string, start: number, stop: number): RedisCommand =>
    make("ZRANGE", key, String(start), String(stop)),
  zrangeWithScores: (key: string, start: number, stop: number): RedisCommand =>
    make("ZRANGE", key, String(start), String(stop), "WITHSCORES"),
  xadd: (
    key: string,
    fields: Record<string, string> | ReadonlyArray<readonly [string, string]>,
    opts?: { readonly id?: string; readonly maxLen?: number; readonly approx?: boolean },
  ): RedisCommand => {
    const args: Array<string | Uint8Array> = [key];
    if (opts?.maxLen !== undefined)
      args.push(...trimArgs({ maxLen: opts.maxLen, approx: opts.approx }));
    args.push(opts?.id ?? "*", ...hashEntriesArgs(fields));
    return { name: "XADD", args };
  },
  xlen: (key: string): RedisCommand => make("XLEN", key),
  xrange: (
    key: string,
    start: string,
    end: string,
    opts?: { readonly count?: number },
  ): RedisCommand => {
    const args = [key, start, end];
    if (opts?.count !== undefined) args.push("COUNT", String(opts.count));
    return { name: "XRANGE", args };
  },
  xrevrange: (
    key: string,
    start: string,
    end: string,
    opts?: { readonly count?: number },
  ): RedisCommand => {
    const args = [key, end, start]; // XREVRANGE scans high→low, so the higher id (end) comes first
    if (opts?.count !== undefined) args.push("COUNT", String(opts.count));
    return { name: "XREVRANGE", args };
  },
  xdel: (key: string, ...ids: ReadonlyArray<string>): RedisCommand => ({
    name: "XDEL",
    args: [key, ...ids],
  }),
  xtrim: (key: string, opts: TrimArgs): RedisCommand => ({
    name: "XTRIM",
    args: [key, ...trimArgs(opts)],
  }),
  xread: (streams: ReadonlyArray<readonly [string, string]>, opts?: XReadArgs): RedisCommand => {
    const args: Array<string> = [];
    if (opts?.count !== undefined) args.push("COUNT", String(opts.count));
    if (opts?.block !== undefined) args.push("BLOCK", wholeMillis(opts.block));
    args.push("STREAMS", ...streams.map(([k]) => k), ...streams.map(([, id]) => id));
    return { name: "XREAD", args };
  },
  xreadGroup: (
    group: string,
    consumer: string,
    streams: ReadonlyArray<readonly [string, string]>,
    opts?: XReadArgs & { readonly noAck?: boolean },
  ): RedisCommand => {
    const args: Array<string> = ["GROUP", group, consumer];
    if (opts?.count !== undefined) args.push("COUNT", String(opts.count));
    if (opts?.block !== undefined) args.push("BLOCK", wholeMillis(opts.block));
    if (opts?.noAck) args.push("NOACK");
    args.push("STREAMS", ...streams.map(([k]) => k), ...streams.map(([, id]) => id));
    return { name: "XREADGROUP", args };
  },
  xgroupCreate: (
    key: string,
    group: string,
    opts?: { readonly from?: string; readonly mkStream?: boolean },
  ): RedisCommand => {
    const args = ["CREATE", key, group, opts?.from ?? "$"];
    if (opts?.mkStream) args.push("MKSTREAM");
    return { name: "XGROUP", args };
  },
  xack: (key: string, group: string, ...ids: ReadonlyArray<string>): RedisCommand => ({
    name: "XACK",
    args: [key, group, ...ids],
  }),
  xpending: (key: string, group: string): RedisCommand => make("XPENDING", key, group),
  xpendingExtended: (
    key: string,
    group: string,
    opts: {
      readonly start?: string;
      readonly end?: string;
      readonly count: number;
      readonly consumer?: string;
      readonly idle?: Duration.Input;
    },
  ): RedisCommand => {
    const args = [key, group];
    if (opts.idle !== undefined) args.push("IDLE", wholeMillis(opts.idle));
    args.push(opts.start ?? "-", opts.end ?? "+", String(opts.count));
    if (opts.consumer !== undefined) args.push(opts.consumer);
    return { name: "XPENDING", args };
  },
  xclaim: (
    key: string,
    group: string,
    consumer: string,
    minIdle: Duration.Input,
    ids: ReadonlyArray<string>,
  ): RedisCommand => ({
    name: "XCLAIM",
    args: [key, group, consumer, wholeMillis(minIdle), ...ids],
  }),
  xautoclaim: (
    key: string,
    group: string,
    consumer: string,
    minIdle: Duration.Input,
    opts?: { readonly start?: string; readonly count?: number },
  ): RedisCommand => {
    const args = [key, group, consumer, wholeMillis(minIdle), opts?.start ?? "0-0"];
    if (opts?.count !== undefined) args.push("COUNT", String(opts.count));
    return { name: "XAUTOCLAIM", args };
  },
  xinfoStream: (key: string): RedisCommand => make("XINFO", "STREAM", key),
  xinfoGroups: (key: string): RedisCommand => make("XINFO", "GROUPS", key),
  raw: (name: string, ...args: ReadonlyArray<string | Uint8Array>): RedisCommand => ({
    name,
    args,
  }),
} as const;
