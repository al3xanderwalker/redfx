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
export const expirySeconds = (d: Duration.DurationInput): string =>
  String(Math.max(1, Math.ceil(Duration.toSeconds(Duration.decode(d)))));

const expiryMillis = (d: Duration.DurationInput): string =>
  String(Math.max(1, Math.ceil(Duration.toMillis(Duration.decode(d)))));

export interface SetCommandOptions {
  readonly ex?: Duration.DurationInput;
  readonly px?: Duration.DurationInput;
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
  expire: (key: string, ttl: Duration.DurationInput): RedisCommand =>
    make("EXPIRE", key, expirySeconds(ttl)),
  ttl: (key: string): RedisCommand => make("TTL", key),
  mget: (...keys: ReadonlyArray<string>): RedisCommand => ({ name: "MGET", args: keys }),
  publish: (channel: string, message: string): RedisCommand => make("PUBLISH", channel, message),
  raw: (name: string, ...args: ReadonlyArray<string | Uint8Array>): RedisCommand => ({
    name,
    args,
  }),
} as const;
