import { Duration, Effect, Schema } from "effect";
import { Redis } from "./Redis.js";
import type { RedisError } from "./RedisError.js";

export interface RateLimitOptions {
  /** Sub-second windows floor to 1s (server-clock resolution). */
  readonly window: Duration.DurationInput;
  readonly max: number;
  /** Counters live at `{prefix}:{identifier}:{bucket}`. Default `"rl"`. */
  readonly prefix?: string;
}

export interface RateLimitDecision {
  readonly allowed: boolean;
  readonly remaining: number;
  /** Time until the current bucket rolls — a usable `Retry-After`. */
  readonly resetAfter: Duration.Duration;
  readonly limit: number;
}

export interface RateLimitHandle {
  readonly check: (identifier: string) => Effect.Effect<RateLimitDecision, RedisError, Redis>;
}

// Sliding-window counter, weighting the previous bucket by its remaining overlap. The whole decision
// runs in one atomic script, so INCR + EXPIRE can't split (no counter stranded TTL-less) and the
// across-edge burst stays bounded by `max`. Returns `{allowed, remaining, resetAfterSeconds}`.
const lua = `
local t = redis.call('TIME')
local now = tonumber(t[1])
local w = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local cur = math.floor(now / w)
local elapsed = now % w
local ck = KEYS[1] .. ':' .. cur
local pk = KEYS[1] .. ':' .. (cur - 1)
local c = tonumber(redis.call('GET', ck) or '0')
local p = tonumber(redis.call('GET', pk) or '0')
local est = p * ((w - elapsed) / w) + c
local reset = w - elapsed
if est + 1 > max then
  return {0, 0, reset}
end
local n = redis.call('INCR', ck)
if n == 1 then
  redis.call('EXPIRE', ck, w * 2)
end
local remaining = math.floor(max - (p * ((w - elapsed) / w) + n))
if remaining < 0 then remaining = 0 end
return {1, remaining, reset}
`;

const Result = Schema.Tuple(Schema.Number, Schema.Number, Schema.Number);

const windowSeconds = (w: Duration.DurationInput): number =>
  Math.max(1, Math.ceil(Duration.toMillis(Duration.decode(w)) / 1000));

export namespace RateLimit {
  /** Builds the Lua once (SHA cached); `check` returns a decision for the caller to act on. */
  export const make = (options: RateLimitOptions): RateLimitHandle => {
    const prefix = options.prefix ?? "rl";
    const window = windowSeconds(options.window);
    const evalScript = Redis.script({ result: Result, lua });
    return {
      check: (identifier) =>
        evalScript([`${prefix}:${identifier}`], [window, options.max]).pipe(
          Effect.map(
            ([allowed, remaining, reset]): RateLimitDecision => ({
              allowed: allowed === 1,
              remaining,
              resetAfter: Duration.seconds(reset),
              limit: options.max,
            }),
          ),
        ),
    };
  };
}
