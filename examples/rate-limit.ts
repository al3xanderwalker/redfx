// Fail open, precisely: a fixed-window rate limiter. Because `RedisError` is a tagged union, we can
// fail OPEN on a lost connection (don't lock everyone out when Redis is down) while a real bug like
// WRONGTYPE still surfaces.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Option } from "effect";

const rateLimit = (key: string, limit: number, window: Duration.Duration) =>
  Effect.gen(function* () {
    const redis = yield* Redis;
    const count = yield* redis.incr(key);
    if (count === 1) yield* redis.expire(key, window);
    return { allowed: count <= limit, reset: yield* redis.ttl(key) };
  }).pipe(
    // only a connection loss fails open; a WRONGTYPE still fails the effect
    Effect.catchTag("ConnectionError", () =>
      Effect.succeed({ allowed: true, reset: Option.some(window) }),
    ),
  );

export const rateLimited = rateLimit("rl:u_123", 100, Duration.minutes(1)).pipe(
  Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))),
);
