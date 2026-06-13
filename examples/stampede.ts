// Stampede protection: when a cold or expired hot key is hit by many requests at once, exactly one
// `lookup` runs (behind a cross-instance Redis lock) while the rest jitter-poll and read the value
// it stores. Type-checked in CI, not run — `fetchQuote` stands in for your origin call.

import { Config, Duration, Effect, Schema } from "effect";
import { RedisCache } from "redfx";
import { IoRedis } from "redfx-ioredis";

const Quote = Schema.Struct({ symbol: Schema.String, priceMinor: Schema.Number });
declare const fetchQuote: (symbol: string) => Effect.Effect<typeof Quote.Type>;

// Availability-first default: a lookup slower than the lock degrades to a lock-free local compute,
// so the request is always served — duplicate work is bounded and self-correcting, never an error.
const quotes = RedisCache.make({
  schema: Quote,
  prefix: "cache:quote",
  ttl: Duration.seconds(30),
  stampede: true,
  lookup: fetchQuote,
});

// Strict single-flight: a waiter that passes its deadline fails with a TimeoutError instead of
// computing — choose this when running the lookup twice is worse than failing the request.
const strictQuotes = RedisCache.make({
  schema: Quote,
  prefix: "cache:quote:strict",
  ttl: Duration.seconds(30),
  stampede: { lockTtl: "5 seconds", onTimeout: "fail" },
  lookup: fetchQuote,
});

export const stampede = Effect.all([quotes.get("AAPL"), strictQuotes.get("AAPL")]).pipe(
  Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))),
);
