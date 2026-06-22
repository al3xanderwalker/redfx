// Two-tier cache: an in-process L1 (nanosecond, per-instance) in front of the Redis L2 (shared,
// durable), kept coherent across instances by pub/sub invalidation. `makeTiered` is a *scoped*
// Effect — it forks the listener fiber.

import { RedisCache } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Duration, Effect, Schema } from "effect";

const Flags = Schema.Record(Schema.String, Schema.Boolean);
declare const loadFlags: (tenant: string) => Effect.Effect<typeof Flags.Type>;

const buildFlagCache = RedisCache.makeTiered({
  schema: Flags,
  prefix: "cache:flags",
  ttl: Duration.minutes(30), // L2 (Redis)
  memory: { capacity: 10_000, ttl: Duration.minutes(5) }, // L1 (in-process); keep <= ttl
  lookup: loadFlags,
});

export const twoTier = Effect.gen(function* () {
  const flags = yield* buildFlagCache; // build once at startup; the handle is context-free

  const f = yield* flags.get("tenant_42"); // L1 (RAM) → L2 (Redis) → origin; Effect<Flags, RedisError>

  yield* flags.invalidate("tenant_42"); // drop L1+L2 here AND publish so other instances evict their L1
  yield* flags.refresh("tenant_42"); // re-fetch, overwrite L2 + L1, publish

  return f;
}).pipe(
  Effect.scoped, // the listener lives for this scope — in a server, the app scope
  Effect.provide(IoRedis.layerPooled({ url: "redis://localhost:6379", size: 10 })),
);
