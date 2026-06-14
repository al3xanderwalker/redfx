// Schema-typed keys: `Redis.ref` ties a key prefix to a Schema, folding in encode/decode and TTL.
// You get `Option` reads with typed decode errors instead of `JSON.parse(await redis.get(key))`,
// manual null checks, and the raw "KEEPTTL" argument.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Schema } from "effect";

const OtpRecord = Schema.Struct({ codeHash: Schema.String, attempts: Schema.Number });
const otp = Redis.ref(OtpRecord, { prefix: "otp", ttl: Duration.minutes(15) });

export const schemaRef = Effect.gen(function* () {
  yield* otp("u_123").set({ codeHash: "abc123", attempts: 0 });
  const rec = yield* otp("u_123").get; // Option<OtpRecord>, decode errors are typed
  yield* otp("u_123").update((r) => ({ ...r, attempts: r.attempts + 1 }), { keepTtl: true });
  const consumed = yield* otp("u_123").getDelete; // atomic GETDEL
  return { rec, consumed };
}).pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
