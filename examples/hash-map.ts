// Schema-typed hashes: `Redis.hashOf` is a `field(string) → value(A)` map — each value encodes/decodes
// via Schema, fields stay raw strings. set / get / getAll are folded in.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Schema } from "effect";

const Session = Schema.Struct({ ip: Schema.String, lastSeen: Schema.Number });
const sessions = Redis.hashOf(Session, { prefix: "user:sessions", ttl: Duration.days(7) });

export const hashMap = Effect.gen(function* () {
  yield* sessions("u_123").set("device:abc", { ip: "1.2.3.4", lastSeen: 0 }); // HSET, re-stamp 7d TTL
  const one = yield* sessions("u_123").get("device:abc"); // Option<Session>, decoded
  const all = yield* sessions("u_123").getAll; // ReadonlyMap<string, Session>
  const active = yield* sessions("u_123").has("device:abc"); // boolean
  yield* sessions("u_123").remove("device:abc"); // HDEL
  return { one, all, active };
}).pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
