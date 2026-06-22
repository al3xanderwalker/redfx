// Schema-typed sets: `Redis.setOf` ties a key prefix to a Schema, so each member encodes/decodes as
// one JSON value and reads come back typed. Membership, add/remove, and a re-stamped TTL are folded in.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Schema } from "effect";

const Tag = Schema.Literals(["vip", "beta", "staff"]);
const userTags = Redis.setOf(Tag, { prefix: "user:tags", ttl: Duration.hours(24) });

export const collectionSet = Effect.gen(function* () {
  yield* userTags("u_123").add("vip", "beta"); // SADD, then re-stamp the 24h TTL
  const tags = yield* userTags("u_123").members; // ReadonlyArray<"vip" | "beta" | "staff">, decoded
  const isVip = yield* userTags("u_123").has("vip"); // boolean, no decode needed
  const count = yield* userTags("u_123").size; // number
  yield* userTags("u_123").remove("beta"); // SREM
  return { tags, isVip, count };
}).pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
