// Distributed cache-aside: a read-through `get` checks Redis, computes on a miss via `lookup`,
// stores the result, and serves it warm next time.

import { RedisCache } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Schema } from "effect";

const User = Schema.Struct({
  id: Schema.String,
  email: Schema.String,
  plan: Schema.Literals(["free", "pro"]),
});

class UserNotFound extends Schema.TaggedErrorClass<UserNotFound>()("UserNotFound", {
  id: Schema.String,
}) {}

declare const fetchUser: (id: string) => Effect.Effect<typeof User.Type, UserNotFound>;

const users = RedisCache.make({
  schema: User,
  prefix: "cache:user",
  ttl: Duration.minutes(10),
  lookup: fetchUser,
});

export const cacheAside = Effect.gen(function* () {
  const a = yield* users.get("u_123"); // miss → fetchUser runs, stored in Redis for 10m
  const b = yield* users.get("u_123"); // hit  → from Redis, fetchUser NOT called
  yield* users.set("u_123", { ...a, plan: "pro" }); // write-through after an update
  yield* users.invalidate("u_123"); // drop on delete
  const fresh = yield* users.refresh("u_123"); // force re-fetch + overwrite
  return { a, b, fresh };
}).pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
