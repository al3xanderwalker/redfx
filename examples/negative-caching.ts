// Negative caching: a read-through `get` returns `A`, not `Option<A>` (unlike `Redis.ref`, a typed
// cell where absence is normal). When "not found" is a normal outcome, model it in the value with
// `Schema.NullOr` so a known-absent key is cached as `null` instead of re-running `lookup` on every
// request. Type-checked in CI, not run.

import { Config, Duration, Effect, Schema } from "effect";
import { RedisCache } from "redfx";
import { IoRedis } from "redfx-ioredis";

const MaybeProfile = Schema.NullOr(Schema.Struct({ bio: Schema.String }));
declare const loadProfile: (id: string) => Effect.Effect<typeof MaybeProfile.Type>;

const profiles = RedisCache.make({
  schema: MaybeProfile,
  prefix: "cache:profile",
  ttl: Duration.minutes(15),
  lookup: loadProfile, // caches `null` (a known-absent) like any other value
});

export const negativeCaching = profiles
  .get("u_404")
  .pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
