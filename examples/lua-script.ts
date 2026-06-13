// Lua scripts: `Redis.script` runs EVALSHA and falls back to SCRIPT LOAD automatically on NOSCRIPT,
// caching the SHA per instance. `numkeys` is taken from the keys array, so one script can take a
// varying number of keys, and the result is decoded with a Schema. Type-checked in CI, not run.

import { Config, Effect, Schema } from "effect";
import { Redis } from "redfx";
import { IoRedis } from "redfx-ioredis";

// Release a lock only if we still hold it — a compare-and-delete that returns 1 on success, 0 if the
// token no longer matches (someone else's lock, or ours already expired).
const releaseLock = Redis.script({
  result: Schema.Number,
  lua: "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
});

export const luaScript = releaseLock(["lock:job-42"], ["my-token"]).pipe(
  Effect.map((deleted) => deleted === 1),
  Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))),
);
