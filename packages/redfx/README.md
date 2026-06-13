# redfx

Core of [redfx](https://github.com/al3xanderwalker/redfx): ergonomic Redis for Effect. Typed
commands (`Option` reads, `Duration` TTLs, errors in the channel), Schema-typed keys (`Redis.ref`),
pub/sub as a `Stream`, Lua `Redis.script` (EVALSHA with a NOSCRIPT fallback), and `RedisCache`
(distributed cache-aside with optional stampede protection, plus a two-tier L1+L2 cache with pub/sub
invalidation), all over a small driver-agnostic `send`-level port.

`effect` is the only dependency. Pair it with a driver adapter:

- [`redfx-ioredis`](https://www.npmjs.com/package/redfx-ioredis) for Node
- [`redfx-bun`](https://www.npmjs.com/package/redfx-bun) for Bun

```ts
import { Redis } from "redfx"
import { IoRedis } from "redfx-ioredis"
import { Config, Duration, Effect } from "effect"

const program = Effect.gen(function* () {
  const redis = yield* Redis
  yield* redis.set("greeting", "hello", { ex: Duration.minutes(15) })
  return yield* redis.get("greeting") // Option<string>
})

program.pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))), Effect.runPromise)
```

See the [project README](https://github.com/al3xanderwalker/redfx) for the full API and design.

## License

MIT
