# @redfx/ioredis

[ioredis](https://github.com/redis/ioredis) driver for [@redfx/core](https://www.npmjs.com/package/@redfx/core).
It implements the `send`-level port over `ioredis.sendCommand`, with a dedicated pub/sub connection
and an optional connection pool.

```ts
import { Redis } from "@redfx/core"
import { IoRedis } from "@redfx/ioredis"
import { Config } from "effect"

const RedisLive = IoRedis.layerConfig(Config.string("REDIS_URL"))
// IoRedis.layer({ url, options, commandTimeout })   single connection
// IoRedis.layerPooled({ url, size: 10 })            pooled commands, dedicated pub/sub
```

Peer dependencies: `effect`, `ioredis`, `@redfx/core`.

## License

MIT
