# redfx-bun

[Bun](https://bun.sh) driver for [redfx](https://www.npmjs.com/package/redfx). It implements the
`send`-level port over Bun's native `RedisClient` (`Bun.redis`), with pub/sub delivered as a `Stream`.

```ts
import { Redis } from "redfx"
import { BunRedis } from "redfx-bun"
import { Config } from "effect"

const RedisLive = BunRedis.layerConfig(Config.string("REDIS_URL"))
// BunRedis.layer({ url, options, commandTimeout })   single connection
// BunRedis.layerPooled({ url, size: 10 })            pooled commands, dedicated pub/sub
```

Requires the Bun runtime. Peer dependencies: `effect`, `redfx`.

A few Bun-specific notes:

- `EXISTS` returns a boolean on Bun where ioredis returns 0/1; redfx normalises this so the command
  surface (`exists` returns `boolean`) matches across adapters.
- Bun retries a lost connection with backoff before a command rejects (around 30s by default). If
  you rely on the `catchTag("ConnectionError")` fail-open pattern, set tighter client options or a
  `commandTimeout`, e.g. `BunRedis.layer({ url, commandTimeout: "2 seconds" })`.
- Values are treated as UTF-8 text in v0.1: byte args are decoded with `TextDecoder` and replies
  come back as strings, so non-UTF-8 binary won't round-trip. Use base64 for binary payloads.

## License

MIT
