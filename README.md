# redfx

Ergonomic Redis for [Effect](https://effect.website): typed commands, Schema-typed keys, pub/sub as
a `Stream`, and distributed caching, over a small driver-agnostic port.

```ts
import { Redis } from "redfx"
import { BunRedis } from "redfx-bun"
import { Config, Duration, Effect } from "effect"

const program = Effect.gen(function* () {
  const redis = yield* Redis
  yield* redis.set("greeting", "hello", { ex: Duration.minutes(15) })
  const greeting = yield* redis.get("greeting") // Option<string>, not string | null
})

program.pipe(Effect.provide(BunRedis.layerConfig(Config.string("REDIS_URL"))), Effect.runPromise)
```

## What it does

redfx is a typed Redis client for Effect. Nullable reads return `Option`, failures go in the error
channel as a tagged `RedisError`, keys are typed with `Schema`, pub/sub is exposed as a `Stream`,
and connections are managed by `Scope`. It also provides read-through and two-tier caches. The typed
layer sits on a small `send`-level port, so it's driver-agnostic: the same code runs on the ioredis
or Bun adapter.

## Packages

| Package | Contents |
| --- | --- |
| [`redfx`](packages/redfx) | core: port, RESP model, typed commands, `Redis.ref`, pub/sub, `script`, `RedisCache`, tracing, layers. Depends on `effect` only. |
| [`redfx-ioredis`](packages/redfx-ioredis) | adapter over `ioredis.sendCommand`. |
| [`redfx-bun`](packages/redfx-bun) | adapter over Bun's native `RedisClient`. |

## Install

```sh
npm install redfx redfx-ioredis ioredis effect   # Node + ioredis
bun add redfx redfx-bun effect                    # Bun
```

`effect` is a peer dependency (`^3.21`).

## Usage

Layers come from the adapter:

```ts
const RedisLive = IoRedis.layerConfig(Config.string("REDIS_URL"))
const RedisLive = IoRedis.layerPooled({ url, size: 10 })
const RedisLive = IoRedis.layerCluster({ nodes: [{ host: "127.0.0.1", port: 7000 }] })
const RedisLive = BunRedis.layerConfig(Config.string("REDIS_URL"))
```

The connection is a scoped resource: it connects on acquire and sends `QUIT` on release.

Commands return `Option` for nullable reads and put failures in the error channel:

```ts
const program = Effect.gen(function* () {
  const redis = yield* Redis
  yield* redis.set("greeting", "hello", { ex: Duration.minutes(15) })
  const v = yield* redis.get("greeting") // Option<string>
  const n = yield* redis.incr("counter") // number
  const pong = yield* redis.call("PING") // RespValue, the raw escape hatch
}) // Effect<void, RedisError, Redis>
```

`RedisError` is a tagged union of `ConnectionError | CommandError | DecodeError | TimeoutError`.

### Schema-typed keys

`Redis.ref` ties a key prefix to a Schema, so encode/decode and TTL are handled for you:

```ts
const OtpRecord = Schema.Struct({ codeHash: Schema.String, attempts: Schema.Number })
const otp = Redis.ref(OtpRecord, { prefix: "otp", ttl: Duration.minutes(15) })

yield* otp(emailHash).set({ codeHash, attempts: 0 })
const rec = yield* otp(emailHash).get // Option<OtpRecord>, decode errors are typed
yield* otp(emailHash).update((r) => ({ ...r, attempts: r.attempts + 1 }), { keepTtl: true })
const consumed = yield* otp(emailHash).getDelete // atomic GETDEL
```

That replaces the usual `JSON.parse(await redis.get(key))`, manual null checks, the `"KEEPTTL"`
string argument, and count-returning `del`.

### Fail open, precisely

Because the errors are tagged, you can fail open on a lost connection without swallowing real bugs:

```ts
const rateLimit = (key: string, limit: number, window: Duration.Duration) =>
  Effect.gen(function* () {
    const redis = yield* Redis
    const count = yield* redis.incr(key)
    if (count === 1) yield* redis.expire(key, window)
    return { allowed: count <= limit, reset: yield* redis.ttl(key) }
  }).pipe(
    // only a connection loss fails open; a WRONGTYPE bug still surfaces
    Effect.catchTag("ConnectionError", () => Effect.succeed({ allowed: true, reset: Option.some(window) })),
  )
```

### Pipelines and Lua

```ts
yield* redis.pipeline([Cmd.incr("c"), Cmd.expire("c", Duration.seconds(60))])

// EVALSHA with an automatic SCRIPT LOAD fallback on NOSCRIPT.
// numkeys is taken from the keys array, so the same script can take a varying number of keys.
const publish = Redis.script({
  result: Schema.Number,
  lua: "local id = redis.call('INCR', KEYS[1]) ... redis.call('PUBLISH', KEYS[3], payload) return id",
})
const id = yield* publish([counterKey, replayKey, channel], [template, "99", "3600"])
```

### Pub/Sub as a Stream

`subscribe` runs on a dedicated connection, decodes with a Schema, and unsubscribes when the
stream's scope closes:

```ts
const SseEnvelope = Schema.Struct({
  id: Schema.String, topic: Schema.String, userId: Schema.String,
  type: Schema.String, data: Schema.Unknown,
})

const userEvents = (topic: string, userId: string) =>
  Redis.use((redis) => redis.subscribe("sse:events", SseEnvelope)).pipe(
    Stream.filter((e) => e.topic === topic && e.userId === userId),
  )
```

No `duplicate()`, no manual listener bookkeeping, no `try/JSON.parse/catch` in your code.

### Caching

Effect's own caching primitives (`Cache.make`, `Effect.cachedWithTTL`) are in-process: they don't
survive a restart and aren't shared across instances. `RedisCache` fills that gap with two
constructors.

`RedisCache.make` is a distributed read-through cache-aside, built on `Redis.ref` for storage. It's
a plain builder, like `ref`: methods thread `Redis` (and the lookup's `R`) through the channel.

```ts
const users = RedisCache.make({
  schema: User,
  prefix: "cache:user",
  ttl: Duration.minutes(10),
  lookup: (id) => fetchUserFromDb(id), // Effect<User, DbError, Db>
})

const user = yield* users.get(userId) // Effect<User, DbError | RedisError, Db | Redis>
yield* users.invalidate(userId) // on a write
```

Unlike `ref` (a typed *cell* where absence is normal, so reads are `Option`), a cache is
read-through: `get` returns `A`, computing on a miss. Model genuine absence in the schema
(`Schema.NullOr`/`Schema.Option`) or in the lookup's error channel.

Add `stampede: true` for cross-instance single-flight on a miss, so a cold key under load computes
once (via a Redis lock) instead of once per caller. It defaults to `onTimeout: "compute"`
(availability-first: a slow degrades to a lock-free recompute rather than failing); use
`{ onTimeout: "fail" }` for strict single-flight, which surfaces a `TimeoutError`.

```ts
const prices = RedisCache.make({
  schema: Price,
  prefix: "cache:price",
  ttl: Duration.seconds(30),
  stampede: true, // or { lockTtl: "5 seconds", onTimeout: "fail" }
  lookup: (sym) => fetchQuote(sym),
})
```

`RedisCache.makeTiered` adds an in-process L1 (an Effect `Cache`) in front of the Redis L2, kept
coherent across instances by pub/sub invalidation. It's a scoped `Effect` (it forks a listener
fiber); the handle it yields is context-free — `Redis` and `R` are discharged at construction, just
like `Cache.make`.

```ts
const program = Effect.gen(function* () {
  const flags = yield* RedisCache.makeTiered({
    schema: FeatureFlags,
    prefix: "cache:flags",
    ttl: Duration.minutes(30),
    memory: { capacity: 1000, ttl: Duration.minutes(5) }, // keep memory.ttl <= ttl
    lookup: (tenant) => loadFlags(tenant),
  })

  const f = yield* flags.get(tenantId) // L1 → L2 → origin; Effect<FeatureFlags, RedisError>
  yield* flags.invalidate(tenantId) // drops L1 + L2 here, publishes so other instances drop their L1
}).pipe(Effect.scoped)
```

A `set`/`invalidate`/`refresh` on one instance publishes on `redfx:cache:inv:<prefix>`; every other
instance's listener drops the matching L1 entry. Invalidation is at-most-once and TTL-bounded — a
publish failure never fails the authoritative L2 write, and a dropped subscription reconnects with
backoff. Keep `memory.ttl <= ttl` so L1 never serves a value Redis has already expired; the
constructor warns if it doesn't. Note the 1s floor on L2 TTL (Redis `EX` is whole seconds). There's
no `invalidateAll` — bump a segment in `prefix` to version a whole namespace instead.

## Examples

Runnable, type-checked walkthroughs live in [`examples/`](examples) (each supplies a real adapter
layer; the origin calls are stubbed):

| File | Shows |
| --- | --- |
| [`schema-ref.ts`](examples/schema-ref.ts) | `Redis.ref` — a Schema-typed key with encode/decode + TTL folded in |
| [`pubsub.ts`](examples/pubsub.ts) | pub/sub as a filtered `Stream` over a dedicated connection |
| [`rate-limit.ts`](examples/rate-limit.ts) | a fail-open rate limiter (`incr`/`expire`, `catchTag` on `ConnectionError`) |
| [`lua-script.ts`](examples/lua-script.ts) | `Redis.script` — EVALSHA with a NOSCRIPT fallback |
| [`cache-aside.ts`](examples/cache-aside.ts) | distributed read-through cache (`get`/`set`/`invalidate`/`refresh`) |
| [`stampede.ts`](examples/stampede.ts) | single-flight on a hot key; availability-first default vs. strict `onTimeout: "fail"` |
| [`two-tier.ts`](examples/two-tier.ts) | L1 + L2 with cross-instance pub/sub invalidation |
| [`negative-caching.ts`](examples/negative-caching.ts) | caching a known-absent value with `Schema.NullOr` |

## The port

Adapters implement one small interface — a `send`, plus `pipeline`, `subscribe`, and `close`:

```ts
interface ConnectionService {
  readonly send: (cmd: RedisCommand) => Effect<RespValue, RedisError>
  readonly pipeline: (cmds: ReadonlyArray<RedisCommand>) => Effect<ReadonlyArray<RespValue>, RedisError>
  readonly subscribe: (channels: ReadonlyArray<string>) => Stream<PushMessage, RedisError>
  readonly close: Effect<void>
}
type RedisCommand = { readonly name: string; readonly args: ReadonlyArray<string | Uint8Array> }
```

It maps directly onto `ioredis.sendCommand` and Bun's `redis.send`.

## Scope and limitations

redfx covers the common string/key commands (`get set del exists expire ttl incr decr incrBy
decrBy mget mset getDelete publish ping`), `call`, `pipeline`, `Redis.ref`, `Redis.script`,
pub/sub, `RedisCache` (distributed cache-aside with optional stampede protection, plus a two-tier
L1+L2 cache with pub/sub invalidation), per-command tracing (`Effect.withSpan`), pooled layers, and
an optional `commandTimeout` that surfaces as `TimeoutError`.

Redis Cluster (`IoRedis.layerCluster`) and Sentinel (ioredis `options: { sentinels, name }`) are
supported on the ioredis adapter; Bun's client is single-node, so neither is available there. In
cluster mode, single-key commands route to the owning slot, but cross-slot multi-key commands
(`mget`/`mset`, a multi-key `pipeline`, multi-`KEYS` scripts) need a shared hash tag `{…}`.

Not here yet: hashes/zsets/sets/streams and command codegen.

Known limitations:

- Values are treated as UTF-8 text. The port accepts `Uint8Array` args, but the Bun adapter
  decodes them with `TextDecoder` and replies come back as strings, so non-UTF-8 binary won't
  round-trip yet.
- `commandTimeout` is a client-side deadline: it interrupts the waiting fiber with `TimeoutError`,
  but the command may still have reached the server. Set it per layer, e.g.
  `IoRedis.layer({ url, commandTimeout: "2 seconds" })`.

## Development

```sh
pnpm install
pnpm build       # tsc, in dependency order (core then adapters)
pnpm typecheck
pnpm test        # conformance suite against ioredis and Bun, plus codec property tests
```

## License

MIT
