# redfx

Ergonomic Redis for [Effect](https://effect.website): typed commands, Schema-typed keys, pub/sub as
a `Stream`, and distributed caching, over a small driver-agnostic port.

```ts
import { Redis } from "@redfx/core"
import { BunRedis } from "@redfx/bun"
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
| [`@redfx/core`](packages/core) | core: port, RESP model, typed commands, `Redis.ref`, pub/sub, `script`, `RedisCache`, tracing, layers. Depends on `effect` only. |
| [`@redfx/ioredis`](packages/ioredis) | adapter over `ioredis.sendCommand`. |
| [`@redfx/bun`](packages/bun) | adapter over Bun's native `RedisClient`. |

## Install

```sh
npm install @redfx/core @redfx/ioredis ioredis effect   # Node + ioredis
bun add @redfx/core @redfx/bun effect                    # Bun
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

### Typed collections

The same `ref` story extends to sets, sorted sets, and hashes. `Redis.setOf` / `Redis.sortedSet` /
`Redis.hashOf` tie a key prefix to a Schema; members (set/zset) and values (hash) each round-trip as
one JSON value, scores stay plain numbers, and hash fields stay raw strings.

```ts
const userTags = Redis.setOf(Tag, { prefix: "user:tags", ttl: Duration.hours(24) })
yield* userTags(userId).add("vip", "beta")
const tags = yield* userTags(userId).members // ReadonlyArray<Tag>, decoded

const board = Redis.sortedSet(Player, { prefix: "game:scores" })
yield* board(season).add(ada, 100)
const total = yield* board(season).incrBy(ada, 50) // new score
const top10 = yield* board(season).rangeWithScores(0, 9) // ReadonlyArray<[Player, number]>

const sessions = Redis.hashOf(Session, { prefix: "user:sessions", ttl: Duration.days(7) })
yield* sessions(userId).set("device:abc", { ip, lastSeen }) // field(string) → value(Session)
const all = yield* sessions(userId).getAll // ReadonlyMap<string, Session>
```

A hash is a `field(string) → value(A)` map, not a struct spread across fields (use `Redis.ref` with a
`Schema.Struct` for that). TTL is a key-level concern: `SADD`/`ZADD`/`HSET` take no inline `EX`, so a
configured `ttl` is re-stamped with a follow-up `EXPIRE` after each mutating write — two round-trips,
not atomic. Pass `{ keepTtl: true }` to leave it untouched, or reach for `Redis.script` when you need
add+expire to be atomic. Decoding fails the whole read on the first bad element (like `ref.get`).

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

### Streams

`Redis.stream` is the durable counterpart to pub/sub: a Schema-typed, replayable append-only log.
Each entry is one `A` encoded as JSON under a single field `"d"`, so a stream reads as a typed log of
`A` — the analogue of `subscribe(channel, Schema)`, but persisted and re-readable.

```ts
const OrderEvent = Schema.Struct({ orderId: Schema.String, kind: Schema.String, at: Schema.Number })
const orders = Redis.stream(OrderEvent, { prefix: "events:orders" })

yield* orders(region).add(event, { maxLen: 1000, approx: true }) // append, approx MAXLEN trim
const recent = yield* orders(region).range("-", "+", { count: 50 }) // page history

// Live tail as a Stream: only entries appended after the read begins (from: "$").
const tail = orders(region).read({ from: "$" }).pipe(
  Stream.runForEach((e) => handle(e.message)), // e: { id, message: OrderEvent }
)
```

The blocking read runs on a **dedicated connection** (like `subscribe`), so `XREAD BLOCK` never rides
the pooled command path or trips `commandTimeout`. On a dropped connection the reader fails fast,
reconnects with capped backoff, and resumes from the last delivered id; `from: "$"` is a best-effort
live tail (the window before the first entry can miss a reconnect). A *silently* half-open socket is
caught by TCP keepalive on the ioredis adapter; Bun's client exposes no keepalive, so a silent
partition can stall a Bun consumer until the socket actually closes.

Consumer groups give at-least-once delivery across workers. `consume` runs a handler per entry and
acks **only after it succeeds** — an unhandled handler or ack error terminates the consumer and leaves
that entry pending (recover it with `claimStale`), so catch inside your handler if it should outlive a
bad message. The lower-level `group`/`streams.readGroup` hand you each entry with a manual
`ack: Effect<void>` instead. `streams.claimStale` is one `XAUTOCLAIM` round for recovering a dead
consumer's pending entries (automatic claiming / dead-lettering is deferred).

```ts
const worker = orders(region)
  .consume({ group: "fulfilment", consumer: "w1" }, (event) => handle(event))
  .pipe(Stream.runDrain) // acks each entry after handle succeeds
```

A decode failure poisons the read on that entry (like `ref.get`). For raw, multi-field entries or the
full command surface (`xadd`/`xrange`/`xreadGroup`/`xpending`/…), reach past the typed ref to the
service methods. Multi-stream `XREAD` is single-key per call in cluster mode (`CROSSSLOT` otherwise).

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
| [`collection-set.ts`](examples/collection-set.ts) | `Redis.setOf` — a Schema-typed set (tags/membership) |
| [`leaderboard.ts`](examples/leaderboard.ts) | `Redis.sortedSet` — a leaderboard with `incrBy`/`rangeWithScores`/`rank` |
| [`hash-map.ts`](examples/hash-map.ts) | `Redis.hashOf` — a Schema-typed `field → value` hash map |
| [`pubsub.ts`](examples/pubsub.ts) | pub/sub as a filtered `Stream` over a dedicated connection |
| [`event-log.ts`](examples/event-log.ts) | `Redis.stream` — a typed append-only log: `add`/`range`, a live tail, and a consumer group |
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
  // a fresh, un-pooled, un-timed connection for blocking reads — the Stream owns and closes it
  readonly dedicated: <A, E>(f: (conn: ConnectionService) => Stream<A, E>) => Stream<A, E | RedisError>
  readonly close: Effect<void>
}
type RedisCommand = { readonly name: string; readonly args: ReadonlyArray<string | Uint8Array> }
```

It maps directly onto `ioredis.sendCommand` and Bun's `redis.send`.

## Scope and limitations

redfx covers the common string/key commands (`get set del exists expire ttl incr decr incrBy
decrBy mget mset getDelete publish ping`), hashes (`hset hget hgetAll hdel hexists hincrBy hkeys
hvals hlen hmget`), sets (`sadd srem smembers sismember scard spop`), sorted sets (`zadd zrem
zscore zincrBy zcard zrank zrange zrangeWithScores`), streams (`xadd xrange xrevrange xlen xtrim
xdel xread` plus consumer groups `xgroupCreate xreadGroup xack xpending xclaim xautoclaim`), the
Schema-typed refs (`Redis.setOf`/`sortedSet`/`hashOf`/`stream`), `call`, `pipeline`, `Redis.ref`,
`Redis.script`, pub/sub, blocking consumer `Stream`s on a dedicated connection, `RedisCache`
(distributed cache-aside with optional stampede protection, plus a two-tier L1+L2 cache with pub/sub
invalidation), per-command tracing (`Effect.withSpan`), pooled layers, and an optional
`commandTimeout` that surfaces as `TimeoutError`.

Redis Cluster (`IoRedis.layerCluster`) and Sentinel (ioredis `options: { sentinels, name }`) are
supported on the ioredis adapter; Bun's client is single-node, so neither is available there. In
cluster mode, single-key commands route to the owning slot, but cross-slot multi-key commands
(`mget`/`mset`, a multi-key `pipeline`, multi-`KEYS` scripts) need a shared hash tag `{…}`.

Not here yet: command codegen.

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
