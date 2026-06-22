# Effect v4 port — verified

> Branch: `spike/effect-v4`. This started as a throwaway probe; it is now a **complete, verified
> port** of redfx to `effect@4.0.0-beta.85`. `main` is untouched.

## Result

- `tsc -p tsconfig.json --noEmit`: **0 errors** across all packages, examples, and tests
  (was 492 on first contact with v4).
- `bun test test/` (live Redis via testcontainers): **138 pass, 3 skip, 0 fail**.
  The 3 skips are `ioredis-cluster.test.ts`, gated on cluster nodes being configured — pre-existing,
  not v4-related.

This proves the port is correct at runtime, not just type-level.

## Status of Effect v4 (June 2026)

v4 is **beta** (`effect@4.0.0-beta.85`); the Effect team still recommends v3 for production and
Schema's API can shift between betas. So: this branch is the reference, **don't ship until v4 RC**.
When shipping, bump redfx to a new major with `peerDependencies: { effect: "^4.0.0" }` and keep the
v3 line on a `2.x` maintenance branch.

## How it was verified

1. Pinned every workspace package's `effect` dep to `4.0.0-beta.85` (a single version everywhere —
   a split install produces false v3-vs-v4 type-identity errors).
2. Ported file-by-file, recompiling after each, using v4's installed `dist/*.d.ts` as the source of
   truth for every replacement (not migration blogs — see "corrections" below).
3. Ran the full runtime suite against a real Redis container.

## The shape of the change

The error count was dominated by **cascades** from a few roots, not by breadth:

- `internal/decode.ts` (~370 lines of `Effect`/`Option`) and `RespValue.ts` ported with **zero
  changes** — the stable core didn't move.
- Two `Context.Tag` → `Context.Service` definitions collapsed ~150 of the 262 core errors.
- Schema was the only deep rewrite, and even it was mechanical once the right symbols were found.

## Verified v4 mappings (what actually compiles + passes tests)

| v3 | v4 | Notes |
|---|---|---|
| `Context.Tag("k")<Self,Shape>()` | `Context.Service<Self,Shape>()("k")` | key moves to 2nd call; the service still `extends Effect`, so `yield*`/`flatMap`/`.use` keep working |
| `Schema.parseJson(s)` | `Schema.fromJsonString(s)` | |
| `Schema.encode/decode(c)` | `Schema.encodeEffect/decodeEffect(c)` | error is `SchemaError`, not `ParseResult.ParseError` |
| `Schema.decodeUnknown(s)` | `Schema.decodeUnknownEffect(s)` | |
| `Schema.Schema<A,I>` | `Schema.Codec<A,I>` | `Schema<T>` is decoded-type-only now |
| `Schema.Literal(a,b,…)` | `Schema.Literals([a,b,…])` | single-arg `Literal` unchanged |
| `Schema.Record({key,value})` | `Schema.Record(key, value)` | positional |
| `Schema.Tuple(a,b,c)` | `Schema.Tuple([a,b,c])` | array arg |
| `Schema.TaggedError<S>()("T",f)` | `Schema.TaggedErrorClass<S>()("T",f)` | same call shape, auto-`_tag` |
| `ParseResult` (top-level) | (gone) | use `SchemaError` / `Schema.*` |
| `Schedule.union(s)` | `Schedule.either(s)` | **recur-if-either, MIN delay.** `both` is the intersection (MAX delay) — wrong here, and the runtime test caught it |
| `Schedule.whileInput(p)` | `Schedule.tapInput((e) => p(e) ? Effect.void : Effect.fail(e))` | v4 has no `whileInput`/`while`/`recurWhile`, and `Stream.retry` takes no `while` option — so the predicate must live in the schedule, halting via its Error channel. Both branches are runtime-tested: reconnect-on-`ConnectionError` and fail-fast-on-`WRONGTYPE` |
| `Stream.repeatEffectChunk(eff)` | `Stream.forever(Stream.fromArrayEffect(eff))` | switch the per-iteration `Chunk` to an array |
| `Stream.asyncScoped((emit)=>…)` | `Stream.callback((queue)=>…)` | push with `Queue.offerUnsafe`; fail with `Queue.failCauseUnsafe(q, Cause.fail(e))` |
| `Stream.unwrapScoped` | `Stream.unwrap` | scope is implicit |
| `Stream.runCollect` → `Chunk<A>` | → `Array<A>` | drop downstream `Chunk.toReadonlyArray` |
| `Effect.catchAll` | `Effect.catch` | exported as `catch_ as catch` (no bare-name decl, but it exists) |
| `Effect.timeoutFail({duration,onTimeout})` | `Effect.timeoutOrElse({duration, orElse: () => Effect.fail(...)})` | onTimeout returns a value → orElse returns an Effect |
| `Effect.zipRight` | `Effect.andThen` | |
| `Effect.either` | `Effect.result` | |
| `Effect.fork` | `Effect.forkChild` | |
| `Cause.failureOption` | `Cause.findErrorOption` | Cause is a flat reason-array in v4 |
| `Layer.scoped` | `Layer.effect` | scope implicit |
| `Layer.unwrapEffect` | `Layer.unwrap` | |
| `ConfigError.ConfigError` | `Config.ConfigError` | no top-level `ConfigError` export |
| `Duration.DurationInput` | `Duration.Input` | and `Duration.decode(d)` is gone — pass input straight to `toMillis`/`toSeconds` |

### One public-API change forced by v4

`Redis.use` (redfx's Stream-opening helper) was renamed to **`Redis.useStream`** — v4's
`Context.Service` reserves `.use` for `Effect`s, and the two signatures collide on the class's
static side.

## Corrections to my earlier (pre-port) guesses

The earlier draft of this doc listed mappings from migration blogs that did **not** survive contact
with the compiler:

- `Effect.catchAll → catch` — I'd marked "no bare `Effect.catch`"; it does exist (aliased).
- `Schedule.whileInput → check` — there is no `check`; the answer is `tapInput`.
- `Schedule.union → both` — **wrong**; it's `either`. `both` typechecks but is the wrong schedule
  (max-delay intersection), so the reconnect test timed out until corrected to `either`.

Lesson: typecheck-green ≠ correct. The runtime suite is what caught the `both`/`either` bug.

## Reproduce / revert

- Reproduce: `pnpm install --no-frozen-lockfile && ./node_modules/.bin/tsc -p tsconfig.json --noEmit && bun test test/`
- Revert: `git checkout main` — `main` is untouched.
