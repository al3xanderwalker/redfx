# Effect v4 port — verified

> Branch: `v4`. This started as a throwaway probe; it is now a **complete, verified port** of redfx
> to `effect@4.0.0-beta.85`, packaged for release on the `beta` dist-tag. `main` stays the v3 line.

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

## Release & install

The v3 and v4 lines ship as the same package names on **different npm dist-tags + majors**, so they
coexist and a consumer's installed `effect` major selects the right one:

| line | redfx | dist-tag | peer |
| --- | --- | --- | --- |
| v3 (current) | `1.x` | `latest` | `effect@^3.21` |
| v4 (now, beta) | `2.0.0-beta.x` | `beta` | `effect@^4.0.0-beta.85` |
| v4 (after Effect v4 GA) | `2.x` | `latest` | `effect@^4` |

Install: `npm i @redfx/core@beta @redfx/ioredis@beta effect@beta` (v3 users on `latest` are untouched).

### Why the peer is a range, not an exact pin

`peerDependencies.effect` is `^4.0.0-beta.85`, not `4.0.0-beta.85`. An exact pin would make redfx
*uninstallable-clean* the moment Effect ships its next beta: under npm the mismatch is a hard
`ERESOLVE` failure, under pnpm/yarn a warning that installs a mismatched beta anyway. The caret range
accepts later `4.0.0` betas **and** stable `4.0.0` (verified via semver), but **not** `4.1.0-beta`.
Reproducibility is the consumer's lockfile's job, plus the "tested against beta.85" note — not the
peer range. See the conversation in this file's history for the verified semver matrix.

### Wiring (done on this branch)

- `package.json` ×3: version `2.0.0-beta.0`, peer `effect@^4.0.0-beta.85` (adapters' `@redfx/core`
  peer is `workspace:^` → rewrites to `^2.0.0-beta.0` on publish).
- `.github/workflows/release.yml`: publishes prereleases to `--tag beta`, stable to `latest`
  (npm tags every publish `latest` by default — a beta would otherwise clobber the v3 `latest`).
- `.github/workflows/effect-beta-watch.yml`: scheduled canary re-running the suite against the
  current `effect@beta`, so beta drift surfaces as a loud CI failure (`workflow_dispatch` works now;
  the `schedule` only auto-fires once `v4` is the repo's default branch — a GitHub constraint).

The one remaining manual step is publishing: bump `2.0.0-beta.N`, cut a GitHub **pre-release**, and
the workflow does the rest. Hold the *stable* `2.0.0`/`latest` promotion until Effect v4 itself GAs.

## Reproduce / revert

- Reproduce: `pnpm install --no-frozen-lockfile && ./node_modules/.bin/tsc -p tsconfig.json --noEmit && bun test test/`
- Revert: `git checkout main` — `main` is the untouched v3 line.
