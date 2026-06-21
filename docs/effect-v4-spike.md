# Effect v4 support — compatibility spike

> Branch: `spike/effect-v4`. This is a **throwaway spike**, not a migration. It exists to
> turn "what would it take to support Effect v4" into verified compiler facts.

## Status of Effect v4 (June 2026)

- v4 is **beta only** — latest is `effect@4.0.0-beta.85`. The Effect team's own guidance:
  *"If you're running Effect in production, v3 remains our recommended choice."*
- APIs (especially **Schema**) still change between beta releases. Anything ported to the v4
  Schema today is likely rework at RC. **Do not ship a v4 port until v4 reaches RC.**

## How this spike was run

1. `pnpm add -D -w effect@4.0.0-beta.85`
2. Pointed every workspace package's `effect` devDependency at `4.0.0-beta.85` (otherwise
   `@redfx/core` resolves its own `effect@3.21.3` and you get false v3-vs-v4 type-identity
   errors — the first run reported 518 errors, ~26 of which were pure cross-version noise).
3. `./node_modules/.bin/tsc -p tsconfig.json --noEmit`

Result: **492 type errors** — but see below, the count is misleading.

## Can v3 and v4 be supported together?

Not from one source tree, and not via a single package with `effect: "^3 || ^4"` — the
`Context.Tag`→`Service` and Schema changes are mutually exclusive at the type level.
**Strategy: major-version split.** `@redfx/core@^3` peers `effect@^4`; keep the v3 line on a
`2.x` maintenance branch. (Same model Effect itself uses for v3↔v4.) Optionally a second
package name (`@redfx/core4`) if consumers need both installed at once during their own migration.

## The 492 errors are mostly cascades from a few root causes

| Area | Errors | Nature |
|---|---|---|
| `packages/core` | 262 | mostly cascades from `Context.Tag` + Schema (see below) |
| `test/` | 200 | downstream of core's broken public types + `Cause`/`Exit` shape |
| `examples/` | 17 | `Schema.TaggedError`, `Schema.Struct` arity, downstream |
| `packages/ioredis` | 7 | `Layer`/`Config`/`Stream` type identity |
| `packages/bun` | 6 | same |

### Verified GOOD news — the stable core ports unchanged

- **`internal/decode.ts` — 0 errors.** ~370 lines, every function returns
  `Effect.Effect<…, DecodeError>`. It uses only the stable core (`Effect.succeed/fail/map/
  flatMap/forEach/all`, `Option`), none of which changed. The Effect-dense decoder is portable.
- **`RespValue.ts` — 0 errors** (pure TS).
- `Effect.gen`, `Option`, `Ref`, `Chunk`, `Clock` usage: no breaks observed.

### Root-cause breaks (the actual work), verified against v4 dist types

1. **Schema — full rewrite. The dominant cost.** In `Redis.ts` (the `ref`/`setOf`/zset/hash/
   stream codecs), `Cache.ts`, `RateLimit.ts`, examples.
   - `Schema.parseJson` — **removed** (TS2339, ~7 sites in `Redis.ts`).
   - `Schema.decode` / `Schema.decodeUnknown` — **removed/renamed** (`Cache.ts:54`,
     `RateLimit.ts:56`, `Redis.ts:1029`).
   - `Schema.TaggedError` — **removed** (examples).
   - `Schema.Schema<T>` now requires explicit type args; base param is `Top`; the RD/RE
     (decode/encode requirement) split changes every codec signature (TS2314, TS2345 "not
     assignable to `Top`", ~dozens of sites).
   - `ParseResult` is **no longer a top-level `effect` export** (`Redis.ts:9`, TS2305) — moved
     under the Schema namespace; cascades into every `Effect<A, ParseResult.ParseError, R>`.
2. **Services — `Context.Tag` removed → `Context.Service`.** Only **2 definition sites**
   (`Redis.ts:732`, `Connection.ts:26`) but they cause the **largest cascade**: ~85 `TS18046
   'r' is unknown`, many `TS2345 typeof Redis not assignable`, and `TS2488 must have
   [Symbol.iterator]` errors all stem from the two services no longer being valid yieldables.
   Fixing these two definitions collapses a large fraction of the 262 core errors.
3. **`Data.TaggedError`** (`RedisError.ts`) — compiled, but verify error/`Cause` semantics; the
   200 test errors include `Cause`/`Exit` shape changes (Cause flattened tree→array in v4).
4. **Mechanical renames** (stable to fix, find/replace-scale):
   - `Schedule.union` → `Schedule.both`; `Schedule.whileInput` → `Schedule.check`/`while*`
     (`Redis.ts:300-301`, `Cache.ts:254`). `exponential`/`jittered`/`spaced` retained.
   - `Stream.repeatEffectChunk` — removed; rebuild the consumer loop on `Stream.fromPull`
     (`Redis.ts:325,371`).
   - `Effect.timeoutFail` → `timeout`/`timeoutOption` (`Redis.ts:557,565`);
     `Effect.catchAll` → `catch` (`Cache.ts:257`); `Effect.zipRight` → `andThen`
     (`Redis.ts:1019`).
   - `Layer.scoped` → `Layer.effect` (scope is implicit in v4) (`Redis.ts:1075`).
   - `Duration.DurationInput` type path moved + `Duration.decode` removed — affects
     `RedisCommand.ts` pervasively, but RedisCommand is otherwise pure string-building.

## Effort read

- **Mechanical renames (items 4): ~half a day.** Safe, bounded, find/replace-scale.
- **Services (item 2): ~half a day.** 2 definitions; collapses most of the cascade.
- **Schema (item 1): the real cost, 1.5–3 days, and unstable** — the API moves between betas.
  This is the part to defer until v4 RC.

## Recommendation

1. **Now:** keep this spike branch as the reference. Ship nothing.
2. **At v4 RC:** redo on a fresh `feat/effect-v4` branch — fix items 2 and 4 first (cheap, high
   cascade-collapse), then the Schema codecs. Lean on the Effect team's codemods for the renames.
3. **Release as a redfx major** with `peerDependencies: { effect: "^4.0.0" }`; keep v3 on a
   maintenance branch.

## Reproduce / revert

- Reproduce: this branch's `package.json` files pin `effect@4.0.0-beta.85` + a root
  `pnpm.overrides.effect`. Run `pnpm install --no-frozen-lockfile && ./node_modules/.bin/tsc -p tsconfig.json --noEmit`.
- Revert: `git checkout main` — `main` is untouched.
