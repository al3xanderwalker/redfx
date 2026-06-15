import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Cmd,
  CommandError,
  RateLimit,
  Redis,
  RedisCache,
  type RedisError,
  type RedisService,
} from "@redfx/core";
import {
  Cause,
  Chunk,
  type ConfigError,
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";

// Run against every adapter, so parity proves the ergonomic layer is driver-agnostic.
export interface ConformanceAdapter {
  readonly name: string;
  readonly layer: (url: string) => Layer.Layer<Redis, RedisError>;
  readonly unreachableLayer: (url: string) => Layer.Layer<Redis, RedisError>;
  readonly pooledLayer: (url: string) => Layer.Layer<Redis, RedisError>;
  readonly configLayer: (url: string) => Layer.Layer<Redis, RedisError | ConfigError.ConfigError>;
}

export const runConformance = (adapter: ConformanceAdapter) => {
  describe(`redfx conformance: ${adapter.name}`, () => {
    let container: StartedTestContainer;
    let url = "";

    beforeAll(async () => {
      // Wait on a log line: testcontainers' default port check uses `docker exec`, which hangs under Bun.
      container = await new GenericContainer("redis:7-alpine")
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
        .start();
      url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
    }, 120_000);

    afterAll(async () => {
      await container?.stop();
    });

    const run = <A>(program: Effect.Effect<A, RedisError, Redis>): Promise<A> =>
      Effect.runPromise(Effect.provide(program, adapter.layer(url)));

    const runExit = <A>(program: Effect.Effect<A, RedisError, Redis>) =>
      Effect.runPromiseExit(Effect.provide(program, adapter.layer(url)));

    const failureTag = <A>(exit: Exit.Exit<A, RedisError>): RedisError | null =>
      Exit.isFailure(exit) ? Option.getOrNull(Cause.failureOption(exit.cause)) : null;

    test("get / set / getDelete are Option-typed", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const before = yield* redis.get("k:str");
          yield* redis.set("k:str", "hello");
          const present = yield* redis.get("k:str");
          const consumed = yield* redis.getDelete("k:str");
          const after = yield* redis.get("k:str");
          return { before, present, consumed, after };
        }),
      );
      expect(Option.isNone(result.before)).toBe(true);
      expect(result.present).toEqual(Option.some("hello"));
      expect(result.consumed).toEqual(Option.some("hello"));
      expect(Option.isNone(result.after)).toBe(true);
    });

    test("exists returns a boolean for key presence", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const missing = yield* redis.exists("k:exists");
          yield* redis.set("k:exists", "1");
          const present = yield* redis.exists("k:exists");
          return { missing, present };
        }),
      );
      expect(result.missing).toBe(false);
      expect(result.present).toBe(true);
    });

    test("incr / decr / incrBy return numbers", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const one = yield* redis.incr("k:n");
          const three = yield* redis.incrBy("k:n", 2);
          const two = yield* redis.decr("k:n");
          return { one, three, two };
        }),
      );
      expect(result).toEqual({ one: 1, three: 3, two: 2 });
    });

    test("ttl / expire surface Option<Duration>", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("k:ttl", "v");
          const noExpiry = yield* redis.ttl("k:ttl");
          const set = yield* redis.expire("k:ttl", Duration.seconds(100));
          const ttl = yield* redis.ttl("k:ttl");
          const missing = yield* redis.ttl("k:ttl:absent");
          return { noExpiry, set, ttl, missing };
        }),
      );
      expect(Option.isNone(result.noExpiry)).toBe(true);
      expect(result.set).toBe(true);
      expect(Option.isNone(result.missing)).toBe(true);
      expect(Option.isSome(result.ttl)).toBe(true);
      const seconds = Duration.toSeconds(Option.getOrThrow(result.ttl));
      expect(seconds).toBeGreaterThan(90);
      expect(seconds).toBeLessThanOrEqual(100);
    });

    test("ttlState distinguishes missing, persistent, and expiring keys", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("k:state:persist", "v");
          yield* redis.set("k:state:exp", "v", { ex: Duration.seconds(100) });
          return {
            missing: yield* redis.ttlState("k:state:absent"),
            persistent: yield* redis.ttlState("k:state:persist"),
            expiring: yield* redis.ttlState("k:state:exp"),
          };
        }),
      );
      expect(result.missing._tag).toBe("Missing");
      expect(result.persistent._tag).toBe("Persistent");
      expect(result.expiring._tag).toBe("Expires");
      if (result.expiring._tag === "Expires") {
        expect(Duration.toSeconds(result.expiring.duration)).toBeGreaterThan(90);
      }
    });

    test("mget / mset round-trip with Option holes", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.mset({ "k:a": "1", "k:b": "2" });
          return yield* redis.mget("k:a", "k:missing", "k:b");
        }),
      );
      expect(result).toEqual([Option.some("1"), Option.none(), Option.some("2")]);
    });

    test("set with EX, then KEEPTTL preserves expiry", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("k:keepttl", "a", { ex: Duration.seconds(100) });
          yield* redis.set("k:keepttl", "b", { keepTtl: true });
          const value = yield* redis.get("k:keepttl");
          const ttl = yield* redis.ttl("k:keepttl");
          return { value, ttl };
        }),
      );
      expect(result.value).toEqual(Option.some("b"));
      expect(Option.isSome(result.ttl)).toBe(true);
    });

    test("Redis.ref: Schema set / get / update / getDelete", async () => {
      const OtpRecord = Schema.Struct({ codeHash: Schema.String, attempts: Schema.Number });
      const otp = Redis.ref(OtpRecord, { prefix: "otp", ttl: Duration.minutes(15) });
      const result = await run(
        Effect.gen(function* () {
          yield* otp("user@example.com").set({ codeHash: "abc", attempts: 0 });
          const got = yield* otp("user@example.com").get;
          yield* otp("user@example.com").update((r) => ({ ...r, attempts: r.attempts + 1 }), {
            keepTtl: true,
          });
          const bumped = yield* otp("user@example.com").get;
          const consumed = yield* otp("user@example.com").getDelete;
          const after = yield* otp("user@example.com").get;
          return { got, bumped, consumed, after };
        }),
      );
      expect(result.got).toEqual(Option.some({ codeHash: "abc", attempts: 0 }));
      expect(result.bumped).toEqual(Option.some({ codeHash: "abc", attempts: 1 }));
      expect(result.consumed).toEqual(Option.some({ codeHash: "abc", attempts: 1 }));
      expect(Option.isNone(result.after)).toBe(true);
    });

    test("pipeline returns raw replies in order", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          return yield* redis.pipeline([
            Cmd.incr("k:pipe"),
            Cmd.incr("k:pipe"),
            Cmd.expire("k:pipe", Duration.seconds(60)),
          ]);
        }),
      );
      expect(result[0]).toBe(1);
      expect(result[1]).toBe(2);
      expect(Boolean(result[2])).toBe(true);
    });

    test("Cmd builders cover the command surface via pipeline", async () => {
      const replies = await run(
        Effect.flatMap(Redis, (redis) =>
          redis.pipeline([
            Cmd.set("c:k", "v"),
            Cmd.get("c:k"),
            Cmd.incr("c:n"),
            Cmd.incrBy("c:n", 4),
            Cmd.decr("c:n"),
            Cmd.decrBy("c:n", 2),
            Cmd.exists("c:k"),
            Cmd.mget("c:k", "c:missing"),
            Cmd.expire("c:k", Duration.seconds(60)),
            Cmd.ttl("c:k"),
            Cmd.getDelete("c:k"),
            Cmd.del("c:n"),
            Cmd.publish("c:ch", "x"),
            Cmd.raw("PING"),
          ]),
        ),
      );
      expect(replies[1]).toBe("v");
      expect(replies[2]).toBe(1);
      expect(replies[3]).toBe(5);
      expect(replies[4]).toBe(4);
      expect(replies[5]).toBe(2);
      expect(replies[7]).toEqual(["v", null]);
      expect(replies[10]).toBe("v");
      expect(replies[13]).toBe("PONG");
    });

    test("pub/sub: Schema-decoded delivery via Redis.use, with filter", async () => {
      const Envelope = Schema.Struct({ topic: Schema.String, n: Schema.Number });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const fiber = yield* Redis.use((r) => r.subscribe("redfx:chan", Envelope)).pipe(
            Stream.filter((e) => e.topic === "wanted"),
            Stream.take(1),
            Stream.runCollect,
            Effect.fork,
          );
          const publisher = yield* Effect.fork(
            Effect.gen(function* () {
              yield* redis.publish("redfx:chan", JSON.stringify({ topic: "ignored", n: 1 }));
              yield* redis.publish("redfx:chan", JSON.stringify({ topic: "wanted", n: 42 }));
            }).pipe(Effect.delay(Duration.millis(100)), Effect.forever),
          );
          const received = yield* Fiber.join(fiber).pipe(
            Effect.timeoutFail({
              duration: Duration.seconds(15),
              onTimeout: () => new CommandError({ message: "pub/sub timed out" }),
            }),
          );
          yield* Fiber.interrupt(publisher);
          return received;
        }),
      );
      expect(Chunk.toReadonlyArray(result)).toEqual([{ topic: "wanted", n: 42 }]);
    });

    test("Lua script: EVALSHA with NOSCRIPT fallback", async () => {
      const incr = Redis.script({
        result: Schema.Number,
        lua: "return redis.call('INCR', KEYS[1])",
      });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const first = yield* incr(["k:lua"]);
          yield* redis.call("SCRIPT", "FLUSH");
          const second = yield* incr(["k:lua"]);
          return { first, second };
        }),
      );
      expect(result.first).toBe(1);
      expect(result.second).toBe(2);
    });

    test("Lua script: multiple KEYS + ARGV with a typed result", async () => {
      const fanout = Redis.script({
        result: Schema.String,
        lua: "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return redis.call('GET', KEYS[1]) .. ':' .. redis.call('GET', KEYS[2])",
      });
      const result = await run(fanout(["k:s1", "k:s2"], ["hello"]));
      expect(result).toBe("hello:hello");
    });

    test("Lua script: numeric args are stringified for ARGV", async () => {
      const store = Redis.script({
        result: Schema.String,
        lua: "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
      });
      const result = await run(store(["k:numarg"], [42]));
      expect(result).toBe("42");
    });

    test("Lua script: numkeys follows the keys array (variadic)", async () => {
      const delAll = Redis.script({
        result: Schema.Number,
        lua: "for i = 1, #KEYS do redis.call('DEL', KEYS[i]) end; return #KEYS",
      });
      const counts = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.mset({ "v:a": "1", "v:b": "1", "v:c": "1" });
          const two = yield* delAll(["v:a", "v:b"]);
          const one = yield* delAll(["v:c"]);
          return { two, one };
        }),
      );
      expect(counts.two).toBe(2);
      expect(counts.one).toBe(1);
    });

    test("Lua script: result schema mismatch fails with DecodeError", async () => {
      const wrong = Redis.script({ result: Schema.Number, lua: "return 'not-a-number'" });
      const exit = await runExit(wrong(["k:badresult"]));
      expect(failureTag(exit)?._tag).toBe("DecodeError");
    });

    test("call escape hatch returns the raw reply", async () => {
      const pong = await run(Effect.flatMap(Redis, (r) => r.call("PING")));
      expect(pong).toBe("PONG");
    });

    test("command error (WRONGTYPE) surfaces as CommandError, not ConnectionError", async () => {
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.call("RPUSH", "k:wrongtype", "a");
          return yield* redis.get("k:wrongtype");
        }),
      );
      const error = failureTag(exit);
      expect(error?._tag).toBe("CommandError");
      expect(error?.message).toContain("WRONGTYPE");
    });

    test("Redis.ref: malformed stored value fails with DecodeError", async () => {
      const rec = Redis.ref(Schema.Struct({ n: Schema.Number }), { prefix: "redfx:decodefail" });
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("redfx:decodefail:x", "not json");
          return yield* rec("x").get;
        }),
      );
      expect(failureTag(exit)?._tag).toBe("DecodeError");
    });

    test("pub/sub: raw subscribe (no schema) delivers strings", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const fiber = yield* redis
            .subscribe("redfx:raw")
            .pipe(Stream.take(1), Stream.runCollect, Effect.fork);
          const publisher = yield* Effect.fork(
            redis
              .publish("redfx:raw", "plain-text")
              .pipe(Effect.delay(Duration.millis(100)), Effect.forever),
          );
          const received = yield* Fiber.join(fiber).pipe(
            Effect.timeoutFail({
              duration: Duration.seconds(15),
              onTimeout: () => new CommandError({ message: "pub/sub timed out" }),
            }),
          );
          yield* Fiber.interrupt(publisher);
          return received;
        }),
      );
      expect(Chunk.toReadonlyArray(result)).toEqual(["plain-text"]);
    });

    test("layerPooled: commands and pipeline work through the pool", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("k:pool", "v");
          const got = yield* redis.get("k:pool");
          const sum = yield* redis.pipeline([Cmd.incr("k:pool:n"), Cmd.incr("k:pool:n")]);
          return { got, sum };
        }).pipe(Effect.provide(adapter.pooledLayer(url))),
      );
      expect(result.got).toEqual(Option.some("v"));
      expect(result.sum[0]).toBe(1);
      expect(result.sum[1]).toBe(2);
    });

    test("layerPooled: concurrent commands stay consistent under contention", async () => {
      const total = await Effect.runPromise(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* Effect.forEach(
            Array.from({ length: 100 }),
            () => redis.incr("c:pool:concurrent"),
            {
              concurrency: "unbounded",
              discard: true,
            },
          );
          return yield* redis.get("c:pool:concurrent");
        }).pipe(Effect.provide(adapter.pooledLayer(url))),
      );
      expect(total).toEqual(Option.some("100"));
    });

    test("layerPooled: recovers after a server-side connection kill", async () => {
      const recover = Schedule.recurs(10).pipe(Schedule.addDelay(() => Duration.millis(100)));
      const value = await Effect.runPromise(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("c:kill", "before");
          yield* redis.call("CLIENT", "KILL", "TYPE", "normal").pipe(Effect.ignore);
          yield* redis.set("c:kill", "after").pipe(Effect.retry(recover));
          return yield* redis.get("c:kill").pipe(Effect.retry(recover));
        }).pipe(Effect.provide(adapter.pooledLayer(url))),
      );
      expect(value).toEqual(Option.some("after"));
    });

    test("a connection releases its socket on scope close", async () => {
      const connectedClients = (r: RedisService) =>
        r
          .call("INFO", "clients")
          .pipe(
            Effect.map((info) => Number(/connected_clients:(\d+)/.exec(String(info))?.[1] ?? "0")),
          );
      const { before, during } = await Effect.runPromise(
        Effect.gen(function* () {
          const control = yield* Redis;
          const before = yield* connectedClients(control);
          let during = before;
          yield* Effect.scoped(
            Layer.build(adapter.layer(url)).pipe(
              Effect.flatMap((context) =>
                Effect.gen(function* () {
                  const worker = Context.get(context, Redis);
                  yield* worker.ping;
                  during = yield* connectedClients(control);
                }),
              ),
            ),
          );
          // Poll until the worker's socket is gone; fails the test if it never drops.
          yield* connectedClients(control).pipe(
            Effect.flatMap((n) =>
              n <= before
                ? Effect.void
                : Effect.fail(new CommandError({ message: "connection not released" })),
            ),
            Effect.retry(Schedule.recurs(50).pipe(Schedule.addDelay(() => Duration.millis(100)))),
          );
          return { before, during };
        }).pipe(Effect.provide(adapter.layer(url))),
      );
      expect(during).toBeGreaterThan(before);
    });

    test("layerConfig builds a working layer from a Config", async () => {
      const pong = await Effect.runPromise(
        Effect.flatMap(Redis, (r) => r.ping).pipe(Effect.provide(adapter.configLayer(url))),
      );
      expect(pong).toBe("PONG");
    });

    test("connection failure surfaces as a tagged ConnectionError", async () => {
      const exit = await Effect.runPromiseExit(
        Effect.flatMap(Redis, (r) => r.ping).pipe(
          Effect.provide(adapter.unreachableLayer("redis://127.0.0.1:1")),
          Effect.timeout(Duration.seconds(20)),
        ),
      );
      expect(Exit.isFailure(exit)).toBe(true);
      const tag = Exit.isFailure(exit)
        ? Cause.failureOption(exit.cause).pipe(Option.map((e) => e._tag))
        : Option.none();
      expect(tag).toEqual(Option.some("ConnectionError"));
    });

    test("RedisCache: cache-aside runs lookup once, then serves warm", async () => {
      const result = await run(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const cache = RedisCache.make({
            schema: Schema.Number,
            prefix: "cache:aside",
            ttl: Duration.minutes(5),
            lookup: () => Ref.updateAndGet(calls, (n) => n + 1),
          });
          const cold = yield* cache.get("k");
          const warm = yield* cache.get("k");
          return { cold, warm, count: yield* Ref.get(calls) };
        }),
      );
      expect(result.cold).toBe(1);
      expect(result.warm).toBe(1);
      expect(result.count).toBe(1);
    });

    test("RedisCache: invalidate forces the next get to re-run lookup", async () => {
      const result = await run(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const cache = RedisCache.make({
            schema: Schema.Number,
            prefix: "cache:inv",
            ttl: Duration.minutes(5),
            lookup: () => Ref.updateAndGet(calls, (n) => n + 1),
          });
          yield* cache.get("k");
          yield* cache.invalidate("k");
          const after = yield* cache.get("k");
          return { after, count: yield* Ref.get(calls) };
        }),
      );
      expect(result.after).toBe(2);
      expect(result.count).toBe(2);
    });

    test("RedisCache: a value re-computes after its L2 ttl expires", async () => {
      const result = await run(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const cache = RedisCache.make({
            schema: Schema.Number,
            prefix: "cache:ttl",
            ttl: Duration.seconds(1), // floored to a 1s `ex`
            lookup: () => Ref.updateAndGet(calls, (n) => n + 1),
          });
          const first = yield* cache.get("k");
          yield* Effect.sleep(Duration.millis(1200));
          const second = yield* cache.get("k");
          return { first, second };
        }),
      );
      expect(result.first).toBe(1);
      expect(result.second).toBe(2);
    });

    test("RedisCache: stampede single-flights a cold key across concurrent gets", async () => {
      const result = await run(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const cache = RedisCache.make({
            schema: Schema.Number,
            prefix: "cache:stampede",
            ttl: Duration.minutes(5),
            stampede: true,
            lookup: () =>
              Effect.sleep(Duration.millis(200)).pipe(
                Effect.flatMap(() => Ref.updateAndGet(calls, (n) => n + 1)),
              ),
          });
          const values = yield* Effect.forEach(Array.from({ length: 8 }), () => cache.get("hot"), {
            concurrency: "unbounded",
          });
          return { values, count: yield* Ref.get(calls) };
        }),
      );
      expect(result.count).toBe(1);
      expect(result.values).toEqual(Array.from({ length: 8 }, () => 1));
    });

    test("RedisCache.makeTiered: L1 serves after the L2 key is deleted", async () => {
      const result = await run(
        Effect.scoped(
          Effect.gen(function* () {
            const redis = yield* Redis;
            const calls = yield* Ref.make(0);
            const cache = yield* RedisCache.makeTiered({
              schema: Schema.Number,
              prefix: "cache:tiered:l1",
              ttl: Duration.minutes(5),
              memory: { capacity: 64, ttl: Duration.minutes(1) },
              lookup: () => Ref.updateAndGet(calls, (n) => n + 1),
            });
            const warm = yield* cache.get("k");
            yield* redis.del("cache:tiered:l1:k"); // drop L2 out from under the cache
            const afterDel = yield* cache.get("k");
            return { warm, afterDel, count: yield* Ref.get(calls) };
          }),
        ),
      );
      expect(result.warm).toBe(1);
      expect(result.afterDel).toBe(1); // still the L1 value; the origin did not re-run
      expect(result.count).toBe(1);
    });

    test("RedisCache.makeTiered: invalidate on one instance drops the other's L1 via pub/sub", async () => {
      const result = await run(
        Effect.scoped(
          Effect.gen(function* () {
            const calls = yield* Ref.make(0);
            const options = {
              schema: Schema.Number,
              prefix: "cache:tiered:inv",
              ttl: Duration.minutes(5),
              memory: { capacity: 64, ttl: Duration.minutes(5) },
              lookup: () => Ref.updateAndGet(calls, (n) => n + 1),
            } as const;
            const cacheA = yield* RedisCache.makeTiered(options);
            const cacheB = yield* RedisCache.makeTiered(options);
            const warmA = yield* cacheA.get("k"); // origin runs once, warms L2 + A's L1
            const warmB = yield* cacheB.get("k"); // served from L2, warms B's L1, origin not re-run
            // Re-invalidate on a loop until B's listener has dropped its L1 (pub/sub is fire-and-forget).
            const invalidator = yield* Effect.fork(
              cacheA.invalidate("k").pipe(Effect.delay(Duration.millis(100)), Effect.forever),
            );
            const afterInvalidate = yield* cacheB.get("k").pipe(
              Effect.flatMap((v) =>
                v >= 2
                  ? Effect.succeed(v)
                  : Effect.fail(new CommandError({ message: "L1 not yet dropped" })),
              ),
              Effect.retry(Schedule.spaced(Duration.millis(100))),
              Effect.timeoutFail({
                duration: Duration.seconds(15),
                onTimeout: () =>
                  new CommandError({ message: "cross-instance invalidation timed out" }),
              }),
            );
            yield* Fiber.interrupt(invalidator);
            return { warmA, warmB, afterInvalidate };
          }),
        ),
      );
      expect(result.warmA).toBe(1);
      expect(result.warmB).toBe(1);
      expect(result.afterInvalidate).toBeGreaterThanOrEqual(2);
    });

    test("RateLimit: admits exactly max per window, then denies with a shaped decision", async () => {
      const result = await run(
        Effect.gen(function* () {
          const limiter = RateLimit.make({
            window: Duration.minutes(1),
            max: 3,
            prefix: "rl:exact",
          });
          const allowed = yield* Effect.all(
            Array.from({ length: 3 }, () => limiter.check("u1")),
            { concurrency: 1 },
          );
          const denied = yield* limiter.check("u1");
          return { allowed, denied };
        }),
      );
      expect(result.allowed.map((d) => d.allowed)).toEqual([true, true, true]);
      expect(result.allowed.map((d) => d.remaining)).toEqual([2, 1, 0]);
      expect(result.denied.allowed).toBe(false);
      expect(result.denied.remaining).toBe(0);
      expect(result.denied.limit).toBe(3);
      expect(Duration.toSeconds(result.denied.resetAfter)).toBeGreaterThan(0);
      expect(Duration.toSeconds(result.denied.resetAfter)).toBeLessThanOrEqual(60);
    });

    test("RateLimit: the counter is written with a TTL atomically (no lost-EXPIRE lockout)", async () => {
      const ttl = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const limiter = RateLimit.make({
            window: Duration.seconds(30),
            max: 5,
            prefix: "rl:ttlatomic",
          });
          yield* limiter.check("u1");
          const keys = yield* redis.call("KEYS", "rl:ttlatomic:u1:*");
          const key = (keys as ReadonlyArray<string>)[0];
          if (key === undefined) return Option.none<Duration.Duration>();
          return yield* redis.ttl(key);
        }),
      );
      expect(Option.isSome(ttl)).toBe(true);
      const seconds = Option.match(ttl, {
        onNone: () => 0,
        onSome: (d) => Duration.toSeconds(d),
      });
      expect(seconds).toBeGreaterThan(0);
      expect(seconds).toBeLessThanOrEqual(60); // 2 * window
    });

    test("RateLimit: concurrent checks admit exactly max under contention", async () => {
      const allowed = await run(
        Effect.gen(function* () {
          const limiter = RateLimit.make({
            window: Duration.minutes(1),
            max: 10,
            prefix: "rl:conc",
          });
          const decisions = yield* Effect.all(
            Array.from({ length: 25 }, () => limiter.check("u1")),
            { concurrency: "unbounded" },
          );
          return decisions.filter((d) => d.allowed).length;
        }),
      );
      expect(allowed).toBe(10);
    });

    test("RateLimit: the previous window's load blocks an across-edge burst (no 2x max)", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const limiter = RateLimit.make({
            window: Duration.seconds(1),
            max: 3,
            prefix: "rl:edge",
          });
          // Align just past a 1s boundary so the burst starts at elapsed≈0.
          const time = yield* redis.call("TIME");
          const micros = Number((time as ReadonlyArray<string>)[1] ?? "0");
          yield* Effect.sleep(Duration.millis(1000 - Math.floor(micros / 1000) + 20));
          const a = yield* Effect.all(
            Array.from({ length: 3 }, () => limiter.check("u1")),
            { concurrency: 1 },
          );
          const overA = yield* limiter.check("u1");
          yield* Effect.sleep(Duration.millis(1000)); // into window B, A still fully weighs
          const firstB = yield* limiter.check("u1");
          yield* Effect.sleep(Duration.millis(1000)); // A decayed out
          const afterDecay = yield* limiter.check("u1");
          return {
            a: a.map((d) => d.allowed),
            overA: overA.allowed,
            firstB: firstB.allowed,
            afterDecay: afterDecay.allowed,
          };
        }),
      );
      expect(result.a).toEqual([true, true, true]);
      expect(result.overA).toBe(false);
      expect(result.firstB).toBe(false);
      expect(result.afterDecay).toBe(true);
    });

    test("hash: hset / hget / hgetAll / hdel / hexists / hincrBy / hlen / hkeys / hvals / hmget", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const added = yield* redis.hset("h:profile", { name: "ada", age: "36" });
          const name = yield* redis.hget("h:profile", "name");
          const missing = yield* redis.hget("h:profile", "nope");
          const all = yield* redis.hgetAll("h:profile");
          const exists = yield* redis.hexists("h:profile", "name");
          const bumped = yield* redis.hincrBy("h:profile", "age", 1);
          const len = yield* redis.hlen("h:profile");
          const keys = yield* redis.hkeys("h:profile");
          const vals = yield* redis.hvals("h:profile");
          const fetched = yield* redis.hmget("h:profile", "name", "nope");
          const removed = yield* redis.hdel("h:profile", "name");
          return { added, name, missing, all, exists, bumped, len, keys, vals, fetched, removed };
        }),
      );
      expect(result.added).toBe(2);
      expect(result.name).toEqual(Option.some("ada"));
      expect(Option.isNone(result.missing)).toBe(true);
      expect(result.all).toEqual({ name: "ada", age: "36" });
      expect(result.exists).toBe(true);
      expect(result.bumped).toBe(37);
      expect(result.len).toBe(2);
      expect([...result.keys].sort()).toEqual(["age", "name"]);
      expect([...result.vals].sort()).toEqual(["37", "ada"]);
      expect(result.fetched).toEqual([Option.some("ada"), Option.none()]);
      expect(result.removed).toBe(1);
    });

    test("set: sadd / smembers / sismember / scard / srem / spop", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const added = yield* redis.sadd("s:tags", "a", "b", "c");
          const dup = yield* redis.sadd("s:tags", "a");
          const members = yield* redis.smembers("s:tags");
          const isMember = yield* redis.sismember("s:tags", "b");
          const notMember = yield* redis.sismember("s:tags", "z");
          const card = yield* redis.scard("s:tags");
          const removed = yield* redis.srem("s:tags", "a");
          const popped = yield* redis.spop("s:tags");
          return { added, dup, members, isMember, notMember, card, removed, popped };
        }),
      );
      expect(result.added).toBe(3);
      expect(result.dup).toBe(0);
      expect([...result.members].sort()).toEqual(["a", "b", "c"]);
      expect(result.isMember).toBe(true);
      expect(result.notMember).toBe(false);
      expect(result.card).toBe(3);
      expect(result.removed).toBe(1);
      expect(Option.isSome(result.popped)).toBe(true);
    });

    test("zset: zadd / zscore / zincrBy / zcard / zrank / zrem", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const added = yield* redis.zadd("z:board", [
            [1, "a"],
            [2, "b"],
            [3, "c"],
          ]);
          const score = yield* redis.zscore("z:board", "b");
          const missingScore = yield* redis.zscore("z:board", "z");
          const incr = yield* redis.zincrBy("z:board", 5, "a");
          const card = yield* redis.zcard("z:board");
          const rank = yield* redis.zrank("z:board", "b"); // b(2) is lowest score → rank 0
          const missingRank = yield* redis.zrank("z:board", "z");
          const removed = yield* redis.zrem("z:board", "c");
          return { added, score, missingScore, incr, card, rank, missingRank, removed };
        }),
      );
      expect(result.added).toBe(3);
      expect(result.score).toEqual(Option.some(2));
      expect(Option.isNone(result.missingScore)).toBe(true);
      expect(result.incr).toBe(6);
      expect(result.card).toBe(3);
      expect(result.rank).toEqual(Option.some(0));
      expect(Option.isNone(result.missingRank)).toBe(true);
      expect(result.removed).toBe(1);
    });

    // Forcing function: HGETALL is a flat array on RESP2 (ioredis) and a map object on RESP3 (Bun).
    test("hgetAll normalizes RESP2 array and RESP3 map to the same Record", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("h:norm");
          yield* redis.hset("h:norm", { f1: "v1", f2: "v2", f3: "v3" });
          return yield* redis.hgetAll("h:norm");
        }),
      );
      expect(result).toEqual({ f1: "v1", f2: "v2", f3: "v3" });
    });

    test("zrange / zrangeWithScores preserve order and scores across RESP2/RESP3", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("z:scores");
          yield* redis.zadd("z:scores", [
            [2, "b"],
            [1, "a"],
            [3, "c"],
          ]);
          const range = yield* redis.zrange("z:scores", 0, -1);
          const withScores = yield* redis.zrangeWithScores("z:scores", 0, -1);
          return { range, withScores };
        }),
      );
      expect(result.range).toEqual(["a", "b", "c"]);
      expect(result.withScores).toEqual([
        ["a", 1],
        ["b", 2],
        ["c", 3],
      ]);
    });

    test("zset scores round-trip Infinity via formatScore / decodeFloat", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("z:inf");
          yield* redis.zadd("z:inf", [
            [Number.POSITIVE_INFINITY, "hi"],
            [Number.NEGATIVE_INFINITY, "lo"],
          ]);
          const hi = yield* redis.zscore("z:inf", "hi");
          const lo = yield* redis.zscore("z:inf", "lo");
          return { hi, lo };
        }),
      );
      expect(result.hi).toEqual(Option.some(Number.POSITIVE_INFINITY));
      expect(result.lo).toEqual(Option.some(Number.NEGATIVE_INFINITY));
    });

    test("collection command on a string key surfaces WRONGTYPE as CommandError", async () => {
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.set("k:strtype", "v");
          return yield* redis.hgetAll("k:strtype");
        }),
      );
      const error = failureTag(exit);
      expect(error?._tag).toBe("CommandError");
      expect(error?.message).toContain("WRONGTYPE");
    });

    test("empty hset / sadd / zadd are no-ops returning 0", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          const h = yield* redis.hset("e:h", {});
          const s = yield* redis.sadd("e:s");
          const z = yield* redis.zadd("e:z", []);
          const exists = yield* redis.exists("e:h");
          return { h, s, z, exists };
        }),
      );
      expect(result).toEqual({ h: 0, s: 0, z: 0, exists: false });
    });

    test("Redis.setOf: Schema-typed set round-trips", async () => {
      const Tag = Schema.Struct({ kind: Schema.String, id: Schema.Number });
      const tags = Redis.setOf(Tag, { prefix: "refset", ttl: Duration.minutes(5) });
      const result = await run(
        Effect.gen(function* () {
          yield* tags("u1").add({ kind: "a", id: 1 }, { kind: "b", id: 2 });
          const members = yield* tags("u1").members;
          const has = yield* tags("u1").has({ kind: "a", id: 1 });
          const size = yield* tags("u1").size;
          const removed = yield* tags("u1").remove({ kind: "a", id: 1 });
          return { members, has, size, removed };
        }),
      );
      expect(result.members.map((m) => m.kind).sort()).toEqual(["a", "b"]);
      expect(result.has).toBe(true);
      expect(result.size).toBe(2);
      expect(result.removed).toBe(1);
    });

    test("Redis.sortedSet: leaderboard add / incrBy / rangeWithScores / rank", async () => {
      const Player = Schema.Struct({ name: Schema.String });
      const board = Redis.sortedSet(Player, { prefix: "refzset" });
      const result = await run(
        Effect.gen(function* () {
          yield* board("game1").add({ name: "ada" }, 10);
          yield* board("game1").add({ name: "bob" }, 20);
          const bumped = yield* board("game1").incrBy({ name: "ada" }, 15); // ada → 25
          const top = yield* board("game1").rangeWithScores(0, -1);
          const rank = yield* board("game1").rank({ name: "bob" }); // bob(20) lowest → rank 0
          return { bumped, top, rank };
        }),
      );
      expect(result.bumped).toBe(25);
      expect(result.top).toEqual([
        [{ name: "bob" }, 20],
        [{ name: "ada" }, 25],
      ]);
      expect(result.rank).toEqual(Option.some(0));
    });

    test("Redis.hashOf: field→value map round-trips", async () => {
      const Pref = Schema.Struct({ enabled: Schema.Boolean });
      const prefs = Redis.hashOf(Pref, { prefix: "refhash" });
      const result = await run(
        Effect.gen(function* () {
          yield* prefs("u1").set("dark", { enabled: true });
          yield* prefs("u1").set("beta", { enabled: false });
          const dark = yield* prefs("u1").get("dark");
          const missing = yield* prefs("u1").get("nope");
          const all = yield* prefs("u1").getAll;
          const has = yield* prefs("u1").has("beta");
          const keys = yield* prefs("u1").keys;
          const size = yield* prefs("u1").size;
          const removed = yield* prefs("u1").remove("beta");
          return { dark, missing, all, has, keys, size, removed };
        }),
      );
      expect(result.dark).toEqual(Option.some({ enabled: true }));
      expect(Option.isNone(result.missing)).toBe(true);
      expect(result.all.get("dark")).toEqual({ enabled: true });
      expect(result.all.get("beta")).toEqual({ enabled: false });
      expect(result.has).toBe(true);
      expect([...result.keys].sort()).toEqual(["beta", "dark"]);
      expect(result.size).toBe(2);
      expect(result.removed).toBe(1);
    });

    test("Redis.hashOf: malformed stored value fails with DecodeError", async () => {
      const rec = Redis.hashOf(Schema.Struct({ n: Schema.Number }), { prefix: "refhash:bad" });
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.call("HSET", "refhash:bad:x", "f", "not json");
          return yield* rec("x").get("f");
        }),
      );
      expect(failureTag(exit)?._tag).toBe("DecodeError");
    });

    test("Redis.setOf: configured ttl is stamped and keepTtl preserves it", async () => {
      const set = Redis.setOf(Schema.String, { prefix: "refset:ttl", ttl: Duration.seconds(100) });
      const result = await run(
        Effect.gen(function* () {
          yield* set("k").add("a");
          const ttl1 = yield* set("k").ttl;
          yield* set("k").addWith(["b"], { keepTtl: true });
          const ttl2 = yield* set("k").ttl;
          return { ttl1, ttl2 };
        }),
      );
      expect(Option.isSome(result.ttl1)).toBe(true);
      expect(Option.isSome(result.ttl2)).toBe(true);
    });

    test("Redis.setOf without a configured ttl leaves the key persistent", async () => {
      const set = Redis.setOf(Schema.String, { prefix: "refset:nottl" });
      const ttl = await run(
        Effect.gen(function* () {
          yield* set("k").add("a");
          return yield* set("k").ttl;
        }),
      );
      expect(Option.isNone(ttl)).toBe(true);
    });

    test("ref empty variadic writes short-circuit to 0 without erroring", async () => {
      const result = await run(
        Effect.gen(function* () {
          const set = Redis.setOf(Schema.String, { prefix: "refset:empty" });
          const zset = Redis.sortedSet(Schema.String, { prefix: "refzset:empty" });
          const hash = Redis.hashOf(Schema.String, { prefix: "refhash:empty" });
          const a = yield* set("k").add();
          const b = yield* set("k").remove();
          const c = yield* zset("k").remove();
          const d = yield* hash("k").remove();
          return { a, b, c, d };
        }),
      );
      expect(result).toEqual({ a: 0, b: 0, c: 0, d: 0 });
    });

    test("Cmd builders cover the collection surface via pipeline", async () => {
      const replies = await run(
        Effect.flatMap(Redis, (redis) =>
          redis.pipeline([
            Cmd.hset("cc:h", { a: "1", b: "2" }),
            Cmd.hget("cc:h", "a"),
            Cmd.hlen("cc:h"),
            Cmd.sadd("cc:s", "x", "y"),
            Cmd.scard("cc:s"),
            Cmd.sismember("cc:s", "x"),
            Cmd.zadd("cc:z", [
              [1, "a"],
              [2, "b"],
            ]),
            Cmd.zscore("cc:z", "b"),
            Cmd.zrange("cc:z", 0, -1),
          ]),
        ),
      );
      expect(replies[0]).toBe(2); // hset added 2 fields
      expect(replies[1]).toBe("1"); // hget a
      expect(replies[2]).toBe(2); // hlen
      expect(replies[3]).toBe(2); // sadd added 2
      expect(replies[4]).toBe(2); // scard
      expect(Boolean(replies[5])).toBe(true); // sismember (1 on RESP2, true on RESP3)
      expect(replies[6]).toBe(2); // zadd added 2
      expect(Number(replies[7])).toBe(2); // zscore ("2" on RESP2, 2 on RESP3)
      expect(replies[8]).toEqual(["a", "b"]); // zrange
    });

    // Forcing function: a stream entry's fields are a flat array on RESP2 (ioredis) and a map on RESP3 (Bun).
    test("xadd / xrange round-trip; entry fields normalize across RESP2/RESP3", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:log");
          const id1 = yield* redis.xadd("x:log", { type: "a", n: "1" });
          const id2 = yield* redis.xadd("x:log", { type: "b", n: "2" });
          const entries = yield* redis.xrange("x:log");
          const reversed = yield* redis.xrevrange("x:log", "-", "+", { count: 1 });
          const len = yield* redis.xlen("x:log");
          return { id1, id2, entries, reversed, len };
        }),
      );
      expect(result.len).toBe(2);
      expect(result.entries.map((e) => e.id)).toEqual([result.id1, result.id2]);
      expect(result.entries[0]?.fields).toEqual({ type: "a", n: "1" });
      expect(result.entries[1]?.fields).toEqual({ type: "b", n: "2" });
      expect(result.reversed.map((e) => e.id)).toEqual([result.id2]); // newest first
    });

    test("xread normalizes the outer shape and returns [] for no data", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:read");
          yield* redis.xadd("x:read", { v: "1" }, { id: "1-1" });
          yield* redis.xadd("x:read", { v: "2" }, { id: "2-1" });
          const reads = yield* redis.xread([["x:read", "0"]]);
          const empty = yield* redis.xread([["x:read", "$"]]); // nothing newer than the tail
          const missing = yield* redis.xread([["x:missing", "0"]]); // missing stream
          return { reads, empty, missing };
        }),
      );
      expect(result.reads.length).toBe(1);
      expect(result.reads[0]?.[0]).toBe("x:read");
      expect(result.reads[0]?.[1].map((e) => e.id)).toEqual(["1-1", "2-1"]);
      expect(result.empty).toEqual([]);
      expect(result.missing).toEqual([]);
    });

    test("xtrim (exact and approx), xdel, and empty xdel → 0", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:trim", "x:approx");
          yield* Effect.forEach(Array.from({ length: 10 }), (_, i) =>
            redis.xadd("x:trim", { i: String(i) }),
          );
          const trimmed = yield* redis.xtrim("x:trim", { maxLen: 5 });
          const exact = yield* redis.xlen("x:trim");
          yield* Effect.forEach(Array.from({ length: 10 }), (_, i) =>
            redis.xadd("x:approx", { i: String(i) }),
          );
          yield* redis.xtrim("x:approx", { maxLen: 5, approx: true });
          const approxLen = yield* redis.xlen("x:approx");
          const firstId = (yield* redis.xrange("x:trim"))[0]?.id ?? "0-0";
          const removed = yield* redis.xdel("x:trim", firstId);
          const noop = yield* redis.xdel("x:trim");
          return { trimmed, exact, approxLen, removed, noop };
        }),
      );
      expect(result.trimmed).toBe(5);
      expect(result.exact).toBe(5);
      expect(result.approxLen).toBeGreaterThanOrEqual(5); // ~ keeps at least maxLen, often more
      expect(result.removed).toBe(1);
      expect(result.noop).toBe(0);
    });

    test("xadd with a non-monotonic explicit id fails with CommandError", async () => {
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:mono");
          yield* redis.xadd("x:mono", { v: "1" }, { id: "5-0" });
          return yield* redis.xadd("x:mono", { v: "2" }, { id: "3-0" });
        }),
      );
      expect(failureTag(exit)?._tag).toBe("CommandError");
    });

    test("Redis.stream: typed log round-trips via add / range", async () => {
      const Event = Schema.Struct({ kind: Schema.String, seq: Schema.Number });
      const log = Redis.stream(Event, { prefix: "stream:typed" });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:typed:t1");
          yield* log("t1").add({ kind: "a", seq: 1 });
          yield* log("t1").add({ kind: "b", seq: 2 });
          const range = yield* log("t1").range();
          const len = yield* log("t1").len;
          return { range, len };
        }),
      );
      expect(result.len).toBe(2);
      expect(result.range.map((e) => e.message)).toEqual([
        { kind: "a", seq: 1 },
        { kind: "b", seq: 2 },
      ]);
    });

    test('Redis.stream: malformed and missing-"d" entries fail with DecodeError', async () => {
      const log = Redis.stream(Schema.Struct({ n: Schema.Number }), { prefix: "stream:bad" });
      const malformed = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:bad:m");
          yield* redis.call("XADD", "stream:bad:m", "*", "d", "not json");
          return yield* log("m").range();
        }),
      );
      const missing = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:bad:x");
          yield* redis.call("XADD", "stream:bad:x", "*", "other", "1"); // no "d" field
          return yield* log("x").range();
        }),
      );
      expect(failureTag(malformed)?._tag).toBe("DecodeError");
      expect(failureTag(missing)?._tag).toBe("DecodeError");
    });

    test("consumer group: xreadGroup / xack / xpending / claimStale", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:grp");
          yield* redis.xadd("x:grp", { v: "1" }, { id: "1-1" });
          yield* redis.xadd("x:grp", { v: "2" }, { id: "2-1" });
          yield* redis.xgroupCreate("x:grp", "g1", { from: "0" });
          const reads = yield* redis.xreadGroup("g1", "c1", [["x:grp", ">"]]);
          const entries = reads.flatMap(([, es]) => es);
          const pendingBefore = yield* redis.xpending("x:grp", "g1");
          yield* redis.xack("x:grp", "g1", entries[0]?.id ?? "0-0");
          const pendingAfter = yield* redis.xpending("x:grp", "g1");
          // c2 claims whatever c1 still holds (minIdle 0 → everything pending)
          const claimed = yield* redis.streams.claimStale("x:grp", "g1", "c2", {
            minIdle: Duration.millis(0),
          });
          return { reads, entries, pendingBefore, pendingAfter, claimed };
        }),
      );
      expect(result.reads[0]?.[0]).toBe("x:grp");
      expect(result.entries.map((e) => e.id)).toEqual(["1-1", "2-1"]);
      expect(result.pendingBefore.count).toBe(2);
      expect(result.pendingAfter.count).toBe(1);
      expect(result.claimed.map((e) => e.id)).toEqual(["2-1"]); // the un-acked one
    });

    test("streams.read live-tails entries and releases its connection on scope close", async () => {
      const connectedClients = (r: RedisService) =>
        r
          .call("INFO", "clients")
          .pipe(
            Effect.map((info) => Number(/connected_clients:(\d+)/.exec(String(info))?.[1] ?? "0")),
          );
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:tail");
          const before = yield* connectedClients(redis);
          const fiber = yield* Redis.use((r) =>
            r.streams.read("x:tail", { from: "$", block: Duration.millis(200) }),
          ).pipe(Stream.take(2), Stream.runCollect, Effect.fork);
          const producer = yield* Effect.fork(
            redis
              .xadd("x:tail", { v: "x" })
              .pipe(Effect.delay(Duration.millis(100)), Effect.forever),
          );
          const received = yield* Fiber.join(fiber).pipe(
            Effect.timeoutFail({
              duration: Duration.seconds(15),
              onTimeout: () => new CommandError({ message: "stream read timed out" }),
            }),
          );
          yield* Fiber.interrupt(producer);
          yield* connectedClients(redis).pipe(
            Effect.flatMap((n) =>
              n <= before
                ? Effect.void
                : Effect.fail(new CommandError({ message: "dedicated connection not released" })),
            ),
            Effect.retry(Schedule.recurs(50).pipe(Schedule.addDelay(() => Duration.millis(100)))),
          );
          return Chunk.toReadonlyArray(received).length;
        }),
      );
      expect(result).toBe(2);
    });

    test("streams.readGroup delivers entries and ack clears the PEL", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:gstream");
          const fiber = yield* Redis.use((r) =>
            r.streams.readGroup("x:gstream", {
              group: "g",
              consumer: "c",
              from: "0",
              block: Duration.millis(200),
            }),
          ).pipe(Stream.take(1), Stream.runCollect, Effect.fork);
          const producer = yield* Effect.fork(
            redis
              .xadd("x:gstream", { v: "hello" })
              .pipe(Effect.delay(Duration.millis(100)), Effect.forever),
          );
          const received = yield* Fiber.join(fiber).pipe(
            Effect.timeoutFail({
              duration: Duration.seconds(15),
              onTimeout: () => new CommandError({ message: "group read timed out" }),
            }),
          );
          yield* Fiber.interrupt(producer);
          const entry = Chunk.toReadonlyArray(received)[0];
          const pendingBefore = yield* redis.xpending("x:gstream", "g");
          yield* entry?.ack ?? Effect.void;
          const pendingAfter = yield* redis.xpending("x:gstream", "g");
          return {
            fields: entry?.fields,
            before: pendingBefore.count,
            after: pendingAfter.count,
          };
        }),
      );
      expect(result.fields).toEqual({ v: "hello" });
      expect(result.before).toBeGreaterThanOrEqual(1);
      expect(result.after).toBe(result.before - 1);
    });

    test("xpendingExtended / xclaim / xinfo expose the group surface", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:info");
          yield* redis.xadd("x:info", { v: "1" }, { id: "1-1" });
          yield* redis.xadd("x:info", { v: "2" }, { id: "2-1" });
          yield* redis.xgroupCreate("x:info", "g", { from: "0" });
          yield* redis.xreadGroup("g", "c1", [["x:info", ">"]]); // both entries now pending for c1
          const pending = yield* redis.xpendingExtended("x:info", "g", { count: 10 });
          const claimed = yield* redis.xclaim("x:info", "g", "c2", Duration.millis(0), ["1-1"]);
          const info = yield* redis.xinfoStream("x:info");
          const groups = yield* redis.xinfoGroups("x:info");
          return { pending, claimed, info, groups };
        }),
      );
      expect(result.pending.map((p) => p.id)).toEqual(["1-1", "2-1"]);
      expect(result.pending.every((p) => p.consumer === "c1" && p.deliveryCount === 1)).toBe(true);
      expect(result.claimed.map((e) => e.id)).toEqual(["1-1"]); // moved from c1 to c2
      expect(result.info.length).toBe("2"); // scalar fields survive; nested entries are dropped
      expect(result.groups.some((g) => g.name === "g")).toBe(true);
    });

    test("streams.read reconnects and resumes after its connection is dropped", async () => {
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:reconnect");
          yield* redis.xadd("x:reconnect", { v: "a" });
          const fiber = yield* Redis.use((r) =>
            r.streams.read("x:reconnect", { from: "0", block: Duration.millis(100) }),
          ).pipe(Stream.take(3), Stream.runCollect, Effect.fork);
          // Let the consumer read "a" and advance its lastId, then drop its dedicated connection.
          // CLIENT KILL spares the caller (SKIPME defaults to yes), so only the consumer dies.
          yield* Effect.sleep(Duration.millis(500));
          yield* redis.call("CLIENT", "KILL", "TYPE", "normal").pipe(Effect.ignore);
          // Produce the rest only after the kill, so delivery proves the consumer reconnected.
          const retryWrite = Schedule.recurs(30).pipe(
            Schedule.addDelay(() => Duration.millis(100)),
          );
          yield* redis.xadd("x:reconnect", { v: "b" }).pipe(Effect.retry(retryWrite));
          yield* redis.xadd("x:reconnect", { v: "c" }).pipe(Effect.retry(retryWrite));
          const received = yield* Fiber.join(fiber).pipe(
            Effect.timeoutFail({
              duration: Duration.seconds(25),
              onTimeout: () => new CommandError({ message: "reconnect read timed out" }),
            }),
          );
          return Chunk.toReadonlyArray(received).map((e) => e.fields.v);
        }),
      );
      expect(result).toEqual(["a", "b", "c"]); // resumed past "a", no loss, no duplicate
    }, 30_000);

    test("streams.read fails fast on WRONGTYPE instead of retrying forever", async () => {
      const exit = await runExit(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("x:wrongtype");
          yield* redis.set("x:wrongtype", "i am a string");
          return yield* Redis.use((r) =>
            r.streams.read("x:wrongtype", { from: "0", block: Duration.millis(100) }),
          ).pipe(Stream.take(1), Stream.runCollect);
        }).pipe(
          // If a non-connection error were retried, this would loop until the deadline fires.
          Effect.timeoutFail({
            duration: Duration.seconds(10),
            onTimeout: () =>
              new CommandError({ message: "stream read did not fail fast (looping?)" }),
          }),
        ),
      );
      expect(failureTag(exit)?._tag).toBe("CommandError");
      expect(failureTag(exit)?.message).toContain("WRONGTYPE");
    });

    test("Redis.stream consume runs the handler and auto-acks after success", async () => {
      const Event = Schema.Struct({ n: Schema.Number });
      const log = Redis.stream(Event, { prefix: "stream:consume" });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:consume:c1");
          yield* log("c1").add({ n: 1 });
          yield* log("c1").add({ n: 2 });
          const handled = yield* Ref.make<ReadonlyArray<number>>([]);
          const emitted = yield* log("c1")
            .consume({ group: "g", consumer: "c", from: "0", block: Duration.millis(200) }, (e) =>
              Ref.update(handled, (xs) => [...xs, e.n]),
            )
            .pipe(
              Stream.take(2),
              Stream.runCollect,
              Effect.timeoutFail({
                duration: Duration.seconds(15),
                onTimeout: () => new CommandError({ message: "consume timed out" }),
              }),
            );
          const pending = yield* redis.xpending("stream:consume:c1", "g");
          return {
            emitted: Chunk.toReadonlyArray(emitted).map((m) => m.n),
            handled: yield* Ref.get(handled),
            pendingCount: pending.count,
          };
        }),
      );
      expect(result.emitted).toEqual([1, 2]);
      expect(result.handled).toEqual([1, 2]);
      expect(result.pendingCount).toBe(0); // every delivered entry was acked after its handler
    });

    test("Redis.stream group yields typed entries with a manual ack", async () => {
      const Event = Schema.Struct({ n: Schema.Number });
      const log = Redis.stream(Event, { prefix: "stream:group" });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:group:g1");
          yield* log("g1").add({ n: 7 });
          const head = yield* log("g1")
            .group({ group: "g", consumer: "c", from: "0", block: Duration.millis(200) })
            .pipe(
              Stream.runHead,
              Effect.timeoutFail({
                duration: Duration.seconds(15),
                onTimeout: () => new CommandError({ message: "group read timed out" }),
              }),
            );
          const entry = Option.getOrNull(head);
          const before = yield* redis.xpending("stream:group:g1", "g");
          yield* entry?.ack ?? Effect.void; // manual ack, not auto
          const after = yield* redis.xpending("stream:group:g1", "g");
          return { message: entry?.message, before: before.count, after: after.count };
        }),
      );
      expect(result.message).toEqual({ n: 7 });
      expect(result.before).toBe(1);
      expect(result.after).toBe(0);
    });

    test("Redis.stream consume leaves the entry pending when the handler fails", async () => {
      const Event = Schema.Struct({ n: Schema.Number });
      const log = Redis.stream(Event, { prefix: "stream:consumefail" });
      const result = await run(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.del("stream:consumefail:c1");
          yield* log("c1").add({ n: 1 });
          const exit = yield* log("c1")
            .consume({ group: "g", consumer: "c", from: "0", block: Duration.millis(200) }, () =>
              Effect.fail(new CommandError({ message: "boom" })),
            )
            .pipe(Stream.runDrain, Effect.timeout(Duration.seconds(10)), Effect.exit);
          const pending = yield* redis.xpending("stream:consumefail:c1", "g");
          return { failed: Exit.isFailure(exit), pending: pending.count };
        }),
      );
      expect(result.failed).toBe(true); // the handler error terminates the consumer
      expect(result.pending).toBe(1); // and the un-acked entry stays pending for claimStale
    });
  });
};
