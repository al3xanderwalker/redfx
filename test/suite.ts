import { afterAll, beforeAll, describe, expect, test } from "bun:test";
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
import { Cmd, CommandError, Redis, RedisCache, type RedisError, type RedisService } from "redfx";
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
  });
};
