import { describe, expect, test } from "bun:test";
import { Duration, Effect, Option, Schema } from "effect";
import { Redis, RedisCache, type RedisError } from "redfx";
import { IoRedis } from "redfx-ioredis";

// Skipped unless pointed at a real cluster (the conformance container is single-node):
//   REDIS_CLUSTER_NODES=127.0.0.1:7100,127.0.0.1:7101 bun test test/ioredis-cluster.test.ts
const raw = process.env.REDIS_CLUSTER_NODES;
const nodes = (raw ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter((s) => s.length > 0)
  .map((hostPort) => {
    const [host, port] = hostPort.split(":");
    return { host: host ?? "127.0.0.1", port: Number(port ?? 6379) };
  });

const suite = nodes.length > 0 ? describe : describe.skip;

suite("redfx ioredis cluster", () => {
  const layer = IoRedis.layerCluster({ nodes });
  const run = <A>(program: Effect.Effect<A, RedisError, Redis>): Promise<A> =>
    Effect.runPromise(Effect.provide(program, layer));

  test("single-key commands route to the owning slot", async () => {
    const result = await run(
      Effect.gen(function* () {
        const redis = yield* Redis;
        yield* redis.set("cl:k", "v");
        return yield* redis.get("cl:k");
      }),
    );
    expect(result).toEqual(Option.some("v"));
  });

  test("Redis.ref round-trips across the cluster", async () => {
    const rec = Redis.ref(Schema.Struct({ n: Schema.Number }), {
      prefix: "cl:ref",
      ttl: Duration.minutes(5),
    });
    const result = await run(
      Effect.gen(function* () {
        yield* rec("x").set({ n: 7 });
        return yield* rec("x").get;
      }),
    );
    expect(result).toEqual(Option.some({ n: 7 }));
  });

  test("RedisCache.make serves a cold then warm key", async () => {
    const cache = RedisCache.make({
      schema: Schema.Number,
      prefix: "cl:cache",
      ttl: Duration.minutes(5),
      lookup: () => Effect.succeed(42),
    });
    const result = await run(
      Effect.gen(function* () {
        const cold = yield* cache.get("k");
        const warm = yield* cache.get("k");
        return { cold, warm };
      }),
    );
    expect(result).toEqual({ cold: 42, warm: 42 });
  });
});
