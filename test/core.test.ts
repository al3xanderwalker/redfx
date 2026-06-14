import { expect, test } from "bun:test";
import {
  ConnectionError,
  type ConnectionService,
  layerConnection,
  pooledConnection,
  Redis,
  type RedisError,
} from "@redfx/core";
import { Cause, Duration, Effect, Exit, Option, Ref, Stream } from "effect";

// A fake in-memory driver, so these can inject failures the real adapters won't produce on cue.

const failureTag = <A>(exit: Exit.Exit<A, RedisError>): RedisError | null =>
  Exit.isFailure(exit) ? Option.getOrNull(Cause.failureOption(exit.cause)) : null;

test("commandTimeout fails a slow command with TimeoutError", async () => {
  const stalled: ConnectionService = {
    send: () => Effect.never,
    pipeline: () => Effect.never,
    subscribe: () => Stream.empty,
    dedicated: () => Stream.empty,
    close: Effect.void,
  };
  const layer = layerConnection(Effect.succeed(stalled), { commandTimeout: Duration.millis(50) });
  const exit = await Effect.runPromiseExit(
    Effect.flatMap(Redis, (r) => r.call("PING")).pipe(Effect.provide(layer)),
  );
  expect(failureTag(exit)?._tag).toBe("TimeoutError");
});

test("pool invalidates a connection that fails with ConnectionError", async () => {
  const created = await Effect.runPromise(
    Effect.gen(function* () {
      const createdRef = yield* Ref.make(0);
      const makeOne = Effect.gen(function* () {
        const id = yield* Ref.updateAndGet(createdRef, (n) => n + 1);
        const calls = yield* Ref.make(0);
        return {
          send: () =>
            Ref.updateAndGet(calls, (n) => n + 1).pipe(
              Effect.flatMap((c) =>
                c >= 2
                  ? Effect.fail(new ConnectionError({ message: `conn ${id} dead` }))
                  : Effect.succeed("OK"),
              ),
            ),
          pipeline: () => Effect.succeed([]),
          subscribe: () => Stream.empty,
          dedicated: () => Stream.empty,
          close: Effect.void,
        } satisfies ConnectionService;
      });
      const layer = layerConnection(
        pooledConnection(
          makeOne,
          1,
          () => Stream.empty,
          () => Stream.empty,
        ),
      );
      yield* Effect.provide(
        Effect.gen(function* () {
          const redis = yield* Redis;
          yield* redis.call("PING");
          yield* Effect.either(redis.call("PING"));
          yield* redis.call("PING");
        }),
        layer,
      );
      return yield* Ref.get(createdRef);
    }),
  );
  expect(created).toBe(2);
});
