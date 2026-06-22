import { Context, Effect, Pool, type Scope, type Stream } from "effect";
import type { RedisCommand } from "./RedisCommand.js";
import type { ConnectionError, RedisError } from "./RedisError.js";
import type { RespValue } from "./RespValue.js";

export interface PushMessage {
  readonly channel: string;
  readonly message: string;
}

export interface ConnectionService {
  readonly send: (command: RedisCommand) => Effect.Effect<RespValue, RedisError>;
  readonly pipeline: (
    commands: ReadonlyArray<RedisCommand>,
  ) => Effect.Effect<ReadonlyArray<RespValue>, RedisError>;
  /** Subscribes on a dedicated connection; the Stream unsubscribes on scope close. */
  readonly subscribe: (channels: ReadonlyArray<string>) => Stream.Stream<PushMessage, RedisError>;
  /** Runs `f` over a fresh, un-pooled, un-timed connection the Stream owns and closes on scope
   *  close — the escape hatch for blocking reads (`XREAD BLOCK`, later `BLPOP`/`WAIT`). */
  readonly dedicated: <A, E>(
    f: (conn: ConnectionService) => Stream.Stream<A, E>,
  ) => Stream.Stream<A, E | RedisError>;
  readonly close: Effect.Effect<void>;
}

export class RedisConnection extends Context.Service<RedisConnection, ConnectionService>()(
  "redfx/RedisConnection",
) {}

/** Pools command connections, dropping any that fail with `ConnectionError`. */
export const pooledConnection = (
  makeConnection: Effect.Effect<ConnectionService, ConnectionError, Scope.Scope>,
  size: number,
  subscribe: ConnectionService["subscribe"],
  dedicated: ConnectionService["dedicated"],
): Effect.Effect<ConnectionService, ConnectionError, Scope.Scope> =>
  Effect.gen(function* () {
    const pool = yield* Pool.make({ acquire: makeConnection, size });
    const withConn = <A>(
      f: (conn: ConnectionService) => Effect.Effect<A, RedisError>,
    ): Effect.Effect<A, RedisError> =>
      Effect.flatMap(Pool.get(pool), (conn) =>
        f(conn).pipe(Effect.tapErrorTag("ConnectionError", () => Pool.invalidate(pool, conn))),
      ).pipe(Effect.scoped);
    return {
      send: (command) => withConn((conn) => conn.send(command)),
      pipeline: (commands) => withConn((conn) => conn.pipeline(commands)),
      subscribe,
      dedicated,
      close: Effect.void,
    };
  });
