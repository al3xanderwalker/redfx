import {
  Context,
  Data,
  Duration,
  Effect,
  Layer,
  Option,
  type ParseResult,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { type ConnectionService, RedisConnection } from "./Connection.js";
import {
  decodeArray,
  decodeBoolean,
  decodeNumber,
  decodeOptionString,
  decodeString,
  decodeTtlSeconds,
} from "./internal/decode.js";
import {
  expirySeconds,
  make,
  type RedisCommand,
  type SetCommandOptions,
  setArgs,
} from "./RedisCommand.js";
import {
  CommandError,
  type ConnectionError,
  DecodeError,
  type RedisError,
  TimeoutError,
} from "./RedisError.js";
import type { RespValue } from "./RespValue.js";

export type SetOptions = SetCommandOptions;

export type KeyTtl = Data.TaggedEnum<{
  readonly Expires: { readonly duration: Duration.Duration };
  readonly Persistent: Record<never, never>;
  readonly Missing: Record<never, never>;
}>;
export const KeyTtl = Data.taggedEnum<KeyTtl>();

const toKeyTtl = (seconds: number): KeyTtl =>
  seconds === -2
    ? KeyTtl.Missing()
    : seconds === -1
      ? KeyTtl.Persistent()
      : KeyTtl.Expires({ duration: Duration.seconds(seconds) });

export interface RedisService {
  readonly get: (key: string) => Effect.Effect<Option.Option<string>, RedisError>;
  readonly set: (
    key: string,
    value: string,
    options?: SetOptions,
  ) => Effect.Effect<void, RedisError>;
  readonly getDelete: (key: string) => Effect.Effect<Option.Option<string>, RedisError>;
  readonly del: (...keys: ReadonlyArray<string>) => Effect.Effect<number, RedisError>;
  readonly exists: (key: string) => Effect.Effect<boolean, RedisError>;
  readonly expire: (key: string, ttl: Duration.DurationInput) => Effect.Effect<boolean, RedisError>;
  /** `None` if the key is missing or persistent; use `ttlState` to tell those apart. */
  readonly ttl: (key: string) => Effect.Effect<Option.Option<Duration.Duration>, RedisError>;
  readonly ttlState: (key: string) => Effect.Effect<KeyTtl, RedisError>;
  readonly incr: (key: string) => Effect.Effect<number, RedisError>;
  readonly decr: (key: string) => Effect.Effect<number, RedisError>;
  readonly incrBy: (key: string, by: number) => Effect.Effect<number, RedisError>;
  readonly decrBy: (key: string, by: number) => Effect.Effect<number, RedisError>;
  readonly mget: (
    ...keys: ReadonlyArray<string>
  ) => Effect.Effect<ReadonlyArray<Option.Option<string>>, RedisError>;
  readonly mset: (
    entries: Record<string, string> | ReadonlyArray<readonly [string, string]>,
  ) => Effect.Effect<void, RedisError>;
  readonly publish: (channel: string, message: string) => Effect.Effect<number, RedisError>;
  readonly ping: Effect.Effect<string, RedisError>;
  readonly call: (
    name: string,
    ...args: ReadonlyArray<string | Uint8Array>
  ) => Effect.Effect<RespValue, RedisError>;
  /** Fails on the first command error; remaining replies are discarded. */
  readonly pipeline: (
    commands: ReadonlyArray<RedisCommand>,
  ) => Effect.Effect<ReadonlyArray<RespValue>, RedisError>;
  readonly subscribe: {
    (channel: string): Stream.Stream<string, RedisError>;
    <A, I>(channel: string, schema: Schema.Schema<A, I>): Stream.Stream<A, RedisError>;
  };
}

const toDecodeError = Effect.mapError(
  (error: ParseResult.ParseError) => new DecodeError({ message: error.message, cause: error }),
);

const makeRedis = (conn: ConnectionService): RedisService => {
  const subscribe = ((channel: string, schema?: Schema.Schema<unknown, unknown>) => {
    const base = conn.subscribe([channel]);
    if (schema === undefined) return Stream.map(base, (m) => m.message);
    const decode = Schema.decode(Schema.parseJson(schema));
    return Stream.mapEffect(base, (m) => decode(m.message).pipe(toDecodeError));
  }) as RedisService["subscribe"];

  return {
    get: (key) => conn.send(make("GET", key)).pipe(Effect.flatMap(decodeOptionString)),
    set: (key, value, options) =>
      conn.send({ name: "SET", args: setArgs(key, value, options) }).pipe(Effect.asVoid),
    getDelete: (key) => conn.send(make("GETDEL", key)).pipe(Effect.flatMap(decodeOptionString)),
    del: (...keys) => conn.send({ name: "DEL", args: keys }).pipe(Effect.flatMap(decodeNumber)),
    exists: (key) => conn.send(make("EXISTS", key)).pipe(Effect.flatMap(decodeBoolean)),
    expire: (key, ttl) =>
      conn.send(make("EXPIRE", key, expirySeconds(ttl))).pipe(Effect.flatMap(decodeBoolean)),
    ttl: (key) => conn.send(make("TTL", key)).pipe(Effect.flatMap(decodeTtlSeconds)),
    ttlState: (key) =>
      conn.send(make("TTL", key)).pipe(Effect.flatMap(decodeNumber), Effect.map(toKeyTtl)),
    incr: (key) => conn.send(make("INCR", key)).pipe(Effect.flatMap(decodeNumber)),
    decr: (key) => conn.send(make("DECR", key)).pipe(Effect.flatMap(decodeNumber)),
    incrBy: (key, by) =>
      conn.send(make("INCRBY", key, String(by))).pipe(Effect.flatMap(decodeNumber)),
    decrBy: (key, by) =>
      conn.send(make("DECRBY", key, String(by))).pipe(Effect.flatMap(decodeNumber)),
    mget: (...keys) =>
      conn.send({ name: "MGET", args: keys }).pipe(
        Effect.flatMap(decodeArray),
        Effect.flatMap((replies) => Effect.forEach(replies, decodeOptionString)),
      ),
    mset: (entries) => {
      const pairs = Array.isArray(entries) ? entries : Object.entries(entries);
      if (pairs.length === 0) return Effect.void; // a zero-pair MSET is an arity error; an empty write is a no-op
      return conn.send({ name: "MSET", args: pairs.flat() }).pipe(Effect.asVoid);
    },
    publish: (channel, message) =>
      conn.send(make("PUBLISH", channel, message)).pipe(Effect.flatMap(decodeNumber)),
    ping: conn.send(make("PING")).pipe(Effect.flatMap(decodeString)),
    call: (name, ...args) => conn.send({ name, args }),
    pipeline: (commands) => conn.pipeline(commands),
    subscribe,
  };
};

const traceConnection = (conn: ConnectionService): ConnectionService => ({
  send: (command) =>
    conn.send(command).pipe(
      Effect.withSpan(`redis.${command.name.toLowerCase()}`, {
        kind: "client",
        attributes: { "db.system": "redis", "db.operation": command.name },
      }),
    ),
  pipeline: (commands) =>
    conn.pipeline(commands).pipe(
      Effect.withSpan("redis.pipeline", {
        kind: "client",
        attributes: {
          "db.system": "redis",
          "db.operation": "PIPELINE",
          "redis.pipeline.commands": commands.length,
        },
      }),
    ),
  subscribe: conn.subscribe,
  close: conn.close,
});

// Subscriptions are long-lived, so the deadline wraps only send/pipeline.
const timeoutConnection = (
  conn: ConnectionService,
  duration: Duration.DurationInput,
): ConnectionService => ({
  send: (command) =>
    conn.send(command).pipe(
      Effect.timeoutFail({
        duration,
        onTimeout: () =>
          new TimeoutError({ message: "redis command timed out", command: command.name }),
      }),
    ),
  pipeline: (commands) =>
    conn.pipeline(commands).pipe(
      Effect.timeoutFail({
        duration,
        onTimeout: () => new TimeoutError({ message: "redis pipeline timed out" }),
      }),
    ),
  subscribe: conn.subscribe,
  close: conn.close,
});

export interface RefOptions {
  readonly prefix: string;
  readonly ttl?: Duration.DurationInput;
}

export interface RedisRef<A> {
  readonly key: string;
  readonly get: Effect.Effect<Option.Option<A>, RedisError, Redis>;
  readonly set: (
    value: A,
    options?: { readonly ttl?: Duration.DurationInput; readonly keepTtl?: boolean },
  ) => Effect.Effect<void, RedisError, Redis>;
  /** Get-then-set, not atomic; use `Redis.script` for compare-and-set. Fails if the key is absent. */
  readonly update: (
    f: (current: A) => A,
    options?: { readonly keepTtl?: boolean },
  ) => Effect.Effect<void, RedisError, Redis>;
  readonly getDelete: Effect.Effect<Option.Option<A>, RedisError, Redis>;
  readonly delete: Effect.Effect<boolean, RedisError, Redis>;
}

export interface ScriptOptions<A, I> {
  readonly result: Schema.Schema<A, I>;
  readonly lua: string;
}

export class Redis extends Context.Tag("redfx/Redis")<Redis, RedisService>() {}

export namespace Redis {
  export const layer: Layer.Layer<Redis, never, RedisConnection> = Layer.effect(
    Redis,
    Effect.map(RedisConnection, (conn) => makeRedis(traceConnection(conn))),
  );

  /** A Schema-typed key family: `ref(Schema, { prefix, ttl })(id)` with encode/decode/TTL folded in. */
  export const ref = <A, I>(schema: Schema.Schema<A, I>, options: RefOptions) => {
    const codec = Schema.parseJson(schema);
    const encode = Schema.encode(codec);
    const decode = Schema.decode(codec);
    return (id: string): RedisRef<A> => {
      const key = `${options.prefix}:${id}`;
      const decodeSome = (s: string) => decode(s).pipe(toDecodeError, Effect.asSome);
      const get: Effect.Effect<Option.Option<A>, RedisError, Redis> = Effect.flatMap(Redis, (r) =>
        r.get(key),
      ).pipe(
        Effect.flatMap(Option.match({ onNone: () => Effect.succeedNone, onSome: decodeSome })),
      );
      const set: RedisRef<A>["set"] = (value, opts) =>
        encode(value).pipe(
          toDecodeError,
          Effect.flatMap((str) =>
            Effect.flatMap(Redis, (r) =>
              r.set(key, str, opts?.keepTtl ? { keepTtl: true } : { ex: opts?.ttl ?? options.ttl }),
            ),
          ),
        );
      return {
        key,
        get,
        set,
        update: (f, opts) =>
          get.pipe(
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  Effect.fail(
                    new CommandError({
                      message: `ref.update: key "${key}" does not exist`,
                      command: "GET",
                    }),
                  ),
                onSome: (current) => set(f(current), { keepTtl: opts?.keepTtl }),
              }),
            ),
          ),
        getDelete: Effect.flatMap(Redis, (r) => r.getDelete(key)).pipe(
          Effect.flatMap(Option.match({ onNone: () => Effect.succeedNone, onSome: decodeSome })),
        ),
        delete: Effect.flatMap(Redis, (r) => r.del(key)).pipe(Effect.map((n) => n > 0)),
      };
    };
  };

  export const script = <A, I>(options: ScriptOptions<A, I>) => {
    const decodeResult = Schema.decodeUnknown(options.result);
    const isNoScript = (e: RedisError): boolean =>
      e._tag === "CommandError" && (e.code === "NOSCRIPT" || e.message.includes("NOSCRIPT"));
    let cachedSha: string | undefined;
    return (
      keys: ReadonlyArray<string>,
      args: ReadonlyArray<string | number | bigint> = [],
    ): Effect.Effect<A, RedisError, Redis> =>
      Effect.flatMap(Redis, (r) => {
        const tail = [String(keys.length), ...keys, ...args.map(String)];
        const load = r.call("SCRIPT", "LOAD", options.lua).pipe(
          Effect.flatMap(decodeString),
          Effect.tap((sha) =>
            Effect.sync(() => {
              cachedSha = sha;
            }),
          ),
        );
        const evalSha = (sha: string) => r.call("EVALSHA", sha, ...tail);
        const ensure = cachedSha !== undefined ? Effect.succeed(cachedSha) : load;
        return ensure.pipe(
          Effect.flatMap(evalSha),
          Effect.catchIf(isNoScript, () => {
            cachedSha = undefined; // drop the evicted SHA so a failed reload still forces a fresh one next call
            return load.pipe(Effect.flatMap(evalSha));
          }),
          Effect.flatMap((reply) => decodeResult(reply).pipe(toDecodeError)),
        );
      });
  };

  /** Open a `Stream` from the `Redis` service, e.g. `Redis.use((r) => r.subscribe(ch, Schema))`. */
  export const use = <A, E, R>(
    f: (redis: RedisService) => Stream.Stream<A, E, R>,
  ): Stream.Stream<A, E, R | Redis> => Stream.unwrap(Effect.map(Redis, f));
}

export const layerConnection = (
  acquire: Effect.Effect<ConnectionService, ConnectionError, Scope.Scope>,
  options?: { readonly commandTimeout?: Duration.DurationInput },
): Layer.Layer<Redis, ConnectionError> => {
  const timeout = options?.commandTimeout;
  const connection =
    timeout === undefined
      ? acquire
      : acquire.pipe(Effect.map((conn) => timeoutConnection(conn, timeout)));
  return Redis.layer.pipe(Layer.provide(Layer.scoped(RedisConnection, connection)));
};
