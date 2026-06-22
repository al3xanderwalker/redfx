import {
  Context,
  Data,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { type ConnectionService, RedisConnection } from "./Connection.js";
import {
  decodeArray,
  decodeBoolean,
  decodeEntries,
  decodeFloat,
  decodeInfoRecord,
  decodeInfoRecordArray,
  decodeNumber,
  decodeOptionFloat,
  decodeOptionNumber,
  decodeOptionString,
  decodePendingEntries,
  decodePendingSummary,
  decodeRecord,
  decodeScoredMembers,
  decodeStreamReads,
  decodeString,
  decodeStringArray,
  decodeTtlSeconds,
  decodeXautoclaim,
  type PendingEntry,
  type PendingSummary,
  type RawStreamEntry,
  type StreamRead,
  type XAutoclaimResult,
} from "./internal/decode.js";
import {
  Cmd,
  expirySeconds,
  formatScore,
  hashEntriesArgs,
  make,
  type RedisCommand,
  type SetCommandOptions,
  setArgs,
  type TrimArgs,
  type XReadArgs,
  zaddArgs,
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

export interface StreamReadOptions {
  readonly count?: number;
  readonly block?: Duration.Input;
  /** First id to read from; default `"$"` (live tail — only entries added after the read begins). */
  readonly from?: string;
}

export interface GroupReadOptions {
  readonly group: string;
  readonly consumer: string;
  /** Group start position at creation (ignored if the group already exists); default `"$"`. */
  readonly from?: string;
  readonly count?: number;
  readonly block?: Duration.Input;
}

/** One entry from a consumer group, carrying its own manual `ack` (`XACK key group id`). */
export interface GroupEntry {
  readonly id: string;
  readonly fields: Record<string, string>;
  readonly ack: Effect.Effect<void, RedisError>;
}

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
  readonly expire: (key: string, ttl: Duration.Input) => Effect.Effect<boolean, RedisError>;
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
  readonly hset: (
    key: string,
    entries: Record<string, string> | ReadonlyArray<readonly [string, string]>,
  ) => Effect.Effect<number, RedisError>;
  readonly hget: (key: string, field: string) => Effect.Effect<Option.Option<string>, RedisError>;
  readonly hgetAll: (key: string) => Effect.Effect<Record<string, string>, RedisError>;
  readonly hdel: (
    key: string,
    ...fields: ReadonlyArray<string>
  ) => Effect.Effect<number, RedisError>;
  readonly hexists: (key: string, field: string) => Effect.Effect<boolean, RedisError>;
  readonly hincrBy: (key: string, field: string, by: number) => Effect.Effect<number, RedisError>;
  readonly hkeys: (key: string) => Effect.Effect<ReadonlyArray<string>, RedisError>;
  readonly hvals: (key: string) => Effect.Effect<ReadonlyArray<string>, RedisError>;
  readonly hlen: (key: string) => Effect.Effect<number, RedisError>;
  readonly hmget: (
    key: string,
    ...fields: ReadonlyArray<string>
  ) => Effect.Effect<ReadonlyArray<Option.Option<string>>, RedisError>;
  readonly sadd: (
    key: string,
    ...members: ReadonlyArray<string>
  ) => Effect.Effect<number, RedisError>;
  readonly srem: (
    key: string,
    ...members: ReadonlyArray<string>
  ) => Effect.Effect<number, RedisError>;
  readonly smembers: (key: string) => Effect.Effect<ReadonlyArray<string>, RedisError>;
  readonly sismember: (key: string, member: string) => Effect.Effect<boolean, RedisError>;
  readonly scard: (key: string) => Effect.Effect<number, RedisError>;
  readonly spop: (key: string) => Effect.Effect<Option.Option<string>, RedisError>;
  readonly zadd: (
    key: string,
    entries: ReadonlyArray<readonly [number, string]>,
  ) => Effect.Effect<number, RedisError>;
  readonly zrem: (
    key: string,
    ...members: ReadonlyArray<string>
  ) => Effect.Effect<number, RedisError>;
  readonly zscore: (
    key: string,
    member: string,
  ) => Effect.Effect<Option.Option<number>, RedisError>;
  readonly zincrBy: (key: string, by: number, member: string) => Effect.Effect<number, RedisError>;
  readonly zcard: (key: string) => Effect.Effect<number, RedisError>;
  readonly zrank: (key: string, member: string) => Effect.Effect<Option.Option<number>, RedisError>;
  readonly zrange: (
    key: string,
    start: number,
    stop: number,
  ) => Effect.Effect<ReadonlyArray<string>, RedisError>;
  readonly zrangeWithScores: (
    key: string,
    start: number,
    stop: number,
  ) => Effect.Effect<ReadonlyArray<readonly [string, number]>, RedisError>;
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
    <A, I>(channel: string, schema: Schema.Codec<A, I>): Stream.Stream<A, RedisError>;
  };
  readonly xadd: (
    key: string,
    fields: Record<string, string> | ReadonlyArray<readonly [string, string]>,
    options?: { readonly id?: string; readonly maxLen?: number; readonly approx?: boolean },
  ) => Effect.Effect<string, RedisError>;
  readonly xlen: (key: string) => Effect.Effect<number, RedisError>;
  readonly xrange: (
    key: string,
    start?: string,
    end?: string,
    options?: { readonly count?: number },
  ) => Effect.Effect<ReadonlyArray<RawStreamEntry>, RedisError>;
  readonly xrevrange: (
    key: string,
    start?: string,
    end?: string,
    options?: { readonly count?: number },
  ) => Effect.Effect<ReadonlyArray<RawStreamEntry>, RedisError>;
  readonly xdel: (key: string, ...ids: ReadonlyArray<string>) => Effect.Effect<number, RedisError>;
  readonly xtrim: (key: string, options: TrimArgs) => Effect.Effect<number, RedisError>;
  readonly xread: (
    streams: ReadonlyArray<readonly [string, string]>,
    options?: XReadArgs,
  ) => Effect.Effect<ReadonlyArray<StreamRead>, RedisError>;
  readonly xgroupCreate: (
    key: string,
    group: string,
    options?: { readonly from?: string; readonly mkStream?: boolean },
  ) => Effect.Effect<void, RedisError>;
  readonly xack: (
    key: string,
    group: string,
    ...ids: ReadonlyArray<string>
  ) => Effect.Effect<number, RedisError>;
  readonly xreadGroup: (
    group: string,
    consumer: string,
    streams: ReadonlyArray<readonly [string, string]>,
    options?: XReadArgs & { readonly noAck?: boolean },
  ) => Effect.Effect<ReadonlyArray<StreamRead>, RedisError>;
  readonly xpending: (key: string, group: string) => Effect.Effect<PendingSummary, RedisError>;
  readonly xpendingExtended: (
    key: string,
    group: string,
    options: {
      readonly start?: string;
      readonly end?: string;
      readonly count: number;
      readonly consumer?: string;
      readonly idle?: Duration.Input;
    },
  ) => Effect.Effect<ReadonlyArray<PendingEntry>, RedisError>;
  readonly xclaim: (
    key: string,
    group: string,
    consumer: string,
    minIdle: Duration.Input,
    ids: ReadonlyArray<string>,
  ) => Effect.Effect<ReadonlyArray<RawStreamEntry>, RedisError>;
  readonly xautoclaim: (
    key: string,
    group: string,
    consumer: string,
    minIdle: Duration.Input,
    options?: { readonly start?: string; readonly count?: number },
  ) => Effect.Effect<XAutoclaimResult, RedisError>;
  readonly xinfoStream: (key: string) => Effect.Effect<Record<string, string>, RedisError>;
  readonly xinfoGroups: (
    key: string,
  ) => Effect.Effect<ReadonlyArray<Record<string, string>>, RedisError>;
  /** Consumer `Stream`s on dedicated (blocking) connections; `Redis.stream` is the typed log on top.
   *  BLOCK on the one-shot `xread`/`xreadGroup` above is discouraged — it ties up a pooled slot. */
  readonly streams: {
    readonly read: (
      key: string,
      options?: StreamReadOptions,
    ) => Stream.Stream<RawStreamEntry, RedisError>;
    readonly readGroup: (
      key: string,
      options: GroupReadOptions,
    ) => Stream.Stream<GroupEntry, RedisError>;
    /** One `XAUTOCLAIM` round from `0-0` — a recovery building block; auto-claiming is deferred. */
    readonly claimStale: (
      key: string,
      group: string,
      consumer: string,
      options: { readonly minIdle: Duration.Input; readonly count?: number },
    ) => Effect.Effect<ReadonlyArray<RawStreamEntry>, RedisError>;
  };
}

const toDecodeError = Effect.mapError(
  (error: { readonly message: string }) =>
    new DecodeError({ message: error.message, cause: error }),
);

const isBusyGroup = (e: RedisError): boolean =>
  e._tag === "CommandError" && (e.code === "BUSYGROUP" || e.message.includes("BUSYGROUP"));

// Reconnect a dropped consumer connection (Cache.ts's backoff, capped at 1/30s). Scoped to
// ConnectionError so command/decode errors (WRONGTYPE, NOGROUP, a poison entry) surface, not loop.
const reconnectSchedule = Schedule.exponential("200 millis").pipe(
  Schedule.jittered,
  Schedule.either(Schedule.spaced("30 seconds")),
  // v4 has no `whileInput`: halt the schedule (via its Error channel) when the failure isn't a
  // ConnectionError, so command/decode errors (WRONGTYPE, NOGROUP, a poison entry) surface, not loop.
  Schedule.tapInput((e: RedisError) =>
    e._tag === "ConnectionError" ? Effect.void : Effect.fail(e),
  ),
);

const defaultBlock = Duration.seconds(5);

// Never block forever: a finite round-trip lets the consumer observe interruption and surface a dead
// connection for Stream.retry. 0 / sub-1ms / undefined fall back to the default.
const consumerBlock = (block: Duration.Input | undefined): Duration.Input =>
  block !== undefined && Duration.toMillis(block) >= 1 ? block : defaultBlock;

const makeRedis = (conn: ConnectionService): RedisService => {
  const subscribe = ((channel: string, schema?: Schema.Codec<unknown, unknown>) => {
    const base = conn.subscribe([channel]);
    if (schema === undefined) return Stream.map(base, (m) => m.message);
    const decode = Schema.decodeEffect(Schema.fromJsonString(schema));
    return Stream.mapEffect(base, (m) => decode(m.message).pipe(toDecodeError));
  }) as RedisService["subscribe"];

  const streams: RedisService["streams"] = {
    read: (key, options) => {
      const count = options?.count;
      const block = consumerBlock(options?.block);
      const seed = options?.from ?? "$";
      const pump = (c: ConnectionService, lastIdRef: Ref.Ref<string>) =>
        // v4 has no `repeatEffectChunk`: `forever` re-runs the per-read stream; the XREAD BLOCK
        // inside the effect is what paces the loop, so this never busy-spins on empty reads.
        Stream.forever(
          Stream.fromArrayEffect(
            Ref.get(lastIdRef).pipe(
              Effect.flatMap((lastId) =>
                c
                  .send(Cmd.xread([[key, lastId]], { count, block }))
                  .pipe(Effect.flatMap(decodeStreamReads)),
              ),
              Effect.flatMap((reads) => {
                const entries = reads.flatMap(([, es]) => es);
                const last = entries[entries.length - 1];
                // $ holds until the first non-empty read (a timeout keeps it — nothing arrived to
                // miss); after that, advance to the last id so a re-read never re-anchors and drops.
                return last === undefined
                  ? Effect.succeed<ReadonlyArray<RawStreamEntry>>([])
                  : Ref.set(lastIdRef, last.id).pipe(Effect.as(entries));
              }),
            ),
          ),
        );
      // The lastId Ref lives outside the retried stream, so a reconnect resumes where it left off.
      return Stream.unwrap(
        Ref.make(seed).pipe(
          Effect.map((lastIdRef) =>
            conn
              .dedicated<RawStreamEntry, RedisError>((c) => pump(c, lastIdRef))
              .pipe(Stream.retry(reconnectSchedule)),
          ),
        ),
      );
    },
    readGroup: (key, options) => {
      const { group, consumer } = options;
      const count = options.count;
      const block = consumerBlock(options.block);
      const from = options.from ?? "$";
      const toEntry = (e: RawStreamEntry): GroupEntry => ({
        id: e.id,
        fields: e.fields,
        ack: conn.send(Cmd.xack(key, group, e.id)).pipe(Effect.asVoid),
      });
      // Create the group once (BUSYGROUP is benign); the loop always pulls `>` (the PEL is
      // server-side, never folded), so a reconnect just re-issues it.
      const ensureGroup = conn.send(Cmd.xgroupCreate(key, group, { from, mkStream: true })).pipe(
        Effect.catchIf(isBusyGroup, () => Effect.void),
        Effect.asVoid,
      );
      const pump = (c: ConnectionService) =>
        Stream.forever(
          Stream.fromArrayEffect(
            c.send(Cmd.xreadGroup(group, consumer, [[key, ">"]], { count, block })).pipe(
              Effect.flatMap(decodeStreamReads),
              Effect.map((reads) => reads.flatMap(([, es]) => es).map(toEntry)),
            ),
          ),
        );
      return Stream.unwrap(
        ensureGroup.pipe(
          Effect.as(
            conn.dedicated<GroupEntry, RedisError>(pump).pipe(Stream.retry(reconnectSchedule)),
          ),
        ),
      );
    },
    claimStale: (key, group, consumer, options) =>
      conn
        .send(Cmd.xautoclaim(key, group, consumer, options.minIdle, { count: options.count }))
        .pipe(
          Effect.flatMap(decodeXautoclaim),
          Effect.map((r) => r.entries),
        ),
  };

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
    hset: (key, entries) => {
      const args = hashEntriesArgs(entries);
      if (args.length === 0) return Effect.succeed(0); // a zero-field HSET is an arity error
      return conn.send({ name: "HSET", args: [key, ...args] }).pipe(Effect.flatMap(decodeNumber));
    },
    hget: (key, field) =>
      conn.send(make("HGET", key, field)).pipe(Effect.flatMap(decodeOptionString)),
    hgetAll: (key) => conn.send(make("HGETALL", key)).pipe(Effect.flatMap(decodeRecord)),
    hdel: (key, ...fields) =>
      conn.send({ name: "HDEL", args: [key, ...fields] }).pipe(Effect.flatMap(decodeNumber)),
    hexists: (key, field) =>
      conn.send(make("HEXISTS", key, field)).pipe(Effect.flatMap(decodeBoolean)),
    hincrBy: (key, field, by) =>
      conn.send(make("HINCRBY", key, field, String(by))).pipe(Effect.flatMap(decodeNumber)),
    hkeys: (key) => conn.send(make("HKEYS", key)).pipe(Effect.flatMap(decodeStringArray)),
    hvals: (key) => conn.send(make("HVALS", key)).pipe(Effect.flatMap(decodeStringArray)),
    hlen: (key) => conn.send(make("HLEN", key)).pipe(Effect.flatMap(decodeNumber)),
    hmget: (key, ...fields) =>
      conn.send({ name: "HMGET", args: [key, ...fields] }).pipe(
        Effect.flatMap(decodeArray),
        Effect.flatMap((replies) => Effect.forEach(replies, decodeOptionString)),
      ),
    sadd: (key, ...members) =>
      members.length === 0
        ? Effect.succeed(0)
        : conn.send({ name: "SADD", args: [key, ...members] }).pipe(Effect.flatMap(decodeNumber)),
    srem: (key, ...members) =>
      conn.send({ name: "SREM", args: [key, ...members] }).pipe(Effect.flatMap(decodeNumber)),
    smembers: (key) => conn.send(make("SMEMBERS", key)).pipe(Effect.flatMap(decodeStringArray)),
    sismember: (key, member) =>
      conn.send(make("SISMEMBER", key, member)).pipe(Effect.flatMap(decodeBoolean)),
    scard: (key) => conn.send(make("SCARD", key)).pipe(Effect.flatMap(decodeNumber)),
    spop: (key) => conn.send(make("SPOP", key)).pipe(Effect.flatMap(decodeOptionString)),
    zadd: (key, entries) => {
      const args = zaddArgs(entries);
      if (args.length === 0) return Effect.succeed(0); // a zero-member ZADD is an arity error
      return conn.send({ name: "ZADD", args: [key, ...args] }).pipe(Effect.flatMap(decodeNumber));
    },
    zrem: (key, ...members) =>
      conn.send({ name: "ZREM", args: [key, ...members] }).pipe(Effect.flatMap(decodeNumber)),
    zscore: (key, member) =>
      conn.send(make("ZSCORE", key, member)).pipe(Effect.flatMap(decodeOptionFloat)),
    zincrBy: (key, by, member) =>
      conn.send(make("ZINCRBY", key, formatScore(by), member)).pipe(Effect.flatMap(decodeFloat)),
    zcard: (key) => conn.send(make("ZCARD", key)).pipe(Effect.flatMap(decodeNumber)),
    zrank: (key, member) =>
      conn.send(make("ZRANK", key, member)).pipe(Effect.flatMap(decodeOptionNumber)),
    zrange: (key, start, stop) =>
      conn
        .send(make("ZRANGE", key, String(start), String(stop)))
        .pipe(Effect.flatMap(decodeStringArray)),
    zrangeWithScores: (key, start, stop) =>
      conn
        .send(make("ZRANGE", key, String(start), String(stop), "WITHSCORES"))
        .pipe(Effect.flatMap(decodeScoredMembers)),
    publish: (channel, message) =>
      conn.send(make("PUBLISH", channel, message)).pipe(Effect.flatMap(decodeNumber)),
    ping: conn.send(make("PING")).pipe(Effect.flatMap(decodeString)),
    call: (name, ...args) => conn.send({ name, args }),
    pipeline: (commands) => conn.pipeline(commands),
    subscribe,
    xadd: (key, fields, options) =>
      conn.send(Cmd.xadd(key, fields, options)).pipe(Effect.flatMap(decodeString)),
    xlen: (key) => conn.send(Cmd.xlen(key)).pipe(Effect.flatMap(decodeNumber)),
    xrange: (key, start = "-", end = "+", options) =>
      conn.send(Cmd.xrange(key, start, end, options)).pipe(Effect.flatMap(decodeEntries)),
    xrevrange: (key, start = "-", end = "+", options) =>
      conn.send(Cmd.xrevrange(key, start, end, options)).pipe(Effect.flatMap(decodeEntries)),
    xdel: (key, ...ids) =>
      ids.length === 0
        ? Effect.succeed(0)
        : conn.send(Cmd.xdel(key, ...ids)).pipe(Effect.flatMap(decodeNumber)),
    xtrim: (key, options) => conn.send(Cmd.xtrim(key, options)).pipe(Effect.flatMap(decodeNumber)),
    xread: (streamsArg, options) =>
      conn.send(Cmd.xread(streamsArg, options)).pipe(Effect.flatMap(decodeStreamReads)),
    xgroupCreate: (key, group, options) =>
      conn.send(Cmd.xgroupCreate(key, group, options)).pipe(Effect.asVoid),
    xack: (key, group, ...ids) =>
      ids.length === 0
        ? Effect.succeed(0)
        : conn.send(Cmd.xack(key, group, ...ids)).pipe(Effect.flatMap(decodeNumber)),
    xreadGroup: (group, consumer, streamsArg, options) =>
      conn
        .send(Cmd.xreadGroup(group, consumer, streamsArg, options))
        .pipe(Effect.flatMap(decodeStreamReads)),
    xpending: (key, group) =>
      conn.send(Cmd.xpending(key, group)).pipe(Effect.flatMap(decodePendingSummary)),
    xpendingExtended: (key, group, options) =>
      conn
        .send(Cmd.xpendingExtended(key, group, options))
        .pipe(Effect.flatMap(decodePendingEntries)),
    xclaim: (key, group, consumer, minIdle, ids) =>
      conn.send(Cmd.xclaim(key, group, consumer, minIdle, ids)).pipe(Effect.flatMap(decodeEntries)),
    xautoclaim: (key, group, consumer, minIdle, options) =>
      conn
        .send(Cmd.xautoclaim(key, group, consumer, minIdle, options))
        .pipe(Effect.flatMap(decodeXautoclaim)),
    xinfoStream: (key) => conn.send(Cmd.xinfoStream(key)).pipe(Effect.flatMap(decodeInfoRecord)),
    xinfoGroups: (key) =>
      conn.send(Cmd.xinfoGroups(key)).pipe(Effect.flatMap(decodeInfoRecordArray)),
    streams,
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
  dedicated: conn.dedicated,
  close: conn.close,
});

// Subscriptions are long-lived, so the deadline wraps only send/pipeline.
const timeoutConnection = (
  conn: ConnectionService,
  duration: Duration.Input,
): ConnectionService => ({
  send: (command) =>
    conn.send(command).pipe(
      Effect.timeoutOrElse({
        duration,
        orElse: () =>
          Effect.fail(
            new TimeoutError({ message: "redis command timed out", command: command.name }),
          ),
      }),
    ),
  pipeline: (commands) =>
    conn.pipeline(commands).pipe(
      Effect.timeoutOrElse({
        duration,
        orElse: () => Effect.fail(new TimeoutError({ message: "redis pipeline timed out" })),
      }),
    ),
  subscribe: conn.subscribe,
  // The load-bearing passthrough: a blocking read on a dedicated connection must escape the deadline.
  dedicated: conn.dedicated,
  close: conn.close,
});

export interface RefOptions {
  readonly prefix: string;
  readonly ttl?: Duration.Input;
}

/** Per-write TTL control: `ttl` overrides the configured TTL for this write; `keepTtl` leaves it untouched. */
export interface WriteOptions {
  readonly ttl?: Duration.Input;
  readonly keepTtl?: boolean;
}

export interface RedisRef<A> {
  readonly key: string;
  readonly get: Effect.Effect<Option.Option<A>, RedisError, Redis>;
  readonly set: (value: A, options?: WriteOptions) => Effect.Effect<void, RedisError, Redis>;
  /** Get-then-set, not atomic; use `Redis.script` for compare-and-set. Fails if the key is absent. */
  readonly update: (
    f: (current: A) => A,
    options?: { readonly keepTtl?: boolean },
  ) => Effect.Effect<void, RedisError, Redis>;
  readonly getDelete: Effect.Effect<Option.Option<A>, RedisError, Redis>;
  readonly delete: Effect.Effect<boolean, RedisError, Redis>;
}

/** SADD/ZADD/HSET take no inline `EX`, so a configured `ttl` is re-stamped via a follow-up
 *  `EXPIRE` after each mutating write — two round-trips, not atomic. Use `Redis.script` if you
 *  need add+expire to be atomic. */
const restampTtl = (
  r: RedisService,
  key: string,
  configured: Duration.Input | undefined,
  opts?: WriteOptions,
): Effect.Effect<void, RedisError> => {
  if (opts?.keepTtl) return Effect.void;
  const ttl = opts?.ttl ?? configured;
  return ttl === undefined ? Effect.void : Effect.asVoid(r.expire(key, ttl));
};

/** A Schema-typed set: each member is one `A` ↔ one JSON string. A single bad element fails the read. */
export interface RedisSetRef<A> {
  readonly key: string;
  readonly add: (...values: ReadonlyArray<A>) => Effect.Effect<number, RedisError, Redis>;
  readonly addWith: (
    values: ReadonlyArray<A>,
    options?: WriteOptions,
  ) => Effect.Effect<number, RedisError, Redis>;
  readonly remove: (...values: ReadonlyArray<A>) => Effect.Effect<number, RedisError, Redis>;
  readonly members: Effect.Effect<ReadonlyArray<A>, RedisError, Redis>;
  readonly has: (value: A) => Effect.Effect<boolean, RedisError, Redis>;
  readonly size: Effect.Effect<number, RedisError, Redis>;
  readonly expire: (duration: Duration.Input) => Effect.Effect<boolean, RedisError, Redis>;
  readonly ttl: Effect.Effect<Option.Option<Duration.Duration>, RedisError, Redis>;
  readonly delete: Effect.Effect<boolean, RedisError, Redis>;
}

/** A Schema-typed sorted set (leaderboard): members are `A` ↔ JSON; scores are plain numbers. */
export interface RedisZSetRef<A> {
  readonly key: string;
  readonly add: (
    member: A,
    score: number,
    options?: WriteOptions,
  ) => Effect.Effect<number, RedisError, Redis>;
  readonly remove: (...members: ReadonlyArray<A>) => Effect.Effect<number, RedisError, Redis>;
  readonly score: (member: A) => Effect.Effect<Option.Option<number>, RedisError, Redis>;
  readonly rank: (member: A) => Effect.Effect<Option.Option<number>, RedisError, Redis>;
  readonly incrBy: (
    member: A,
    by: number,
    options?: WriteOptions,
  ) => Effect.Effect<number, RedisError, Redis>;
  readonly range: (
    start: number,
    stop: number,
  ) => Effect.Effect<ReadonlyArray<A>, RedisError, Redis>;
  readonly rangeWithScores: (
    start: number,
    stop: number,
  ) => Effect.Effect<ReadonlyArray<readonly [A, number]>, RedisError, Redis>;
  readonly size: Effect.Effect<number, RedisError, Redis>;
  readonly expire: (duration: Duration.Input) => Effect.Effect<boolean, RedisError, Redis>;
  readonly ttl: Effect.Effect<Option.Option<Duration.Duration>, RedisError, Redis>;
  readonly delete: Effect.Effect<boolean, RedisError, Redis>;
}

/** A Schema-typed hash: a `field(string) → value(A)` map; values are `A` ↔ JSON, fields stay raw. */
export interface RedisHashRef<A> {
  readonly key: string;
  readonly set: (
    field: string,
    value: A,
    options?: WriteOptions,
  ) => Effect.Effect<void, RedisError, Redis>;
  readonly get: (field: string) => Effect.Effect<Option.Option<A>, RedisError, Redis>;
  readonly getAll: Effect.Effect<ReadonlyMap<string, A>, RedisError, Redis>;
  readonly has: (field: string) => Effect.Effect<boolean, RedisError, Redis>;
  readonly remove: (...fields: ReadonlyArray<string>) => Effect.Effect<number, RedisError, Redis>;
  readonly keys: Effect.Effect<ReadonlyArray<string>, RedisError, Redis>;
  readonly size: Effect.Effect<number, RedisError, Redis>;
  readonly expire: (duration: Duration.Input) => Effect.Effect<boolean, RedisError, Redis>;
  readonly ttl: Effect.Effect<Option.Option<Duration.Duration>, RedisError, Redis>;
  readonly delete: Effect.Effect<boolean, RedisError, Redis>;
}

export interface StreamEntry<A> {
  readonly id: string;
  readonly message: A;
}

/** A typed group entry: the decoded message plus its manual `ack`. */
export interface StreamGroupEntry<A> {
  readonly id: string;
  readonly message: A;
  readonly ack: Effect.Effect<void, RedisError>;
}

/** A Schema-typed append-only log: each entry is one `A` stored under the single field `"d"`. The
 *  analogue of `subscribe(channel, Schema)` over a durable, replayable stream. */
export interface RedisStreamRef<A> {
  readonly key: string;
  readonly add: (
    message: A,
    options?: { readonly maxLen?: number; readonly approx?: boolean },
  ) => Effect.Effect<string, RedisError, Redis>;
  readonly range: (
    start?: string,
    end?: string,
    options?: { readonly count?: number },
  ) => Effect.Effect<ReadonlyArray<StreamEntry<A>>, RedisError, Redis>;
  readonly revRange: (
    start?: string,
    end?: string,
    options?: { readonly count?: number },
  ) => Effect.Effect<ReadonlyArray<StreamEntry<A>>, RedisError, Redis>;
  readonly len: Effect.Effect<number, RedisError, Redis>;
  readonly trim: (options: TrimArgs) => Effect.Effect<number, RedisError, Redis>;
  readonly delete: (...ids: ReadonlyArray<string>) => Effect.Effect<number, RedisError, Redis>;
  readonly drop: Effect.Effect<boolean, RedisError, Redis>;
  readonly read: (options?: StreamReadOptions) => Stream.Stream<StreamEntry<A>, RedisError, Redis>;
  readonly group: (
    options: GroupReadOptions,
  ) => Stream.Stream<StreamGroupEntry<A>, RedisError, Redis>;
  /** Reads the group, runs `handler` per message, and acks only after it succeeds (at-least-once).
   *  An unhandled handler or ack error terminates the stream and leaves that entry pending (recover it
   *  with `claimStale`); catch inside `handler` if you want the consumer to outlive a bad message. */
  readonly consume: <E, R>(
    options: GroupReadOptions,
    handler: (message: A) => Effect.Effect<void, E, R>,
  ) => Stream.Stream<A, RedisError | E, Redis | R>;
}

export interface ScriptOptions<A, I> {
  readonly result: Schema.Codec<A, I>;
  readonly lua: string;
}

export class Redis extends Context.Service<Redis, RedisService>()("redfx/Redis") {}

export namespace Redis {
  export const layer: Layer.Layer<Redis, never, RedisConnection> = Layer.effect(
    Redis,
    Effect.map(RedisConnection, (conn) => makeRedis(traceConnection(conn))),
  );

  /** A Schema-typed key family: `ref(Schema, { prefix, ttl })(id)` with encode/decode/TTL folded in. */
  export const ref = <A, I>(schema: Schema.Codec<A, I>, options: RefOptions) => {
    const codec = Schema.fromJsonString(schema);
    const encode = Schema.encodeEffect(codec);
    const decode = Schema.decodeEffect(codec);
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

  /** A Schema-typed set: `setOf(Schema, { prefix, ttl })(id)`. Members encode/decode as one JSON
   *  value each; a configured `ttl` is re-stamped after each mutating write (see `restampTtl`). */
  export const setOf = <A, I>(schema: Schema.Codec<A, I>, options: RefOptions) => {
    const codec = Schema.fromJsonString(schema);
    const encode = Schema.encodeEffect(codec);
    const decode = Schema.decodeEffect(codec);
    return (id: string): RedisSetRef<A> => {
      const key = `${options.prefix}:${id}`;
      const encodeAll = (values: ReadonlyArray<A>) =>
        Effect.forEach(values, (v) => encode(v).pipe(toDecodeError));
      const addWith: RedisSetRef<A>["addWith"] = (values, opts) =>
        values.length === 0
          ? Effect.succeed(0)
          : Effect.gen(function* () {
              const encoded = yield* encodeAll(values);
              const r = yield* Redis;
              const added = yield* r.sadd(key, ...encoded);
              yield* restampTtl(r, key, options.ttl, opts);
              return added;
            });
      return {
        key,
        add: (...values) => addWith(values),
        addWith,
        remove: (...values) =>
          values.length === 0
            ? Effect.succeed(0)
            : encodeAll(values).pipe(
                Effect.flatMap((encoded) => Effect.flatMap(Redis, (r) => r.srem(key, ...encoded))),
              ),
        members: Effect.flatMap(Redis, (r) => r.smembers(key)).pipe(
          Effect.flatMap((members) =>
            Effect.forEach(members, (m) => decode(m).pipe(toDecodeError)),
          ),
        ),
        has: (value) =>
          encode(value).pipe(
            toDecodeError,
            Effect.flatMap((m) => Effect.flatMap(Redis, (r) => r.sismember(key, m))),
          ),
        size: Effect.flatMap(Redis, (r) => r.scard(key)),
        expire: (duration) => Effect.flatMap(Redis, (r) => r.expire(key, duration)),
        ttl: Effect.flatMap(Redis, (r) => r.ttl(key)),
        delete: Effect.flatMap(Redis, (r) => r.del(key)).pipe(Effect.map((n) => n > 0)),
      };
    };
  };

  /** A Schema-typed sorted set (leaderboard): `sortedSet(Schema, { prefix, ttl })(id)`. Members
   *  encode/decode via Schema; scores are plain numbers. Configured `ttl` re-stamped per write. */
  export const sortedSet = <A, I>(schema: Schema.Codec<A, I>, options: RefOptions) => {
    const codec = Schema.fromJsonString(schema);
    const encode = Schema.encodeEffect(codec);
    const decode = Schema.decodeEffect(codec);
    return (id: string): RedisZSetRef<A> => {
      const key = `${options.prefix}:${id}`;
      const decodeMember = (m: string) => decode(m).pipe(toDecodeError);
      return {
        key,
        add: (member, score, opts) =>
          encode(member).pipe(
            toDecodeError,
            Effect.flatMap((m) =>
              Effect.gen(function* () {
                const r = yield* Redis;
                const added = yield* r.zadd(key, [[score, m]]);
                yield* restampTtl(r, key, options.ttl, opts);
                return added;
              }),
            ),
          ),
        remove: (...members) =>
          members.length === 0
            ? Effect.succeed(0)
            : Effect.forEach(members, (m) => encode(m).pipe(toDecodeError)).pipe(
                Effect.flatMap((encoded) => Effect.flatMap(Redis, (r) => r.zrem(key, ...encoded))),
              ),
        score: (member) =>
          encode(member).pipe(
            toDecodeError,
            Effect.flatMap((m) => Effect.flatMap(Redis, (r) => r.zscore(key, m))),
          ),
        rank: (member) =>
          encode(member).pipe(
            toDecodeError,
            Effect.flatMap((m) => Effect.flatMap(Redis, (r) => r.zrank(key, m))),
          ),
        incrBy: (member, by, opts) =>
          encode(member).pipe(
            toDecodeError,
            Effect.flatMap((m) =>
              Effect.gen(function* () {
                const r = yield* Redis;
                const score = yield* r.zincrBy(key, by, m);
                yield* restampTtl(r, key, options.ttl, opts);
                return score;
              }),
            ),
          ),
        range: (start, stop) =>
          Effect.flatMap(Redis, (r) => r.zrange(key, start, stop)).pipe(
            Effect.flatMap((members) => Effect.forEach(members, decodeMember)),
          ),
        rangeWithScores: (start, stop) =>
          Effect.flatMap(Redis, (r) => r.zrangeWithScores(key, start, stop)).pipe(
            Effect.flatMap((pairs) =>
              Effect.forEach(pairs, ([m, score]) =>
                decodeMember(m).pipe(Effect.map((a) => [a, score] as const)),
              ),
            ),
          ),
        size: Effect.flatMap(Redis, (r) => r.zcard(key)),
        expire: (duration) => Effect.flatMap(Redis, (r) => r.expire(key, duration)),
        ttl: Effect.flatMap(Redis, (r) => r.ttl(key)),
        delete: Effect.flatMap(Redis, (r) => r.del(key)).pipe(Effect.map((n) => n > 0)),
      };
    };
  };

  /** A Schema-typed hash: `hashOf(Schema, { prefix, ttl })(id)`. A `field(string) → value(A)` map;
   *  each value encodes/decodes via Schema, fields stay raw strings. Configured `ttl` re-stamped per write. */
  export const hashOf = <A, I>(schema: Schema.Codec<A, I>, options: RefOptions) => {
    const codec = Schema.fromJsonString(schema);
    const encode = Schema.encodeEffect(codec);
    const decode = Schema.decodeEffect(codec);
    return (id: string): RedisHashRef<A> => {
      const key = `${options.prefix}:${id}`;
      return {
        key,
        set: (field, value, opts) =>
          encode(value).pipe(
            toDecodeError,
            Effect.flatMap((v) =>
              Effect.gen(function* () {
                const r = yield* Redis;
                yield* r.hset(key, [[field, v]]);
                yield* restampTtl(r, key, options.ttl, opts);
              }),
            ),
          ),
        get: (field) =>
          Effect.flatMap(Redis, (r) => r.hget(key, field)).pipe(
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.succeedNone,
                onSome: (v) => decode(v).pipe(toDecodeError, Effect.asSome),
              }),
            ),
          ),
        getAll: Effect.flatMap(Redis, (r) => r.hgetAll(key)).pipe(
          Effect.flatMap((record) =>
            Effect.forEach(Object.entries(record), ([field, v]) =>
              decode(v).pipe(
                toDecodeError,
                Effect.map((a) => [field, a] as const),
              ),
            ),
          ),
          Effect.map((entries) => new Map(entries)),
        ),
        has: (field) => Effect.flatMap(Redis, (r) => r.hexists(key, field)),
        remove: (...fields) =>
          fields.length === 0
            ? Effect.succeed(0)
            : Effect.flatMap(Redis, (r) => r.hdel(key, ...fields)),
        keys: Effect.flatMap(Redis, (r) => r.hkeys(key)),
        size: Effect.flatMap(Redis, (r) => r.hlen(key)),
        expire: (duration) => Effect.flatMap(Redis, (r) => r.expire(key, duration)),
        ttl: Effect.flatMap(Redis, (r) => r.ttl(key)),
        delete: Effect.flatMap(Redis, (r) => r.del(key)).pipe(Effect.map((n) => n > 0)),
      };
    };
  };

  /** A Schema-typed append-only log: `stream(Schema, { prefix })(id)`. Each entry is one `A` ↔ JSON
   *  under the single field `"d"`; `read`/`group`/`consume` deliver decoded entries as a `Stream`,
   *  `range`/`revRange` page history. Reach for `r.xadd` to write raw multi-field entries. */
  export const stream = <A, I>(
    schema: Schema.Codec<A, I>,
    options: { readonly prefix: string },
  ) => {
    const codec = Schema.fromJsonString(schema);
    const encode = Schema.encodeEffect(codec);
    const decode = Schema.decodeEffect(codec);
    return (id: string): RedisStreamRef<A> => {
      const key = `${options.prefix}:${id}`;
      // A missing "d" decodes the empty string, which fails deterministically — matching `ref.get`.
      const decodeMessage = (fields: Record<string, string>) =>
        decode(fields.d ?? "").pipe(toDecodeError);
      const decodeTyped = (e: RawStreamEntry): Effect.Effect<StreamEntry<A>, DecodeError> =>
        decodeMessage(e.fields).pipe(Effect.map((message) => ({ id: e.id, message })));
      return {
        key,
        add: (message, opts) =>
          encode(message).pipe(
            toDecodeError,
            Effect.flatMap((d) =>
              Effect.flatMap(Redis, (r) =>
                r.xadd(key, { d }, { maxLen: opts?.maxLen, approx: opts?.approx }),
              ),
            ),
          ),
        range: (start = "-", end = "+", opts) =>
          Effect.flatMap(Redis, (r) => r.xrange(key, start, end, opts)).pipe(
            Effect.flatMap((entries) => Effect.forEach(entries, decodeTyped)),
          ),
        revRange: (start = "-", end = "+", opts) =>
          Effect.flatMap(Redis, (r) => r.xrevrange(key, start, end, opts)).pipe(
            Effect.flatMap((entries) => Effect.forEach(entries, decodeTyped)),
          ),
        len: Effect.flatMap(Redis, (r) => r.xlen(key)),
        trim: (opts) => Effect.flatMap(Redis, (r) => r.xtrim(key, opts)),
        delete: (...ids) =>
          ids.length === 0 ? Effect.succeed(0) : Effect.flatMap(Redis, (r) => r.xdel(key, ...ids)),
        drop: Effect.flatMap(Redis, (r) => r.del(key)).pipe(Effect.map((n) => n > 0)),
        read: (opts) =>
          Redis.useStream((r) => r.streams.read(key, opts)).pipe(Stream.mapEffect(decodeTyped)),
        group: (opts) =>
          Redis.useStream((r) => r.streams.readGroup(key, opts)).pipe(
            Stream.mapEffect((e) =>
              decodeMessage(e.fields).pipe(
                Effect.map((message) => ({ id: e.id, message, ack: e.ack })),
              ),
            ),
          ),
        consume: (opts, handler) =>
          Redis.useStream((r) => r.streams.readGroup(key, opts)).pipe(
            Stream.mapEffect((e) =>
              decodeMessage(e.fields).pipe(
                Effect.flatMap((message) =>
                  handler(message).pipe(Effect.andThen(e.ack), Effect.as(message)),
                ),
              ),
            ),
          ),
      };
    };
  };

  export const script = <A, I>(options: ScriptOptions<A, I>) => {
    const decodeResult = Schema.decodeUnknownEffect(options.result);
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

  /** Open a `Stream` from the `Redis` service, e.g. `Redis.useStream((r) => r.subscribe(ch, Schema))`.
   *  Named `useStream` (not `use`) because v4's `Context.Service` reserves `.use` for `Effect`s. */
  export const useStream = <A, E, R>(
    f: (redis: RedisService) => Stream.Stream<A, E, R>,
  ): Stream.Stream<A, E, R | Redis> => Stream.unwrap(Effect.map(Redis, f));
}

export const layerConnection = (
  acquire: Effect.Effect<ConnectionService, ConnectionError, Scope.Scope>,
  options?: { readonly commandTimeout?: Duration.Input },
): Layer.Layer<Redis, ConnectionError> => {
  const timeout = options?.commandTimeout;
  const connection =
    timeout === undefined
      ? acquire
      : acquire.pipe(Effect.map((conn) => timeoutConnection(conn, timeout)));
  return Redis.layer.pipe(Layer.provide(Layer.effect(RedisConnection, connection)));
};
