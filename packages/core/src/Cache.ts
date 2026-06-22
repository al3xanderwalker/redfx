import {
  Cache,
  Clock,
  Duration,
  Effect,
  Option,
  Schedule,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { Redis } from "./Redis.js";
import { DecodeError, type RedisError, TimeoutError } from "./RedisError.js";

export interface StampedeOptions {
  /** Lock lifetime; a SAFETY bound that must exceed the worst-case lookup. Default `"10 seconds"`. */
  readonly lockTtl?: Duration.Input;
  /** How long a waiter polls before giving up. Default = `lockTtl`. */
  readonly waitTimeout?: Duration.Input;
  /** Base poll interval; jittered on each round. Default `"75 millis"`. */
  readonly pollInterval?: Duration.Input;
  /** On wait timeout: `"compute"` (degrade to a lock-free lookup) or `"fail"`. Default `"compute"`. */
  readonly onTimeout?: "compute" | "fail";
}

export interface CacheOptions<A, I, E, R> {
  readonly schema: Schema.Codec<A, I>;
  readonly prefix: string;
  readonly ttl: Duration.Input;
  readonly lookup: (key: string) => Effect.Effect<A, E, R>;
  /** Cross-instance single-flight on a miss. `true` uses the defaults. */
  readonly stampede?: boolean | StampedeOptions;
}

export interface TieredOptions<A, I, E, R> extends CacheOptions<A, I, E, R> {
  /** In-process L1 in front of the Redis L2. Keep `memory.ttl <= ttl`. */
  readonly memory: { readonly capacity: number; readonly ttl: Duration.Input };
}

export interface RedisCacheHandle<A, E, R> {
  readonly get: (key: string) => Effect.Effect<A, E | RedisError, R | Redis>;
  readonly set: (key: string, value: A) => Effect.Effect<void, RedisError, Redis>;
  readonly invalidate: (key: string) => Effect.Effect<void, RedisError, Redis>;
  readonly refresh: (key: string) => Effect.Effect<A, E | RedisError, R | Redis>;
}

export interface TieredCacheHandle<A, E> {
  readonly get: (key: string) => Effect.Effect<A, E | RedisError>;
  readonly set: (key: string, value: A) => Effect.Effect<void, RedisError>;
  readonly invalidate: (key: string) => Effect.Effect<void, RedisError>;
  readonly refresh: (key: string) => Effect.Effect<A, E | RedisError>;
}

const toMillis = (d: Duration.Input): number => Duration.toMillis(d);

// floored at 1: a `PX 0` lock would error
const millisString = (d: Duration.Input): string => String(Math.max(1, Math.ceil(toMillis(d))));

interface ResolvedStampede {
  readonly lockTtl: Duration.Input;
  readonly waitTimeout: Duration.Input;
  readonly pollInterval: Duration.Input;
  readonly onTimeout: "compute" | "fail";
}

const resolveStampede = (s: boolean | StampedeOptions | undefined): ResolvedStampede | null => {
  if (!s) return null;
  const opts = s === true ? {} : s;
  const lockTtl = opts.lockTtl ?? "10 seconds";
  return {
    lockTtl,
    waitTimeout: opts.waitTimeout ?? lockTtl,
    pollInterval: opts.pollInterval ?? "75 millis",
    onTimeout: opts.onTimeout ?? "compute",
  };
};

// compare-and-delete by token, so an unconditional release is safe
const releaseLua =
  "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end";

const makeCore = <A, I, E, R>(options: CacheOptions<A, I, E, R>) => {
  const refFor = Redis.ref(options.schema, { prefix: options.prefix, ttl: options.ttl });
  const st = resolveStampede(options.stampede);
  const releaseScript = Redis.script({ result: Schema.Number, lua: releaseLua });

  const store = (key: string, value: A): Effect.Effect<A, RedisError, Redis> =>
    refFor(key).set(value, { ttl: options.ttl }).pipe(Effect.as(value));

  const lookupAndStore = (key: string): Effect.Effect<A, E | RedisError, Redis | R> =>
    options.lookup(key).pipe(Effect.flatMap((v) => store(key, v)));

  const cacheAside = (key: string): Effect.Effect<A, E | RedisError, Redis | R> =>
    refFor(key).get.pipe(
      Effect.flatMap(Option.match({ onNone: () => lookupAndStore(key), onSome: Effect.succeed })),
    );

  const acquireLock = (
    lockKey: string,
    token: string,
    lockMs: string,
  ): Effect.Effect<boolean, RedisError, Redis> =>
    Effect.flatMap(Redis, (r) => r.call("SET", lockKey, token, "PX", lockMs, "NX")).pipe(
      Effect.flatMap((reply) =>
        reply === "OK"
          ? Effect.succeed(true)
          : reply === null
            ? Effect.succeed(false)
            : Effect.fail(new DecodeError({ message: "unexpected SET NX reply", value: reply })),
      ),
    );

  const releaseLock = (lockKey: string, token: string): Effect.Effect<void, never, Redis> =>
    releaseScript([lockKey], [token]).pipe(Effect.ignore);

  const stampedeGet = (
    key: string,
    s: ResolvedStampede,
  ): Effect.Effect<A, E | RedisError, Redis | R> => {
    const lockKey = `${options.prefix}:lock:${key}`;
    const lockMs = millisString(s.lockTtl);
    const pollMs = toMillis(s.pollInterval);
    const waitMs = toMillis(s.waitTimeout);

    const tryRound = (token: string): Effect.Effect<Option.Option<A>, E | RedisError, Redis | R> =>
      refFor(key).get.pipe(
        Effect.flatMap(
          Option.match({
            onSome: Effect.succeedSome,
            onNone: () =>
              Effect.acquireUseRelease(
                acquireLock(lockKey, token, lockMs),
                (acquired) =>
                  acquired ? lookupAndStore(key).pipe(Effect.asSome) : Effect.succeedNone,
                (acquired) => (acquired ? releaseLock(lockKey, token) : Effect.void),
              ),
          }),
        ),
      );

    const onTimeout =
      s.onTimeout === "fail"
        ? Effect.fail(
            new TimeoutError({
              message: `cache stampede wait timed out for "${key}"`,
              command: "GET",
              key,
            }),
          )
        : lookupAndStore(key);

    // jitter so waiters don't re-poll in lockstep
    const jitterSleep = Effect.flatMap(
      Effect.sync(() => Duration.millis(pollMs * (0.5 + Math.random()))),
      Effect.sleep,
    );

    return Effect.gen(function* () {
      const token = yield* Effect.sync(() => crypto.randomUUID());
      const deadline = (yield* Clock.currentTimeMillis) + waitMs;
      const loop: Effect.Effect<A, E | RedisError, Redis | R> = Effect.flatMap(
        tryRound(token),
        Option.match({
          onSome: Effect.succeed,
          onNone: () =>
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) =>
                now >= deadline ? onTimeout : jitterSleep.pipe(Effect.flatMap(() => loop)),
              ),
            ),
        }),
      );
      return yield* loop;
    });
  };

  const l2Get = (key: string): Effect.Effect<A, E | RedisError, Redis | R> =>
    st === null ? cacheAside(key) : stampedeGet(key, st);

  return {
    refFor,
    store,
    lookupAndStore,
    l2Get,
    del: (key: string) => refFor(key).delete,
  };
};

const InvMsg = Schema.Struct({ i: Schema.String, k: Schema.String });

export namespace RedisCache {
  /** Distributed read-through cache-aside, with optional cross-instance stampede protection. */
  export const make = <A, I, E, R>(
    options: CacheOptions<A, I, E, R>,
  ): RedisCacheHandle<A, E, R> => {
    const core = makeCore(options);
    return {
      get: core.l2Get,
      set: (key, value) => core.store(key, value).pipe(Effect.asVoid),
      invalidate: (key) => core.del(key).pipe(Effect.asVoid),
      refresh: (key) => core.lookupAndStore(key),
    };
  };

  /** Two-tier cache: in-process L1 over the Redis L2, kept coherent across instances by pub/sub
   *  invalidation. Scoped: forks a listener fiber. Handle methods are context-free. */
  export const makeTiered = <A, I, E, R>(
    options: TieredOptions<A, I, E, R>,
  ): Effect.Effect<TieredCacheHandle<A, E>, never, Redis | R | Scope.Scope> =>
    Effect.gen(function* () {
      const core = makeCore(options);
      const svc = yield* Redis;
      const env = yield* Effect.context<R>();
      const instanceId = crypto.randomUUID();
      const channel = `redfx:cache:inv:${options.prefix}`;

      if (toMillis(options.memory.ttl) > toMillis(options.ttl)) {
        yield* Effect.logWarning(
          `redfx cache "${options.prefix}": memory.ttl exceeds L2 ttl; L1 may serve values Redis has already expired`,
        );
      }

      // L1 lookup must run with Redis + R already provided, before Cache.make captures it
      const provided = (key: string) =>
        core.l2Get(key).pipe(Effect.provideService(Redis, svc), Effect.provide(env));

      const l1 = yield* Cache.make({
        capacity: options.memory.capacity,
        timeToLive: options.memory.ttl,
        lookup: provided,
      });

      const publishInv = (key: string): Effect.Effect<void> =>
        Effect.sync(() => JSON.stringify({ i: instanceId, k: key })).pipe(
          Effect.flatMap((msg) => svc.publish(channel, msg)),
          Effect.tapError((e) =>
            Effect.logError(`redfx cache "${options.prefix}": invalidation publish failed`, e),
          ),
          Effect.ignore,
        );

      const refreshOne = (key: string): Effect.Effect<A, E | RedisError> =>
        core.lookupAndStore(key).pipe(Effect.provideService(Redis, svc), Effect.provide(env));

      // skip our own messages; reconnect with capped backoff if the sub drops
      const listener = Redis.useStream((r) => r.subscribe(channel, InvMsg)).pipe(
        Stream.filter((m) => m.i !== instanceId),
        Stream.runForEach((m) => Cache.invalidate(l1, m.k)),
        Effect.provideService(Redis, svc),
        Effect.retry(
          Schedule.exponential("200 millis").pipe(
            Schedule.jittered,
            Schedule.either(Schedule.spaced("30 seconds")),
          ),
        ),
        Effect.catch((e) =>
          Effect.logError(`redfx cache "${options.prefix}": invalidation listener stopped`, e),
        ),
      );
      yield* Effect.forkScoped(listener);

      return {
        get: (key) => Cache.get(l1, key),
        set: (key, value) =>
          core.store(key, value).pipe(
            Effect.provideService(Redis, svc),
            Effect.flatMap(() => Cache.set(l1, key, value)),
            Effect.flatMap(() => publishInv(key)),
          ),
        invalidate: (key) =>
          core.del(key).pipe(
            Effect.provideService(Redis, svc),
            Effect.flatMap(() => Cache.invalidate(l1, key)),
            Effect.flatMap(() => publishInv(key)),
          ),
        refresh: (key) =>
          refreshOne(key).pipe(
            Effect.tap((v) => Cache.set(l1, key, v)),
            Effect.tap(() => publishInv(key)),
          ),
      };
    });
}
