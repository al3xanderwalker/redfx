import {
  CommandError,
  ConnectionError,
  type ConnectionService,
  layerConnection,
  type PushMessage,
  pooledConnection,
  type Redis,
  type RedisCommand,
  type RedisError,
  type RespValue,
} from "@redfx/core";
import {
  type Config,
  type ConfigError,
  type Duration,
  Effect,
  Layer,
  type Scope,
  Stream,
} from "effect";
import {
  Cluster,
  type ClusterNode,
  type ClusterOptions,
  Command,
  Redis as IORedisClient,
  type RedisOptions,
} from "ioredis";

// Redis and Cluster share the surface the port drives, so both run the same connection logic.
type RedisLike = IORedisClient | Cluster;

export interface ClientConfig {
  readonly url?: string;
  readonly options?: RedisOptions;
  /** Effect-level per-command deadline; on expiry the command fails with `TimeoutError`. */
  readonly commandTimeout?: Duration.DurationInput;
}

export interface ClusterConfig {
  readonly nodes: ReadonlyArray<ClusterNode>;
  readonly options?: ClusterOptions;
  readonly commandTimeout?: Duration.DurationInput;
}

const DEFAULTS: RedisOptions = {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => (times > 5 ? null : Math.min(times * 100, 2000)),
  keepAlive: 30_000, // surface a silently-dead peer so a blocking read fails instead of hanging
};

// The consumer Stream owns reconnection (Stream.retry), so disable ioredis's own recovery: a dropped
// XREAD must fail fast with a ConnectionError, not be silently reconnected and resent under us.
const DEDICATED_OVERRIDES: RedisOptions = {
  retryStrategy: () => null,
  autoResendUnfulfilledCommands: false,
  enableOfflineQueue: false,
  maxRetriesPerRequest: 0,
};

const CLUSTER_DEFAULTS: ClusterOptions = {
  lazyConnect: true,
  redisOptions: { maxRetriesPerRequest: 3, keepAlive: 30_000 },
};

const CONNECTION_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "EPIPE",
  "ETIMEDOUT",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ERR_REDIS_CONNECTION_CLOSED",
]);

const mapError = (command: string, cause: unknown): RedisError => {
  const err = cause as { code?: string; message?: string; name?: string };
  const message = err.message ?? String(cause);
  if (
    // MaxRetriesPerRequestError extends AbortError; both mean "aborted because the connection is gone".
    err.name === "MaxRetriesPerRequestError" ||
    err.name === "AbortError" ||
    (err.code !== undefined && CONNECTION_CODES.has(err.code)) ||
    /connection is closed|stream isn't writeable|connection is already closed|connection ended|reached the max retries|command aborted|failed to refresh slots cache|none of (the )?startup nodes/i.test(
      message,
    )
  ) {
    return new ConnectionError({ message, cause });
  }
  return new CommandError({ message, command, code: err.code ?? message.split(" ")[0], cause });
};

const toArg = (a: string | Uint8Array): string | Buffer =>
  typeof a === "string" ? a : Buffer.from(a);

const closeQuietly = (client: RedisLike): Effect.Effect<void> =>
  Effect.promise(() =>
    client
      .quit()
      .then(() => undefined)
      .catch(() => void client.disconnect()),
  );

const makeClient = (
  config: ClientConfig,
): Effect.Effect<IORedisClient, ConnectionError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const client = config.url
          ? new IORedisClient(config.url, { ...DEFAULTS, ...config.options })
          : new IORedisClient({ ...DEFAULTS, ...config.options });
        // Errors surface as command/connect rejections; a listener just prevents Node's unhandled-'error' crash.
        client.on("error", () => {});
        await client.connect();
        return client;
      },
      catch: (cause) => new ConnectionError({ message: "ioredis: failed to connect", cause }),
    }),
    closeQuietly,
  );

const makeDedicatedClient = (
  config: ClientConfig,
): Effect.Effect<IORedisClient, ConnectionError, Scope.Scope> =>
  makeClient({ ...config, options: { ...config.options, ...DEDICATED_OVERRIDES } });

const makeClusterClient = (
  config: ClusterConfig,
): Effect.Effect<Cluster, ConnectionError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const client = new Cluster([...config.nodes], { ...CLUSTER_DEFAULTS, ...config.options });
        client.on("error", () => {});
        await client.connect();
        return client;
      },
      catch: (cause) =>
        new ConnectionError({ message: "ioredis cluster: failed to connect", cause }),
    }),
    closeQuietly,
  );

const send =
  (client: RedisLike) =>
  (command: RedisCommand): Effect.Effect<RespValue, RedisError> =>
    Effect.tryPromise({
      try: () =>
        client.sendCommand(
          new Command(command.name, command.args.map(toArg), { replyEncoding: "utf8" }),
        ) as Promise<RespValue>,
      catch: (cause) => mapError(command.name, cause),
    });

const pipeline =
  (client: RedisLike) =>
  (commands: ReadonlyArray<RedisCommand>): Effect.Effect<ReadonlyArray<RespValue>, RedisError> =>
    Effect.tryPromise({
      // ioredis' batch form dispatches by JS method name (lowercase), unlike `sendCommand`.
      try: () =>
        client.pipeline(commands.map((c) => [c.name.toLowerCase(), ...c.args.map(toArg)])).exec(),
      catch: (cause) => mapError("PIPELINE", cause),
    }).pipe(
      Effect.flatMap((results) => {
        if (results === null) return Effect.succeed<ReadonlyArray<RespValue>>([]);
        const failed = results.find(([err]) => err != null);
        if (failed?.[0]) return Effect.fail(mapError("PIPELINE", failed[0]));
        return Effect.succeed(results.map(([, value]) => value as RespValue));
      }),
    );

const subscribeStream =
  (acquire: Effect.Effect<RedisLike, ConnectionError, Scope.Scope>) =>
  (channels: ReadonlyArray<string>): Stream.Stream<PushMessage, RedisError> =>
    Stream.asyncScoped<PushMessage, RedisError>((emit) =>
      Effect.gen(function* () {
        const sub = yield* acquire;
        sub.on("message", (channel: string, message: string) => emit.single({ channel, message }));
        sub.on("error", (cause) =>
          emit.fail(new ConnectionError({ message: "ioredis: subscriber error", cause })),
        );
        yield* Effect.tryPromise({
          try: () => sub.subscribe(...channels),
          catch: (cause) => mapError("SUBSCRIBE", cause),
        });
      }),
    );

const dedicatedStream =
  (
    acquire: Effect.Effect<RedisLike, ConnectionError, Scope.Scope>,
  ): ConnectionService["dedicated"] =>
  (f) =>
    Stream.unwrapScoped(
      Effect.gen(function* () {
        const client = yield* acquire;
        // Force the socket down first (LIFO) so a blocking read aborts at once, not after quit() waits it out.
        yield* Effect.addFinalizer(() => Effect.sync(() => client.disconnect()));
        return f({
          send: send(client),
          pipeline: pipeline(client),
          subscribe: subscribeStream(acquire),
          dedicated: dedicatedStream(acquire),
          close: closeQuietly(client),
        });
      }),
    );

const makeConnection = (
  acquire: Effect.Effect<RedisLike, ConnectionError, Scope.Scope>,
  // Defaults to `acquire` (cluster reuses its own client); single-node layers pass a fail-fast one.
  dedicatedAcquire: Effect.Effect<RedisLike, ConnectionError, Scope.Scope> = acquire,
): Effect.Effect<ConnectionService, ConnectionError, Scope.Scope> =>
  Effect.gen(function* () {
    const client = yield* acquire;
    return {
      send: send(client),
      pipeline: pipeline(client),
      subscribe: subscribeStream(acquire),
      dedicated: dedicatedStream(dedicatedAcquire),
      close: closeQuietly(client),
    } satisfies ConnectionService;
  });

export namespace IoRedis {
  export const layer = (config: ClientConfig = {}): Layer.Layer<Redis, ConnectionError> =>
    layerConnection(makeConnection(makeClient(config), makeDedicatedClient(config)), {
      commandTimeout: config.commandTimeout,
    });

  export const layerConfig = (
    url: Config.Config<string>,
    config?: Omit<ClientConfig, "url">,
  ): Layer.Layer<Redis, ConnectionError | ConfigError.ConfigError> =>
    Layer.unwrapEffect(Effect.map(url, (resolved) => layer({ ...config, url: resolved })));

  /** Pools `size` command connections; pub/sub still uses a dedicated connection. */
  export const layerPooled = (
    config: ClientConfig & { readonly size: number },
  ): Layer.Layer<Redis, ConnectionError> => {
    const acquire = makeClient(config);
    return layerConnection(
      pooledConnection(
        makeConnection(acquire),
        config.size,
        subscribeStream(acquire),
        dedicatedStream(makeDedicatedClient(config)),
      ),
      { commandTimeout: config.commandTimeout },
    );
  };

  /** Connects to a Redis Cluster (the client pools per-node internally, so no pooled variant).
   *  Cross-slot multi-key commands need a shared hash tag `{…}` or they fail with `CROSSSLOT`. */
  export const layerCluster = (config: ClusterConfig): Layer.Layer<Redis, ConnectionError> =>
    layerConnection(makeConnection(makeClusterClient(config)), {
      commandTimeout: config.commandTimeout,
    });
}
