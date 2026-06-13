import { RedisClient } from "bun";
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
} from "redfx";

type BunRedisOptions = ConstructorParameters<typeof RedisClient>[1];

export interface ClientConfig {
  readonly url?: string;
  readonly options?: BunRedisOptions;
  /** Effect-level per-command deadline; on expiry the command fails with `TimeoutError`. */
  readonly commandTimeout?: Duration.DurationInput;
}

const textDecoder = new TextDecoder();
const toStr = (a: string | Uint8Array): string =>
  typeof a === "string" ? a : textDecoder.decode(a);

const CONNECTION_CODES = new Set([
  "ERR_REDIS_CONNECTION_CLOSED",
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "ENOTFOUND",
]);

const mapError = (command: string, cause: unknown): RedisError => {
  const err = cause as { code?: string; message?: string };
  const message = err.message ?? String(cause);
  if (
    (err.code !== undefined && CONNECTION_CODES.has(err.code)) ||
    /connection (is )?closed|connection refused|ERR_REDIS_CONNECTION_CLOSED/i.test(message)
  ) {
    return new ConnectionError({ message, cause });
  }
  return new CommandError({ message, command, code: err.code ?? message.split(" ")[0], cause });
};

const makeClient = (
  config: ClientConfig,
): Effect.Effect<RedisClient, ConnectionError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const client = new RedisClient(config.url, config.options);
        await client.connect();
        return client;
      },
      catch: (cause) => new ConnectionError({ message: "bun: failed to connect", cause }),
    }),
    (client) => Effect.sync(() => client.close()),
  );

const send =
  (client: RedisClient) =>
  (command: RedisCommand): Effect.Effect<RespValue, RedisError> =>
    Effect.tryPromise({
      try: () =>
        client.send(command.name, command.args.map(toStr)) as unknown as Promise<RespValue>,
      catch: (cause) => mapError(command.name, cause),
    });

// Bun auto-pipelines concurrent sends, so Promise.all is a real pipeline on the wire (order preserved).
const pipeline =
  (client: RedisClient) =>
  (commands: ReadonlyArray<RedisCommand>): Effect.Effect<ReadonlyArray<RespValue>, RedisError> =>
    Effect.tryPromise({
      try: () =>
        Promise.all(
          commands.map((c) => client.send(c.name, c.args.map(toStr))),
        ) as unknown as Promise<ReadonlyArray<RespValue>>,
      catch: (cause) => mapError("PIPELINE", cause),
    });

const subscribeStream =
  (config: ClientConfig) =>
  (channels: ReadonlyArray<string>): Stream.Stream<PushMessage, RedisError> =>
    Stream.asyncScoped<PushMessage, RedisError>((emit) =>
      Effect.gen(function* () {
        const sub = yield* makeClient(config);
        // Surface an unexpected drop; the finalizer (runs before our own close) suppresses teardown.
        let active = true;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            active = false;
          }),
        );
        sub.onclose = (cause) => {
          if (active)
            emit.fail(new ConnectionError({ message: "bun: subscriber connection closed", cause }));
        };
        const listener = (message: string, channel: string) => emit.single({ channel, message });
        yield* Effect.forEach(channels, (channel) =>
          Effect.tryPromise({
            try: () => sub.subscribe(channel, listener),
            catch: (cause) => mapError("SUBSCRIBE", cause),
          }),
        );
      }),
    );

const makeConnection = (
  config: ClientConfig,
): Effect.Effect<ConnectionService, ConnectionError, Scope.Scope> =>
  Effect.gen(function* () {
    const client = yield* makeClient(config);
    return {
      send: send(client),
      pipeline: pipeline(client),
      subscribe: subscribeStream(config),
      close: Effect.sync(() => client.close()),
    } satisfies ConnectionService;
  });

export namespace BunRedis {
  export const layer = (config: ClientConfig = {}): Layer.Layer<Redis, ConnectionError> =>
    layerConnection(makeConnection(config), { commandTimeout: config.commandTimeout });

  export const layerConfig = (
    url: Config.Config<string>,
    config?: Omit<ClientConfig, "url">,
  ): Layer.Layer<Redis, ConnectionError | ConfigError.ConfigError> =>
    Layer.unwrapEffect(Effect.map(url, (resolved) => layer({ ...config, url: resolved })));

  /** Pools `size` command connections; pub/sub uses a dedicated connection. */
  export const layerPooled = (
    config: ClientConfig & { readonly size: number },
  ): Layer.Layer<Redis, ConnectionError> =>
    layerConnection(
      pooledConnection(makeConnection(config), config.size, subscribeStream(config)),
      {
        commandTimeout: config.commandTimeout,
      },
    );
}
