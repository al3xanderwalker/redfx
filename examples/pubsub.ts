// Pub/sub as a Stream: `subscribe` runs on a dedicated connection, decodes each message with the
// Schema, and unsubscribes when the stream's scope closes. No `duplicate()`, no manual listener
// bookkeeping, no try/JSON.parse/catch. Type-checked in CI (see tsconfig `include`), not run.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Effect, Schema, Stream } from "effect";

const SseEvent = Schema.Struct({
  topic: Schema.String,
  userId: Schema.String,
  data: Schema.Unknown,
});

const userEvents = (topic: string, userId: string) =>
  Redis.useStream((redis) => redis.subscribe("sse:events", SseEvent)).pipe(
    Stream.filter((e) => e.topic === topic && e.userId === userId),
  );

export const pubsubStream = userEvents("billing", "u_123").pipe(
  Stream.take(10),
  Stream.runForEach((e) => Effect.log(`event: ${JSON.stringify(e.data)}`)),
  Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))),
);
