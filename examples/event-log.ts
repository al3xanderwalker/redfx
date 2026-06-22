// A typed append-only log on Redis Streams: `Redis.stream` ties a key prefix to a Schema, so each
// entry is one `A` encoded under the single field "d". `add` appends (with optional MAXLEN trim),
// `range` pages history, and `read({ from: "$" })` is a live tail as a `Stream`. A consumer group's
// `consume` runs a handler per entry with at-least-once delivery.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Duration, Effect, Schema, Stream } from "effect";

const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  kind: Schema.Literals(["placed", "paid", "shipped"]),
  at: Schema.Number,
});

const orders = Redis.stream(OrderEvent, { prefix: "events:orders" });

// Producer: append events, keeping only the most recent ~1000 (approximate trim, one round-trip).
export const record = (event: typeof OrderEvent.Type) =>
  orders("eu-west").add(event, { maxLen: 1000, approx: true });

// Bounded history read, newest first.
export const recent = orders("eu-west").range("-", "+", { count: 50 });

// Live tail: only events appended after the read begins.
export const tail = orders("eu-west")
  .read({ from: "$" })
  .pipe(
    Stream.take(10),
    Stream.runForEach((e) => Effect.log(`order ${e.message.orderId}: ${e.message.kind}`)),
    Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))),
  );

// Consumer group: at-least-once delivery, acked only after the handler succeeds.
export const worker = orders("eu-west")
  .consume({ group: "fulfilment", consumer: "worker-1", block: Duration.seconds(5) }, (event) =>
    Effect.log(`handling ${event.kind} for ${event.orderId}`),
  )
  .pipe(Stream.runDrain, Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
