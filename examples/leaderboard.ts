// Schema-typed sorted sets: `Redis.sortedSet` is a leaderboard — members encode/decode via Schema,
// scores are plain numbers. add / incrBy / rank / rangeWithScores are folded in.

import { Redis } from "@redfx/core";
import { IoRedis } from "@redfx/ioredis";
import { Config, Effect, Schema } from "effect";

const Player = Schema.Struct({ id: Schema.String, name: Schema.String });
const leaderboard = Redis.sortedSet(Player, { prefix: "game:scores" });

export const topPlayers = Effect.gen(function* () {
  const ada = { id: "u_1", name: "Ada" };
  yield* leaderboard("season:1").add(ada, 100);
  const total = yield* leaderboard("season:1").incrBy(ada, 50); // ZINCRBY → new score 150
  const rank = yield* leaderboard("season:1").rank(ada); // Option<number>, 0-based by score
  const top10 = yield* leaderboard("season:1").rangeWithScores(0, 9); // ReadonlyArray<[Player, number]>
  return { total, rank, top10 };
}).pipe(Effect.provide(IoRedis.layerConfig(Config.string("REDIS_URL"))));
