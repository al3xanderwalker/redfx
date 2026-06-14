import { BunRedis } from "@redfx/bun";
import { Config } from "effect";
import { runConformance } from "./suite.js";

runConformance({
  name: "bun",
  layer: (url) => BunRedis.layer({ url }),
  unreachableLayer: (url) =>
    BunRedis.layer({
      url,
      options: {
        maxRetries: 0,
        connectionTimeout: 1500,
        enableOfflineQueue: false,
        autoReconnect: false,
      },
    }),
  pooledLayer: (url) => BunRedis.layerPooled({ url, size: 4 }),
  configLayer: (url) => BunRedis.layerConfig(Config.succeed(url)),
});
