import { IoRedis } from "@redfx/ioredis";
import { Config } from "effect";
import { runConformance } from "./suite.js";

runConformance({
  name: "ioredis",
  layer: (url) => IoRedis.layer({ url }),
  unreachableLayer: (url) =>
    IoRedis.layer({
      url,
      options: {
        retryStrategy: () => null,
        maxRetriesPerRequest: 0,
        connectTimeout: 1500,
        enableOfflineQueue: false,
      },
    }),
  pooledLayer: (url) => IoRedis.layerPooled({ url, size: 4 }),
  configLayer: (url) => IoRedis.layerConfig(Config.succeed(url)),
});
