import { expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import * as fc from "fast-check";

const OtpRecord = Schema.Struct({ codeHash: Schema.String, attempts: Schema.Number });
const codec = Schema.fromJsonString(OtpRecord);
const encode = Schema.encodeEffect(codec);
const decode = Schema.decodeEffect(codec);

test("Redis.ref codec round-trips through JSON", () => {
  fc.assert(
    fc.property(fc.record({ codeHash: fc.string(), attempts: fc.integer() }), (value) => {
      const roundTripped = Effect.runSync(encode(value).pipe(Effect.flatMap(decode)));
      expect(roundTripped).toEqual(value);
    }),
  );
});

test("decode rejects malformed JSON with a typed ParseError", () => {
  const exit = Effect.runSyncExit(decode("not json"));
  expect(exit._tag).toBe("Failure");
});
