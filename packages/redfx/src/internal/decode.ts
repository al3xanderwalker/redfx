import { Duration, Effect, Option } from "effect";
import { DecodeError } from "../RedisError.js";
import type { RespValue } from "../RespValue.js";

const textDecoder = new TextDecoder();

const asText = (v: RespValue): string | undefined => {
  if (typeof v === "string") return v;
  if (v instanceof Uint8Array) return textDecoder.decode(v);
  if (typeof v === "number" || typeof v === "bigint") return String(v);
  return undefined;
};

export const decodeOptionString = (
  v: RespValue,
): Effect.Effect<Option.Option<string>, DecodeError> => {
  if (v === null) return Effect.succeedNone;
  const s = asText(v);
  return s === undefined
    ? Effect.fail(
        new DecodeError({ message: `expected bulk string or nil, got ${typeof v}`, value: v }),
      )
    : Effect.succeed(Option.some(s));
};

export const decodeString = (v: RespValue): Effect.Effect<string, DecodeError> => {
  const s = asText(v);
  return s === undefined
    ? Effect.fail(new DecodeError({ message: `expected string, got ${typeof v}`, value: v }))
    : Effect.succeed(s);
};

export const decodeNumber = (v: RespValue): Effect.Effect<number, DecodeError> => {
  if (typeof v === "number") return Effect.succeed(v);
  if (typeof v === "bigint") return Effect.succeed(Number(v));
  if (typeof v === "string" && v.trim() !== "" && !Number.isNaN(Number(v)))
    return Effect.succeed(Number(v));
  return Effect.fail(
    new DecodeError({ message: `expected integer reply, got ${typeof v}`, value: v }),
  );
};

export const decodeBoolean = (v: RespValue): Effect.Effect<boolean, DecodeError> => {
  if (typeof v === "boolean") return Effect.succeed(v);
  if (typeof v === "number") return Effect.succeed(v !== 0);
  if (typeof v === "bigint") return Effect.succeed(v !== 0n);
  if (typeof v === "string") {
    if (v === "1" || v.toLowerCase() === "true") return Effect.succeed(true);
    if (v === "0" || v.toLowerCase() === "false") return Effect.succeed(false);
  }
  return Effect.fail(
    new DecodeError({ message: `expected boolean-ish reply, got ${typeof v}`, value: v }),
  );
};

/** `-2` (no key) and `-1` (no expiry) both become `None`. */
export const decodeTtlSeconds = (
  v: RespValue,
): Effect.Effect<Option.Option<Duration.Duration>, DecodeError> =>
  decodeNumber(v).pipe(
    Effect.map((n) => (n < 0 ? Option.none() : Option.some(Duration.seconds(n)))),
  );

export const decodeArray = (v: RespValue): Effect.Effect<ReadonlyArray<RespValue>, DecodeError> =>
  Array.isArray(v)
    ? Effect.succeed(v)
    : Effect.fail(new DecodeError({ message: `expected array reply, got ${typeof v}`, value: v }));
