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

export const decodeStringArray = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<string>, DecodeError> =>
  decodeArray(v).pipe(Effect.flatMap((arr) => Effect.forEach(arr, decodeString)));

const recordFromFlat = (
  arr: ReadonlyArray<RespValue>,
): Effect.Effect<Record<string, string>, DecodeError> => {
  if (arr.length % 2 !== 0)
    return Effect.fail(
      new DecodeError({
        message: `expected even-length map array, got length ${arr.length}`,
        value: arr,
      }),
    );
  return Effect.forEach(arr, decodeString).pipe(
    Effect.map((flat) => {
      const out: Record<string, string> = {};
      for (let i = 0; i + 1 < flat.length; i += 2) {
        const field = flat[i];
        const value = flat[i + 1];
        if (field !== undefined && value !== undefined) out[field] = value;
      }
      return out;
    }),
  );
};

/** `HGETALL`: a flat `[f,v,…]` array on RESP2, a `{f:v}` map object on RESP3. Both normalize here. */
export const decodeRecord = (v: RespValue): Effect.Effect<Record<string, string>, DecodeError> => {
  if (v === null) return Effect.succeed({});
  if (Array.isArray(v)) return recordFromFlat(v);
  if (v instanceof Uint8Array)
    return Effect.fail(
      new DecodeError({ message: "expected map or array reply, got Uint8Array", value: v }),
    );
  if (typeof v === "object")
    return Effect.forEach(Object.entries(v), ([field, raw]) =>
      decodeString(raw).pipe(Effect.map((s) => [field, s] as const)),
    ).pipe(Effect.map((entries) => Object.fromEntries(entries)));
  return Effect.fail(
    new DecodeError({ message: `expected map or array reply, got ${typeof v}`, value: v }),
  );
};

/** Bulk-string or RESP3 double; maps Redis's `inf`/`-inf`/`nan` sentinels. */
export const decodeFloat = (v: RespValue): Effect.Effect<number, DecodeError> => {
  if (typeof v === "number") return Effect.succeed(v);
  const s = asText(v);
  if (s === undefined)
    return Effect.fail(
      new DecodeError({ message: `expected float reply, got ${typeof v}`, value: v }),
    );
  const lower = s.trim().toLowerCase();
  if (lower === "inf" || lower === "+inf") return Effect.succeed(Number.POSITIVE_INFINITY);
  if (lower === "-inf") return Effect.succeed(Number.NEGATIVE_INFINITY);
  if (lower === "nan") return Effect.succeed(Number.NaN);
  const n = Number(s);
  return Number.isNaN(n)
    ? Effect.fail(
        new DecodeError({ message: `expected float, got ${JSON.stringify(s)}`, value: v }),
      )
    : Effect.succeed(n);
};

export const decodeOptionFloat = (
  v: RespValue,
): Effect.Effect<Option.Option<number>, DecodeError> =>
  v === null ? Effect.succeedNone : decodeFloat(v).pipe(Effect.map(Option.some));

export const decodeOptionNumber = (
  v: RespValue,
): Effect.Effect<Option.Option<number>, DecodeError> =>
  v === null ? Effect.succeedNone : decodeNumber(v).pipe(Effect.map(Option.some));

const decodeScorePair = (
  pair: RespValue,
): Effect.Effect<readonly [string, number], DecodeError> => {
  if (!Array.isArray(pair) || pair.length !== 2)
    return Effect.fail(new DecodeError({ message: "expected [member, score] pair", value: pair }));
  const [member, score] = pair;
  return Effect.all([decodeString(member), decodeFloat(score)]);
};

/** `WITHSCORES`: flat `[m,s,…]` on RESP2, `[[m,s],…]` pairs on RESP3. Order-preserving; never a map. */
export const decodeScoredMembers = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<readonly [string, number]>, DecodeError> =>
  decodeArray(v).pipe(
    Effect.flatMap((arr) => {
      if (arr.length === 0) return Effect.succeed<ReadonlyArray<readonly [string, number]>>([]);
      if (Array.isArray(arr[0])) return Effect.forEach(arr, decodeScorePair);
      if (arr.length % 2 !== 0)
        return Effect.fail(
          new DecodeError({
            message: `expected even-length WITHSCORES array, got length ${arr.length}`,
            value: v,
          }),
        );
      const pairs: Array<Effect.Effect<readonly [string, number], DecodeError>> = [];
      for (let i = 0; i + 1 < arr.length; i += 2) {
        const member = arr[i];
        const score = arr[i + 1];
        if (member !== undefined && score !== undefined)
          pairs.push(Effect.all([decodeString(member), decodeFloat(score)]));
      }
      return Effect.all(pairs);
    }),
  );

export interface RawStreamEntry {
  readonly id: string;
  readonly fields: Record<string, string>;
}

/** One `[key, entries]` pair from an `XREAD`/`XREADGROUP` reply. */
export type StreamRead = readonly [string, ReadonlyArray<RawStreamEntry>];

export interface PendingSummary {
  readonly count: number;
  readonly minId: Option.Option<string>;
  readonly maxId: Option.Option<string>;
  readonly consumers: ReadonlyArray<readonly [string, number]>;
}

export interface PendingEntry {
  readonly id: string;
  readonly consumer: string;
  readonly idleMs: number;
  readonly deliveryCount: number;
}

export interface XAutoclaimResult {
  readonly cursor: string;
  readonly entries: ReadonlyArray<RawStreamEntry>;
  readonly deleted: ReadonlyArray<string>;
}

/** A stream entry `[id, fields]`; fields reuse `decodeRecord` (flat array on RESP2, map on RESP3). */
export const decodeEntry = (v: RespValue): Effect.Effect<RawStreamEntry, DecodeError> => {
  if (!Array.isArray(v) || v.length !== 2)
    return Effect.fail(
      new DecodeError({ message: "expected [id, fields] stream entry", value: v }),
    );
  const [idReply, fieldsReply] = v;
  return Effect.all([decodeString(idReply), decodeRecord(fieldsReply)]).pipe(
    Effect.map(([id, fields]) => ({ id, fields })),
  );
};

/** `XRANGE`/`XREVRANGE`/`XCLAIM`: an array of entries; nil → `[]`. */
export const decodeEntries = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<RawStreamEntry>, DecodeError> =>
  v === null
    ? Effect.succeed([])
    : decodeArray(v).pipe(Effect.flatMap((arr) => Effect.forEach(arr, decodeEntry)));

/** `XREAD`/`XREADGROUP`: `[[key, [entry…]]…]` on RESP2, `{key: [entry…]}` on RESP3. Nil (no data /
 *  BLOCK timeout) → `[]` — the consumer loop depends on this. */
export const decodeStreamReads = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<StreamRead>, DecodeError> => {
  if (v === null) return Effect.succeed([]);
  if (Array.isArray(v))
    return Effect.forEach(v, (pair) => {
      if (!Array.isArray(pair) || pair.length !== 2)
        return Effect.fail(
          new DecodeError({ message: "expected [key, entries] stream read", value: pair }),
        );
      const [key, entries] = pair;
      return Effect.all([decodeString(key), decodeEntries(entries)]);
    });
  if (v instanceof Uint8Array)
    return Effect.fail(
      new DecodeError({ message: "expected stream read map or array, got Uint8Array", value: v }),
    );
  if (typeof v === "object")
    return Effect.forEach(Object.entries(v), ([key, entries]) =>
      decodeEntries(entries).pipe(Effect.map((es) => [key, es] as const)),
    );
  return Effect.fail(
    new DecodeError({ message: `expected stream read map or array, got ${typeof v}`, value: v }),
  );
};

/** `XAUTOCLAIM`: `[cursor, [entry…], [deleted…]]`; the Redis 6.2 form omits the deleted list. */
export const decodeXautoclaim = (v: RespValue): Effect.Effect<XAutoclaimResult, DecodeError> => {
  if (!Array.isArray(v) || v.length < 2)
    return Effect.fail(new DecodeError({ message: "expected XAUTOCLAIM reply", value: v }));
  const [cursor, entries, deleted] = v;
  return Effect.all({
    cursor: decodeString(cursor),
    entries: decodeEntries(entries),
    deleted:
      deleted === undefined || deleted === null
        ? Effect.succeed<ReadonlyArray<string>>([])
        : decodeStringArray(deleted),
  });
};

/** `XPENDING key group` summary: `[count, minId, maxId, [[consumer, count]…]]`. */
export const decodePendingSummary = (v: RespValue): Effect.Effect<PendingSummary, DecodeError> => {
  if (!Array.isArray(v) || v.length < 4)
    return Effect.fail(new DecodeError({ message: "expected XPENDING summary", value: v }));
  const [count, minId, maxId, consumers] = v;
  const decodeConsumers = (
    r: RespValue,
  ): Effect.Effect<ReadonlyArray<readonly [string, number]>, DecodeError> =>
    r === null
      ? Effect.succeed([])
      : decodeArray(r).pipe(
          Effect.flatMap((arr) =>
            Effect.forEach(arr, (pair) => {
              if (!Array.isArray(pair) || pair.length !== 2)
                return Effect.fail(
                  new DecodeError({ message: "expected [consumer, count] pair", value: pair }),
                );
              const [name, n] = pair;
              return Effect.all([decodeString(name), decodeNumber(n)]);
            }),
          ),
        );
  return Effect.all({
    count: decodeNumber(count),
    minId: decodeOptionString(minId),
    maxId: decodeOptionString(maxId),
    consumers: decodeConsumers(consumers),
  });
};

/** `XPENDING` extended: rows of `[id, consumer, idleMs, deliveryCount]`; nil → `[]`. */
export const decodePendingEntries = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<PendingEntry>, DecodeError> =>
  v === null
    ? Effect.succeed([])
    : decodeArray(v).pipe(
        Effect.flatMap((arr) =>
          Effect.forEach(arr, (row) => {
            if (!Array.isArray(row) || row.length !== 4)
              return Effect.fail(
                new DecodeError({ message: "expected XPENDING extended row", value: row }),
              );
            const [id, consumer, idle, deliveries] = row;
            return Effect.all({
              id: decodeString(id),
              consumer: decodeString(consumer),
              idleMs: decodeNumber(idle),
              deliveryCount: decodeNumber(deliveries),
            });
          }),
        ),
      );

const scalarText = (v: RespValue | undefined): string | undefined => {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "string") return v;
  if (v instanceof Uint8Array) return textDecoder.decode(v);
  if (typeof v === "number" || typeof v === "bigint" || typeof v === "boolean") return String(v);
  return undefined; // nested arrays/maps (e.g. XINFO's first-entry) are dropped
};

/** `XINFO STREAM`/`XINFO GROUPS` interleave scalars with nested entries; keep only the scalar
 *  fields — this is a raw introspection surface, not a typed model. */
export const decodeInfoRecord = (
  v: RespValue,
): Effect.Effect<Record<string, string>, DecodeError> => {
  if (v === null) return Effect.succeed({});
  if (Array.isArray(v)) {
    const out: Record<string, string> = {};
    for (let i = 0; i + 1 < v.length; i += 2) {
      const field = scalarText(v[i]);
      const value = scalarText(v[i + 1]);
      if (field !== undefined && value !== undefined) out[field] = value;
    }
    return Effect.succeed(out);
  }
  if (v instanceof Uint8Array)
    return Effect.fail(
      new DecodeError({ message: "expected XINFO map or array, got Uint8Array", value: v }),
    );
  if (typeof v === "object") {
    const out: Record<string, string> = {};
    for (const [field, raw] of Object.entries(v)) {
      const s = scalarText(raw);
      if (s !== undefined) out[field] = s;
    }
    return Effect.succeed(out);
  }
  return Effect.fail(
    new DecodeError({ message: `expected XINFO map or array, got ${typeof v}`, value: v }),
  );
};

export const decodeInfoRecordArray = (
  v: RespValue,
): Effect.Effect<ReadonlyArray<Record<string, string>>, DecodeError> =>
  v === null
    ? Effect.succeed([])
    : decodeArray(v).pipe(Effect.flatMap((arr) => Effect.forEach(arr, decodeInfoRecord)));
