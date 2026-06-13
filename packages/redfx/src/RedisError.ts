import { Data } from "effect";

/** Connection lost or unusable (refused, reset, closed, auth). */
export class ConnectionError extends Data.TaggedError("ConnectionError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Server replied with an error (WRONGTYPE, NOSCRIPT, OOM). */
export class CommandError extends Data.TaggedError("CommandError")<{
  readonly message: string;
  readonly command?: string;
  readonly code?: string;
  readonly cause?: unknown;
}> {}

export class DecodeError extends Data.TaggedError("DecodeError")<{
  readonly message: string;
  readonly value?: unknown;
  readonly cause?: unknown;
}> {}

export class TimeoutError extends Data.TaggedError("TimeoutError")<{
  readonly message: string;
  readonly command?: string;
  readonly key?: string;
}> {}

export type RedisError = ConnectionError | CommandError | DecodeError | TimeoutError;
