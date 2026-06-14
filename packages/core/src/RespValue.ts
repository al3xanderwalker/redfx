export type RespValue =
  | string
  | number
  | bigint
  | boolean
  | null
  | Uint8Array
  | ReadonlyArray<RespValue>
  | { readonly [key: string]: RespValue };
