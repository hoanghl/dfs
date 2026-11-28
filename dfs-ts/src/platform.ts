/**
 * The only way core reaches outside itself.
 *
 * Interfaces carry bytes and plain values, never packets: protocol knowledge
 * stays inside core. Adapters fail by rejecting; core decides what to do.
 */

import type { NodeAddress } from "./types.js";

/** Handle for an open inbound transport, returned by {@link Transport.listen}. */
export interface Server {
  /** Port actually bound; may differ from the requested port when using `0`. */
  readonly port: number;
  close(): Promise<void>;
}

/**
 * One TCP connection per frame: read to EOF, then report the peer.
 *
 * Implementations report the peer's IPv4 without the `::ffff:` prefix.
 *
 * `onFrame` must never be called synchronously from inside `send`: the driver
 * handles one input at a time and does not queue a call that arrives while it
 * is already handling one. Deliver on a later tick, as a real socket does.
 */
export interface Transport {
  listen(port: number, onFrame: (frame: Uint8Array, peer: NodeAddress) => void): Promise<Server>;
  send(to: NodeAddress, frame: Uint8Array): Promise<void>;
}

/** File storage on a Data node. Names are opaque strings chosen by the catalog. */
export interface BlobStore {
  write(name: string, data: Uint8Array): Promise<void>;
  read(name: string): Promise<Uint8Array>;
  has(name: string): Promise<boolean>;
}

/** Cancellation handle returned by {@link Clock.every}. */
export interface Interval {
  cancel(): void;
}

/** Cancellation handle returned by {@link Clock.after}. */
export interface Timer {
  cancel(): void;
}

/** Injected time, so timers and timeouts are testable. */
export interface Clock {
  /** Milliseconds since the epoch. */
  now(): number;
  /** Run `fn` once after `ms`; call `cancel()` to stop it. */
  after(ms: number, fn: () => void): Timer;
  /** Run `tick` every `intervalMs`; call `cancel()` to stop. */
  every(intervalMs: number, tick: () => void): Interval;
}

/** Structured logging; adapters decide where the lines go. */
export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}
