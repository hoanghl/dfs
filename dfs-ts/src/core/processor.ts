/**
 * The contract between a role and the driver that runs it.
 *
 * A processor is pure and synchronous: it receives one {@link Input}, returns
 * the {@link Output}s it wants carried out, and touches no socket, timer or
 * promise. Time arrives in each input as `now`. Nothing here imports `driver/`.
 */

import type { NodeAddress } from "../types.js";

/** Something that happened to the node; `now` is epoch milliseconds. */
export type Input =
  /** The listener is bound; fed once, before any `frame`. */
  | { readonly kind: "started"; readonly port: number; readonly now: number }
  | { readonly kind: "frame"; readonly bytes: Uint8Array; readonly peer: NodeAddress; readonly now: number }
  /** The deadline from {@link Processor.nextDeadline} has passed. */
  | { readonly kind: "timeout"; readonly now: number }
  /** A `send` output was rejected by the transport. */
  | { readonly kind: "sendFailed"; readonly to: NodeAddress; readonly now: number };

/** Something the processor asks the driver to do. */
export type Output =
  | { readonly kind: "send"; readonly to: NodeAddress; readonly bytes: Uint8Array }
  | {
      readonly kind: "log";
      readonly level: "debug" | "info" | "warn" | "error";
      readonly message: string;
      readonly meta?: Record<string, unknown>;
    };

export interface Processor {
  /** Never throws and never awaits: bad input becomes a `log` output. */
  handle(input: Input): Output[];
  /** Epoch milliseconds of the next `timeout` the processor wants, or `null` for none. */
  nextDeadline(): number | null;
}
