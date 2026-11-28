/**
 * Data processor: finds the Master through DNS and notifies it.
 *
 * Unlike the Rust Data node, which gives up for good when DNS has no Master,
 * an empty `AskIpAck` here schedules another `AskIp`, so a Data node started
 * before the Master keeps asking until one exists. Pure and synchronous: the
 * retry is a deadline the driver turns into a `timeout` input.
 */

import { encode } from "../protocol/codec.js";
import {
  createAskIp,
  createNotify,
  PacketId,
  type AskIpAckPacket,
  type Packet,
} from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";
import { onFrame } from "./frame.js";
import type { Input, Output, Processor } from "./processor.js";

/** Pause between two `AskIp` attempts while DNS has no Master, in milliseconds. */
export const DEFAULT_RETRY_MS = 1_000;

/** The Data role: asks DNS for the Master on start, retries until there is one, then notifies it. */
export class DataProcessor implements Processor {
  private port: number | null = null;
  private master: NodeAddress | null = null;
  private retryAt: number | null = null;

  /**
   * @param dns Where `AskIp` goes.
   * @param retryMs Pause before asking again after an empty `AskIpAck`.
   */
  constructor(
    private readonly dns: NodeAddress,
    private readonly retryMs: number = DEFAULT_RETRY_MS,
  ) {}

  /** React to one input: ask DNS on `started` and on a due `timeout`, join on an `AskIpAck`. */
  handle(input: Input): Output[] {
    switch (input.kind) {
      case "started":
        this.port = input.port;
        return this.askIp();
      case "timeout":
        return this.onTimeout(input.now);
      case "frame":
        return onFrame(input, (packet) => this.onPacket(packet, input.now));
      default:
        return [];
    }
  }

  /** The time of the next `AskIp` retry, or `null` when none is pending. */
  nextDeadline(): number | null {
    return this.retryAt;
  }

  /** Ask DNS for the Master, advertising the bound port so DNS replies to the right socket. */
  private askIp(): Output[] {
    if (this.port === null) return [];
    return [
      { kind: "send", to: this.dns, bytes: encode(createAskIp(this.port)) },
      { kind: "log", level: "debug", message: "data asked dns for the master", meta: { dns: this.dns } },
    ];
  }

  /** Retry once the deadline has passed; an early or unexpected `timeout` does nothing. */
  private onTimeout(now: number): Output[] {
    if (this.retryAt === null || now < this.retryAt) return [];
    this.retryAt = null;
    return this.askIp();
  }

  /** Route one decoded packet; packets the Data node does not own are ignored. */
  private onPacket(packet: Packet, now: number): Output[] {
    switch (packet.id) {
      case PacketId.AskIpAck:
        return this.onAskIpAck(packet, now);
      default:
        return [];
    }
  }

  /** Notify the master from the ack, or schedule a retry when the ack is empty. */
  private onAskIpAck(packet: AskIpAckPacket, now: number): Output[] {
    // Already joined: a late or duplicate ack must not reset the join or the retry.
    if (this.master !== null || this.port === null) return [];

    if (packet.master === null) {
      this.retryAt = now + this.retryMs;
      return [
        {
          kind: "log",
          level: "debug",
          message: "data found no master, will retry",
          meta: { retryAt: this.retryAt },
        },
      ];
    }

    this.retryAt = null;
    this.master = packet.master;
    return [
      { kind: "send", to: this.master, bytes: encode(createNotify("data", this.port)) },
      { kind: "log", level: "info", message: "data notified master", meta: { master: this.master } },
    ];
  }
}
