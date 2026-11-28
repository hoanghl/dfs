/**
 * Master processor: announces itself to DNS and records every node that
 * notifies it.
 *
 * Unlike the Rust Master, the role byte of a `Notify` is trusted: a peer Master
 * is recorded as a master instead of being filed as Data. The IP always comes
 * from the socket peer and the port from the payload, so the ephemeral TCP port
 * never reaches membership.
 */

import { decode, encode } from "../protocol/codec.js";
import { createNotify, PacketId } from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";
import type { Cluster } from "./cluster.js";
import type { Input, Output, Processor } from "./processor.js";

/** The Master role: joins through DNS on start, then records every `Notify` sender. */
export class MasterProcessor implements Processor {
  /**
   * @param dns Where the startup `Notify` goes.
   * @param cluster Membership store the notifying nodes are recorded in.
   */
  constructor(
    private readonly dns: NodeAddress,
    private readonly cluster: Cluster,
  ) {}

  /** React to one input: notify DNS on `started`, record members from `Notify` frames. */
  handle(input: Input): Output[] {
    if (input.kind === "started") {
      // Advertise the bound port so a config of `0` still announces the real one.
      return [
        { kind: "send", to: this.dns, bytes: encode(createNotify("master", input.port)) },
        {
          kind: "log",
          level: "info",
          message: "master notified dns",
          meta: { dns: this.dns, port: input.port },
        },
      ];
    }
    if (input.kind !== "frame") return [];

    const result = decode(input.bytes);
    if (!result.ok) {
      return [
        {
          kind: "log",
          level: "warn",
          message: "dropped undecodable frame",
          meta: { code: result.error.code, message: result.error.message, peer: input.peer },
        },
      ];
    }

    const packet = result.value;
    if (packet.id !== PacketId.Notify) return [];

    if (packet.role !== "master" && packet.role !== "data") {
      return [
        {
          kind: "log",
          level: "warn",
          message: "master ignored notify",
          meta: { role: packet.role, peer: input.peer },
        },
      ];
    }

    // IP from the socket, port from the payload; the key is that pair.
    const address: NodeAddress = { ip: input.peer.ip, port: packet.port };
    const member = this.cluster.upsert(address, packet.role, input.now);
    return [
      {
        kind: "log",
        level: "info",
        message: "master recorded member",
        meta: { address: member.address, role: member.role },
      },
    ];
  }

  /** The Master has no timers until the M1.2 heartbeat. */
  nextDeadline(): number | null {
    return null;
  }
}
