/**
 * Master processor: announces itself to DNS and records every node that
 * notifies it.
 *
 * Unlike the Rust Master, the role byte of a `Notify` is trusted: a peer Master
 * is recorded as a master instead of being filed as Data. The IP always comes
 * from the socket peer and the port from the payload, so the ephemeral TCP port
 * never reaches membership.
 */

import { encode } from "../protocol/codec.js";
import { createNotify, PacketId, type NotifyPacket, type Packet } from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";
import type { Cluster } from "./cluster.js";
import { onFrame, type FrameInput } from "./frame.js";
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
    switch (input.kind) {
      case "started":
        return this.onStarted(input.port);
      case "frame":
        return onFrame(input, (packet) => this.onPacket(packet, input));
      default:
        return [];
    }
  }

  /** The Master has no timers until the M1.2 heartbeat. */
  nextDeadline(): number | null {
    return null;
  }

  /** Advertise the bound port so a config of `0` still announces the real one. */
  private onStarted(port: number): Output[] {
    return [
      { kind: "send", to: this.dns, bytes: encode(createNotify("master", port)) },
      {
        kind: "log",
        level: "info",
        message: "master notified dns",
        meta: { dns: this.dns, port },
      },
    ];
  }

  /** Route one decoded packet; packets the Master does not own are ignored. */
  private onPacket(packet: Packet, input: FrameInput): Output[] {
    switch (packet.id) {
      case PacketId.Notify:
        return this.onNotify(packet, input);
      default:
        return [];
    }
  }

  /** Record a master or data sender; dns and client roles are logged and ignored. */
  private onNotify(packet: NotifyPacket, input: FrameInput): Output[] {
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
}
