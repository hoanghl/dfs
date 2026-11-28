/**
 * DNS processor: the registry of the one current Master.
 *
 * Answers `AskIp` with the master address or an empty ack, and records a
 * member only when the sender notifies as master. Pure and synchronous: each
 * input returns the sends and logs it caused. The cluster is injected so tests
 * can inspect it and M2 can serialize it.
 */

import { encode } from "../protocol/codec.js";
import {
  createAskIpAck,
  PacketId,
  type AskIpPacket,
  type NotifyPacket,
  type Packet,
} from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";
import { memberKey, type Cluster, type ClusterMember } from "./cluster.js";
import { onFrame, type FrameInput } from "./frame.js";
import type { Input, Output, Processor } from "./processor.js";

/** The DNS role; it has no startup send and no timers. */
export class DnsProcessor implements Processor {
  private currentMaster: ClusterMember | null = null;

  /** @param cluster Membership store; only the current master is written to it. */
  constructor(private readonly cluster: Cluster) {}

  /** React to one input: answer `AskIp`, record a master `Notify`, log the rest. */
  handle(input: Input): Output[] {
    if (input.kind !== "frame") return [];
    return onFrame(input, (packet) => this.onPacket(packet, input));
  }

  /** DNS has no timers. */
  nextDeadline(): number | null {
    return null;
  }

  /** Route one decoded packet; packets DNS does not own are ignored. */
  private onPacket(packet: Packet, input: FrameInput): Output[] {
    switch (packet.id) {
      case PacketId.AskIp:
        return this.onAskIp(packet, input);
      case PacketId.Notify:
        return this.onNotify(packet, input);
      default:
        return [];
    }
  }

  /** Reply to the advertised port, not the socket's, with the master or an empty ack. */
  private onAskIp(packet: AskIpPacket, input: FrameInput): Output[] {
    return [
      {
        kind: "send",
        to: { ip: input.peer.ip, port: packet.port },
        bytes: encode(createAskIpAck(this.currentMaster?.address ?? null)),
      },
    ];
  }

  /** Record a master; any other role is logged and ignored. */
  private onNotify(packet: NotifyPacket, input: FrameInput): Output[] {
    if (packet.role !== "master") {
      return [
        {
          kind: "log",
          level: "debug",
          message: "dns ignored notify",
          meta: { role: packet.role, peer: input.peer },
        },
      ];
    }

    // IP from the socket, port from the payload; only one master is kept.
    const address: NodeAddress = { ip: input.peer.ip, port: packet.port };
    if (
      this.currentMaster !== null &&
      memberKey(this.currentMaster.address) !== memberKey(address)
    ) {
      this.cluster.remove(this.currentMaster.address);
    }
    this.currentMaster = this.cluster.upsert(address, "master", input.now);
    return [];
  }
}
