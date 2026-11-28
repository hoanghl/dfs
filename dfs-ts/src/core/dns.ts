/**
 * DNS processor: the registry of the one current Master.
 *
 * Answers `AskIp` with the master address or an empty ack, and records a
 * member only when the sender notifies as master. Pure and synchronous: each
 * input returns the sends and logs it caused. The cluster is injected so tests
 * can inspect it and M2 can serialize it.
 */

import { decode, encode } from "../protocol/codec.js";
import { createAskIpAck, PacketId } from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";
import { memberKey, type Cluster, type ClusterMember } from "./cluster.js";
import type { Input, Output, Processor } from "./processor.js";

/** The DNS role; it has no startup send and no timers. */
export class DnsProcessor implements Processor {
  private currentMaster: ClusterMember | null = null;

  /** @param cluster Membership store; only the current master is written to it. */
  constructor(private readonly cluster: Cluster) {}

  /** React to one input: answer `AskIp`, record a master `Notify`, log the rest. */
  handle(input: Input): Output[] {
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
    if (packet.id === PacketId.AskIp) {
      return [
        {
          kind: "send",
          to: { ip: input.peer.ip, port: packet.port },
          bytes: encode(createAskIpAck(this.currentMaster?.address ?? null)),
        },
      ];
    }

    if (packet.id !== PacketId.Notify) return [];

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

  /** DNS has no timers. */
  nextDeadline(): number | null {
    return null;
  }
}
