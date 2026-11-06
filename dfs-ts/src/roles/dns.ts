/**
 * DNS role: the registry of the one current Master.
 *
 * Answers `AskIp` with the master address or an empty ack, and records a
 * member only when the sender notifies as master. The cluster is injected so
 * tests can inspect it and M2 can serialize it.
 */

import { memberKey, type Cluster, type ClusterMember } from "../cluster.js";
import type { PacketHandler } from "../node.js";
import { encode } from "../protocol/codec.js";
import { createAskIpAck, PacketId } from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";

/** Build the DNS handler around a membership store. */
export function createDnsRole(cluster: Cluster): PacketHandler {
  let currentMaster: ClusterMember | null = null;

  return async (packet, peer, { transport, logger }) => {
    if (packet.id === PacketId.AskIp) {
      await transport.send(
        { ip: peer.ip, port: packet.port },
        encode(createAskIpAck(currentMaster?.address ?? null)),
      );
      return;
    }

    if (packet.id !== PacketId.Notify) return;

    if (packet.role !== "master") {
      logger.debug("dns ignored notify", { role: packet.role, peer });
      return;
    }

    // IP from the socket, port from the payload; only one master is kept.
    const address: NodeAddress = { ip: peer.ip, port: packet.port };
    if (
      currentMaster !== null &&
      memberKey(currentMaster.address) !== memberKey(address)
    ) {
      cluster.remove(currentMaster.address);
    }
    currentMaster = cluster.upsert(address, "master");
  };
}
