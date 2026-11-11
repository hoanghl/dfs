/**
 * Master role: announces itself to DNS and records every node that notifies it.
 *
 * Unlike the Rust Master, the role byte of a `Notify` is trusted: a peer Master
 * is recorded as a master instead of being filed as Data. The IP always comes
 * from the socket peer and the port from the payload, so the ephemeral TCP port
 * never reaches membership.
 */

import type { Cluster } from "../cluster.js";
import type { RoleRuntime } from "../node.js";
import { encode } from "../protocol/codec.js";
import { createNotify, PacketId } from "../protocol/packets.js";
import type { NodeAddress } from "../types.js";

/** Build the Master role around a membership store. */
export function createMasterRole(cluster: Cluster): RoleRuntime {
  return {
    // Advertise the bound port so a config of `0` still announces the real one.
    async start({ config, port, transport, logger }) {
      await transport.send(config.dns, encode(createNotify("master", port)));
      logger.info("master notified dns", { dns: config.dns, port });
    },

    handle(packet, peer, { logger }) {
      if (packet.id !== PacketId.Notify) return;

      if (packet.role !== "master" && packet.role !== "data") {
        logger.warn("master ignored notify", { role: packet.role, peer });
        return;
      }

      // IP from the socket, port from the payload; the key is that pair.
      const address: NodeAddress = { ip: peer.ip, port: packet.port };
      const member = cluster.upsert(address, packet.role);
      logger.info("master recorded member", {
        address: member.address,
        role: member.role,
      });
    },
  };
}
