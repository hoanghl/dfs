/**
 * Cluster membership: which nodes exist and when each one was last heard from.
 *
 * State only, no I/O: members live in a `Map` keyed `"ip:port"`, the same id Rust
 * builds from `SocketAddrV4`. Expiry is deliberately absent here; M1.2 reads
 * `lastSeen` to mark a node down after two missed heartbeats.
 */

import type { Clock } from "./platform.js";
import type { NodeAddress, Role } from "./types.js";

/** One node the cluster knows about. */
export interface ClusterMember {
  /** Address the node listens on; also the map key. */
  readonly address: NodeAddress;
  readonly role: Role;
  /** `clock.now()` at the last upsert; the heartbeat timer will read it. */
  readonly lastSeen: number;
}

/** Membership set shared by the roles. */
export interface Cluster {
  /**
   * Insert a member, or refresh an existing one, and stamp `lastSeen`.
   *
   * The role replaces the previous one, so a node that re-notifies with a new
   * role ends up with exactly that role.
   */
  upsert(address: NodeAddress, role: Role): ClusterMember;
  /** Member at `address`, or `undefined` when unknown. */
  get(address: NodeAddress): ClusterMember | undefined;
  has(address: NodeAddress): boolean;
  /** Snapshot of every member, in insertion order. Callers filter by role. */
  list(): readonly ClusterMember[];
  /** Drop a member; returns whether it existed. */
  remove(address: NodeAddress): boolean;
}

/** The id Rust calls `node_id`: an address formatted as `"ip:port"`. */
export function memberKey(address: NodeAddress): string {
  return `${address.ip}:${address.port}`;
}

/** Create an empty membership set that reads time from `clock`. */
export function createCluster(clock: Clock): Cluster {
  const members = new Map<string, ClusterMember>();

  return {
    upsert(address, role) {
      const member: ClusterMember = { address, role, lastSeen: clock.now() };
      members.set(memberKey(address), member);
      return member;
    },
    get(address) {
      return members.get(memberKey(address));
    },
    has(address) {
      return members.has(memberKey(address));
    },
    list() {
      return [...members.values()];
    },
    remove(address) {
      return members.delete(memberKey(address));
    },
  };
}
