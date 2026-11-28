/**
 * Cluster membership: which nodes exist and when each one was last heard from.
 *
 * State only, no I/O: members live in a `Map` keyed `"ip:port"`, the same id Rust
 * builds from `SocketAddrV4`. Expiry is deliberately absent here; M1.2 reads
 * `lastSeen` to mark a node down after two missed heartbeats.
 */

import type { NodeAddress, Role } from "../types.js";

/** One node the cluster knows about. */
export interface ClusterMember {
  /** Address the node listens on; also the map key. */
  readonly address: NodeAddress;
  readonly role: Role;
  /** `now` at the last upsert, in epoch milliseconds; the heartbeat timer will read it. */
  readonly lastSeen: number;
}

/** The id Rust calls `node_id`: an address formatted as `"ip:port"`. */
export function memberKey(address: NodeAddress): string {
  return `${address.ip}:${address.port}`;
}

/**
 * Membership set shared by the roles.
 *
 * It never reads a clock: callers pass `now`, so a processor stays a pure
 * function of its inputs.
 */
export class Cluster {
  private readonly members = new Map<string, ClusterMember>();

  /**
   * Insert a member, or refresh an existing one, and stamp `lastSeen` with `now`.
   *
   * The role replaces the previous one, so a node that re-notifies with a new
   * role ends up with exactly that role.
   */
  upsert(address: NodeAddress, role: Role, now: number): ClusterMember {
    const member: ClusterMember = { address, role, lastSeen: now };
    this.members.set(memberKey(address), member);
    return member;
  }

  /** Member at `address`, or `undefined` when unknown. */
  get(address: NodeAddress): ClusterMember | undefined {
    return this.members.get(memberKey(address));
  }

  has(address: NodeAddress): boolean {
    return this.members.has(memberKey(address));
  }

  /** Snapshot of every member, in insertion order. Callers filter by role. */
  list(): readonly ClusterMember[] {
    return [...this.members.values()];
  }

  /** Drop a member; returns whether it existed. */
  remove(address: NodeAddress): boolean {
    return this.members.delete(memberKey(address));
  }
}
