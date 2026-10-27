/**
 * Value objects shared by every role.
 *
 * These are plain data: they never carry packets and never perform I/O.
 */

/** Role a node plays in the cluster. */
export type Role = "dns" | "master" | "data" | "client";

/** Dotted-quad IPv4 address plus TCP port. */
export interface NodeAddress {
  /** Dotted-quad IPv4, e.g. `"192.168.0.133"`. */
  readonly ip: string;
  readonly port: number;
}

/** Configuration a node needs before it can join the cluster. */
export interface NodeConfig {
  readonly role: Role;
  /** Address this node listens on for inbound frames. */
  readonly listen: NodeAddress;
  /** DNS node used during initialization. */
  readonly dns: NodeAddress;
}
