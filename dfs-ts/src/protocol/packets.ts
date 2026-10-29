/**
 * Packets exchanged during initialization, plus the wire codes they share.
 *
 * A packet is a plain value: no bytes, no I/O. Only `codec.ts` knows the frame
 * layout, and only the platform adapters see sockets.
 */

import type { NodeAddress, Role } from "../types.js";

/**
 * Packet ids from the Rust `PacketId`. Only the M1.1 packets are listed; the
 * remaining ids arrive with the milestone that needs them.
 */
export const PacketId = {
  AskIp: 6,
  AskIpAck: 7,
  Notify: 15,
} as const;

export type PacketId = (typeof PacketId)[keyof typeof PacketId];

/**
 * Wire code for every role, from the Rust `Role` enum.
 *
 * `default` (0) is not a node role: it is rejected wherever a role is read.
 */
export const RoleCode = {
  Default: 0,
  Master: 1,
  Data: 2,
  Dns: 3,
  Client: 4,
} as const;

/** Data or Client → DNS: "which node is the current master?". */
export interface AskIpPacket {
  readonly id: typeof PacketId.AskIp;
  /** Port the sender listens on for replies; the IP comes from the socket. */
  readonly port: number;
}

/**
 * DNS → requester: the current master, or `null` while there is none.
 *
 * The Rust decoder turns an empty payload into an error, so the Data node never
 * sees it and never recovers; here the empty payload is a valid value.
 */
export interface AskIpAckPacket {
  readonly id: typeof PacketId.AskIpAck;
  readonly master: NodeAddress | null;
}

/** Master → DNS, Master → Master, Data → Master: "I am here, at this port". */
export interface NotifyPacket {
  readonly id: typeof PacketId.Notify;
  readonly role: Role;
  /** Port the sender listens on; the IP always comes from the socket peer. */
  readonly port: number;
}

/** Every packet this milestone can encode and decode. */
export type Packet = AskIpPacket | AskIpAckPacket | NotifyPacket;

/** Build an `AskIp` packet. */
export function createAskIp(port: number): AskIpPacket {
  return { id: PacketId.AskIp, port };
}

/** Build an `AskIpAck` packet; `null` means the DNS has no master yet. */
export function createAskIpAck(master: NodeAddress | null): AskIpAckPacket {
  return { id: PacketId.AskIpAck, master };
}

/** Build a `Notify` packet. */
export function createNotify(role: Role, port: number): NotifyPacket {
  return { id: PacketId.Notify, role, port };
}

/** Encode a role for the wire. Throws only on a role that has no code. */
export function roleToCode(role: Role): number {
  switch (role) {
    case "master":
      return RoleCode.Master;
    case "data":
      return RoleCode.Data;
    case "dns":
      return RoleCode.Dns;
    case "client":
      return RoleCode.Client;
  }
}

/** Decode a role byte; `null` for anything that is not a node role. */
export function roleFromCode(code: number): Role | null {
  switch (code) {
    case RoleCode.Master:
      return "master";
    case RoleCode.Data:
      return "data";
    case RoleCode.Dns:
      return "dns";
    case RoleCode.Client:
      return "client";
    default:
      return null;
  }
}
