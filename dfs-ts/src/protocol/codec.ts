/**
 * Frame encoding and decoding.
 *
 * Frame = `packetId (u8) + payloadSize (u32 BE) + payload`, header 5 bytes,
 * `payloadSize` counted in bytes only. Decoding is total: every malformed input
 * comes back as a `ProtocolError` value, never as a throw, so the node loop can
 * log it and keep listening.
 */

import {
  readIpv4,
  readU16BE,
  readU32BE,
  writeIpv4,
  writeU16BE,
  writeU32BE,
  writeU8,
} from "./bytes.js";
import {
  createAskIp,
  createAskIpAck,
  createNotify,
  PacketId,
  roleFromCode,
  roleToCode,
  type Packet,
} from "./packets.js";

/** Bytes before the payload: `packetId` + `payloadSize`. */
export const FRAME_HEADER_SIZE = 5;

/** Minimal result type, so protocol failures stay values instead of throws. */
export type Result<T, E> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: E };

export type ProtocolErrorCode =
  /** Fewer than 5 bytes: not even a full header. */
  | "truncated_header"
  /** Declared payload size does not match the bytes actually received. */
  | "size_mismatch"
  /** Known packet id, but its payload length is one the packet forbids. */
  | "invalid_payload_size"
  /** `Notify` carried a role byte outside the role-code table. */
  | "unknown_role"
  /** Packet id this milestone does not handle (including `Default`, 0). */
  | "unknown_packet";

export interface ProtocolError {
  readonly code: ProtocolErrorCode;
  readonly message: string;
  /** Id read from the frame, when the header was intact. */
  readonly packetId?: number;
  /** Payload sizes the packet accepts, when the failure is about length. */
  readonly expectedPayloadSize?: number | readonly number[];
  readonly actualPayloadSize?: number;
  /** Offending byte, for `unknown_role` and `unknown_packet`. */
  readonly value?: number;
}

function ok<T>(value: T): Result<T, ProtocolError> {
  return { ok: true, value };
}

function fail(error: ProtocolError): Result<never, ProtocolError> {
  return { ok: false, error };
}

/**
 * Encode one packet into a complete frame.
 *
 * Throws `RangeError` only when a value cannot be put on the wire (port out of
 * `u16` range, malformed IPv4): that is a caller bug, not a decode failure.
 */
export function encode(packet: Packet): Uint8Array {
  const payload = encodePayload(packet);
  const frame = new Uint8Array(FRAME_HEADER_SIZE + payload.length);
  writeU8(frame, 0, packet.id);
  writeU32BE(frame, 1, payload.length);
  frame.set(payload, FRAME_HEADER_SIZE);
  return frame;
}

function encodePayload(packet: Packet): Uint8Array {
  switch (packet.id) {
    case PacketId.AskIp: {
      const payload = new Uint8Array(2);
      writeU16BE(payload, 0, packet.port);
      return payload;
    }
    case PacketId.AskIpAck: {
      if (packet.master === null) {
        return new Uint8Array(0);
      }
      const payload = new Uint8Array(6);
      writeIpv4(payload, 0, packet.master.ip);
      writeU16BE(payload, 4, packet.master.port);
      return payload;
    }
    case PacketId.Notify: {
      const payload = new Uint8Array(3);
      writeU8(payload, 0, roleToCode(packet.role));
      writeU16BE(payload, 1, packet.port);
      return payload;
    }
  }
}

/**
 * Decode one complete frame.
 *
 * The frame must hold exactly the declared payload: the Rust parser rejects
 * both short and trailing bytes, and so does this one.
 */
export function decode(frame: Uint8Array): Result<Packet, ProtocolError> {
  if (frame.length < FRAME_HEADER_SIZE) {
    return fail({
      code: "truncated_header",
      message: `Frame needs a ${FRAME_HEADER_SIZE}-byte header, got ${frame.length}`,
      actualPayloadSize: Math.max(frame.length - FRAME_HEADER_SIZE, 0),
    });
  }

  const packetId = frame[0] as number;
  const payloadSize = readU32BE(frame, 1);
  const actualPayloadSize = frame.length - FRAME_HEADER_SIZE;
  if (payloadSize !== actualPayloadSize) {
    return fail({
      code: "size_mismatch",
      message: `Frame declares ${payloadSize} payload bytes but carries ${actualPayloadSize}`,
      packetId,
      expectedPayloadSize: payloadSize,
      actualPayloadSize,
    });
  }

  switch (packetId) {
    case PacketId.AskIp: {
      if (payloadSize !== 2) {
        return wrongSize(packetId, "AskIp", [2], payloadSize);
      }
      return ok(createAskIp(readU16BE(frame, FRAME_HEADER_SIZE)));
    }

    case PacketId.AskIpAck: {
      if (payloadSize === 0) {
        return ok(createAskIpAck(null));
      }
      if (payloadSize !== 6) {
        return wrongSize(packetId, "AskIpAck", [0, 6], payloadSize);
      }
      return ok(
        createAskIpAck({
          ip: readIpv4(frame, FRAME_HEADER_SIZE),
          port: readU16BE(frame, FRAME_HEADER_SIZE + 4),
        }),
      );
    }

    case PacketId.Notify: {
      if (payloadSize !== 3) {
        return wrongSize(packetId, "Notify", [3], payloadSize);
      }
      const roleByte = frame[FRAME_HEADER_SIZE] as number;
      const role = roleFromCode(roleByte);
      if (role === null) {
        return fail({
          code: "unknown_role",
          message: `Notify carries unknown role byte ${roleByte}`,
          packetId,
          actualPayloadSize: payloadSize,
          value: roleByte,
        });
      }
      return ok(createNotify(role, readU16BE(frame, FRAME_HEADER_SIZE + 1)));
    }

    default:
      return fail({
        code: "unknown_packet",
        message: `Unknown packet id ${packetId}`,
        packetId,
        actualPayloadSize: payloadSize,
        value: packetId,
      });
  }
}

function wrongSize(
  packetId: number,
  name: string,
  expectedPayloadSize: readonly number[],
  actualPayloadSize: number,
): Result<never, ProtocolError> {
  const expected =
    expectedPayloadSize.length === 1
      ? `${expectedPayloadSize[0]}`
      : expectedPayloadSize.join(" or ");
  return fail({
    code: "invalid_payload_size",
    message: `${name} payload must be ${expected} bytes, got ${actualPayloadSize}`,
    packetId,
    expectedPayloadSize,
    actualPayloadSize,
  });
}
