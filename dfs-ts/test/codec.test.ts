import { describe, expect, test } from "bun:test";

import {
  decode,
  encode,
  FRAME_HEADER_SIZE,
  type ProtocolError,
  type ProtocolErrorCode,
  type Result,
} from "../src/protocol/codec.ts";
import {
  createAskIp,
  createAskIpAck,
  createNotify,
  PacketId,
  type Packet,
} from "../src/protocol/packets.ts";
import { writeU32BE } from "../src/protocol/bytes.ts";
import type { NodeAddress } from "../src/types.ts";

/** Golden bytes below are captured from the Rust `Packet::to_bytes`. */

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"))
    .join(" ")
    .toUpperCase();
}

function fromHex(hex: string): Uint8Array {
  return Uint8Array.from(hex.trim().split(/\s+/), (byte) =>
    Number.parseInt(byte, 16),
  );
}

function frameOf(packetId: number, payload: readonly number[]): Uint8Array {
  const frame = new Uint8Array(FRAME_HEADER_SIZE + payload.length);
  frame[0] = packetId;
  writeU32BE(frame, 1, payload.length);
  frame.set(payload, FRAME_HEADER_SIZE);
  return frame;
}

function expectOk(result: Result<Packet, ProtocolError>): Packet {
  if (!result.ok) {
    throw new Error(
      `expected a decoded packet, got ${result.error.code}: ${result.error.message}`,
    );
  }
  return result.value;
}

function expectError(
  frame: Uint8Array,
  code: ProtocolErrorCode,
): ProtocolError {
  const result = decode(frame);
  if (result.ok) {
    throw new Error(`expected ${code}, got packet ${JSON.stringify(result.value)}`);
  }
  expect(result.error.code).toBe(code);
  return result.error;
}

/** Encode must match the golden bytes; decode must return the same packet. */
function expectGolden(packet: Packet, hex: string): void {
  const bytes = fromHex(hex);
  expect(toHex(encode(packet))).toBe(hex);
  expect(decode(bytes)).toEqual({ ok: true, value: packet });
  expect(toHex(encode(expectOk(decode(bytes))))).toBe(hex);
}

describe("C1 AskIp golden bytes", () => {
  test("port 9000", () => {
    expectGolden(createAskIp(9000), "06 00 00 00 02 23 28");
  });

  test("port 1 proves big-endian order", () => {
    expectGolden(createAskIp(1), "06 00 00 00 02 00 01");
  });
});

describe("C2 AskIpAck golden bytes", () => {
  test("master unknown, empty payload", () => {
    expectGolden(createAskIpAck(null), "07 00 00 00 00");
  });

  test("master 192.168.0.10:7888", () => {
    expectGolden(
      createAskIpAck({ ip: "192.168.0.10", port: 7888 }),
      "07 00 00 00 06 C0 A8 00 0A 1E D0",
    );
  });
});

describe("C3 Notify golden bytes", () => {
  test("master on 7888", () => {
    expectGolden(createNotify("master", 7888), "0F 00 00 00 03 01 1E D0");
  });

  test("data on 9000", () => {
    expectGolden(createNotify("data", 9000), "0F 00 00 00 03 02 23 28");
  });
});

describe("C4 encode/decode fidelity", () => {
  const packets: Packet[] = [
    createAskIp(1),
    createAskIp(65535),
    createAskIp(0),
    createAskIpAck(null),
    createAskIpAck({ ip: "0.0.0.0", port: 1 }),
    createAskIpAck({ ip: "255.255.255.255", port: 65535 }),
    createAskIpAck({ ip: "10.20.30.40", port: 8080 }),
    createNotify("data", 1),
    createNotify("master", 65535),
    createNotify("dns", 0),
    createNotify("client", 4242),
  ];

  test("bytes in, bytes out", () => {
    for (const packet of packets) {
      const bytes = encode(packet);
      expect(decode(bytes)).toEqual({ ok: true, value: packet });
      expect(encode(expectOk(decode(bytes)))).toEqual(bytes);
    }
  });

  test("decoding does not mutate the frame", () => {
    const bytes = encode(createNotify("data", 9000));
    const before = Uint8Array.from(bytes);
    decode(bytes);
    expect(bytes).toEqual(before);
  });
});

describe("C5 truncated header", () => {
  test("fewer than five bytes", () => {
    for (let length = 0; length < FRAME_HEADER_SIZE; length += 1) {
      expectError(new Uint8Array(length), "truncated_header");
    }
  });

  test("partial header still reports truncation, not size", () => {
    const error = expectError(fromHex("06 00 00 00"), "truncated_header");
    expect(error.actualPayloadSize).toBe(0);
  });
});

describe("C6 payload size mismatch", () => {
  test("announced size exceeds the bytes received", () => {
    expectError(fromHex("06 00 00 00 02 23"), "size_mismatch");
  });

  test("trailing bytes after the payload", () => {
    expectError(fromHex("06 00 00 00 02 23 28 00"), "size_mismatch");
  });

  test("larger announced size", () => {
    expectError(fromHex("06 00 01 00 00"), "size_mismatch");
  });
});

describe("C7 per-packet payload size", () => {
  const cases: { id: number; sizes: number[] }[] = [
    { id: PacketId.AskIp, sizes: [0, 1, 3] },
    { id: PacketId.AskIpAck, sizes: [1, 5, 7] },
    { id: PacketId.Notify, sizes: [2, 4] },
  ];

  test("wrong payload size is rejected per packet", () => {
    for (const { id, sizes } of cases) {
      for (const size of sizes) {
        const error = expectError(
          frameOf(id, new Array<number>(size).fill(0)),
          "invalid_payload_size",
        );
        expect(error.packetId).toBe(id);
        expect(error.actualPayloadSize).toBe(size);
      }
    }
  });
});

describe("C8 unknown role byte", () => {
  test("Notify with a role outside the table", () => {
    for (const roleByte of [0, 5, 255]) {
      const error = expectError(
        frameOf(PacketId.Notify, [roleByte, 0x1e, 0xd0]),
        "unknown_role",
      );
      expect(error.value).toBe(roleByte);
    }
  });
});

describe("C9 unknown packet id", () => {
  test("ids this milestone does not handle", () => {
    for (const packetId of [0, 18, 255]) {
      const error = expectError(
        frameOf(packetId, []),
        "unknown_packet",
      );
      expect(error.packetId).toBe(packetId);
    }
  });
});

describe("address value objects", () => {
  test("AskIpAck keeps the port and IP verbatim", () => {
    const master: NodeAddress = { ip: "172.16.254.3", port: 65534 };
    expect(expectOk(decode(encode(createAskIpAck(master))))).toEqual({
      id: PacketId.AskIpAck,
      master,
    });
  });
});
