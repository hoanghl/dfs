import { describe, expect, test } from "bun:test";

import { createNode, type Node, type PacketHandler } from "../src/node.ts";
import { encode } from "../src/protocol/codec.ts";
import { createAskIp, createNotify, PacketId, type Packet } from "../src/protocol/packets.ts";
import type { NodeAddress } from "../src/types.ts";
import {
  createManualClock,
  createMemoryNetwork,
  createRecordingLogger,
  settle,
  type MemoryNetwork,
  type RecordingLogger,
} from "./memory-platform.ts";

const DNS: NodeAddress = { ip: "10.0.0.1", port: 7000 };
const NODE_IP = "10.0.0.2";
const PEER_IP = "10.0.0.3";

interface Harness {
  readonly node: Node;
  readonly logger: RecordingLogger;
  readonly peerPort: number;
  send(packet: Packet): Promise<void>;
  sendRaw(frame: Uint8Array): Promise<void>;
}

async function startNode(network: MemoryNetwork, onPacket: PacketHandler): Promise<Harness> {
  const transport = network.createTransport(NODE_IP);
  const clock = createManualClock();
  const logger = createRecordingLogger();
  const node = await createNode(
    { role: "data", listen: { ip: NODE_IP, port: 0 }, dns: DNS },
    { transport, clock, logger },
    onPacket,
  );
  const peer = network.createTransport(PEER_IP);
  const peerServer = await peer.listen(0, () => {});
  const sendRaw = (frame: Uint8Array): Promise<void> =>
    peer.send({ ip: NODE_IP, port: node.port }, frame);
  return {
    node,
    logger,
    peerPort: peerServer.port,
    send: (packet) => sendRaw(encode(packet)),
    sendRaw,
  };
}

/** Let deliveries run, then wait for the handler queue to drain. */
async function pump(harness: Harness): Promise<void> {
  await settle();
  await harness.node.idle();
}

describe("createNode", () => {
  test("frame dispatch", async () => {
    const seen: Array<{ packet: Packet; peer: NodeAddress }> = [];
    const harness = await startNode(createMemoryNetwork(), (packet, peer) => {
      seen.push({ packet, peer });
    });
    await harness.send(createAskIp(7000));
    await pump(harness);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.packet).toEqual(createAskIp(7000));
    expect(seen[0]?.peer).toEqual({ ip: PEER_IP, port: harness.peerPort });
  });

  test("serialized handler queue", async () => {
    const started: number[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = await startNode(createMemoryNetwork(), async (packet) => {
      if (packet.id !== PacketId.AskIp) return;
      started.push(packet.port);
      if (started.length === 1) await gate;
    });
    await harness.send(createAskIp(1));
    await harness.send(createAskIp(2));
    await settle();
    expect(started).toEqual([1]);
    release();
    await harness.node.idle();
    expect(started).toEqual([1, 2]);
  });

  test("handler rejection recovery", async () => {
    const handled: number[] = [];
    const harness = await startNode(createMemoryNetwork(), (packet) => {
      if (packet.id === PacketId.AskIp) throw new Error("boom");
      handled.push(packet.id);
    });
    await harness.send(createAskIp(1));
    await harness.send(createNotify("master", 2));
    await pump(harness);
    expect(handled).toEqual([PacketId.Notify]);
    const errors = harness.logger.entries.filter((entry) => entry.level === "error");
    expect(errors).toHaveLength(1);
    expect(errors[0]?.message).toBe("packet handler failed");
  });

  test("undecodable and unhandled frames", async () => {
    const handled: number[] = [];
    const harness = await startNode(createMemoryNetwork(), (packet) => {
      handled.push(packet.id);
    });
    await harness.sendRaw(Uint8Array.of(6, 0)); // truncated header
    await harness.sendRaw(Uint8Array.of(99, 0, 0, 0, 0)); // unknown packet id
    await harness.send(createAskIp(3));
    await pump(harness);
    expect(handled).toEqual([PacketId.AskIp]);
    const warnings = harness.logger.entries.filter((entry) => entry.level === "warn");
    expect(warnings.map((entry) => entry.meta?.code)).toEqual([
      "truncated_header",
      "unknown_packet",
    ]);
  });

  test("stop closes listener", async () => {
    const handled: number[] = [];
    const harness = await startNode(createMemoryNetwork(), (packet) => {
      handled.push(packet.id);
    });
    await harness.node.stop();
    await harness.node.stop(); // idempotent
    await harness.send(createAskIp(1));
    await settle();
    expect(handled).toEqual([]);
  });
});
