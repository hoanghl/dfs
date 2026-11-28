import { describe, expect, test } from "bun:test";

import { Cluster } from "../src/core/cluster.ts";
import { DnsProcessor } from "../src/core/dns.ts";
import type { Output } from "../src/core/processor.ts";
import { decode, encode } from "../src/protocol/codec.ts";
import {
  createAskIp,
  createAskIpAck,
  createNotify,
  type Packet,
} from "../src/protocol/packets.ts";
import type { NodeAddress } from "../src/types.ts";

const MASTER: NodeAddress = { ip: "10.0.0.4", port: 7000 };
const DATA: NodeAddress = { ip: "10.0.0.5", port: 7001 };
const CLIENT: NodeAddress = { ip: "10.0.0.9", port: 50_123 };
const NOW = 1_000;

function setup() {
  const cluster = new Cluster();
  const dns = new DnsProcessor(cluster);
  const frame = (packet: Packet, peer: NodeAddress, now = NOW) =>
    dns.handle({ kind: "frame", bytes: encode(packet), peer, now });
  return { cluster, dns, frame };
}

/** The packet inside a `send` output, decoded; fails the test for anything else. */
function sentPacket(output: Output | undefined): { to: NodeAddress; packet: Packet } {
  if (output?.kind !== "send") throw new Error("expected a send output");
  const result = decode(output.bytes);
  if (!result.ok) throw new Error("send output does not decode");
  return { to: output.to, packet: result.value };
}

describe("DnsProcessor", () => {
  test("empty askIpAck", () => {
    const { cluster, frame } = setup();

    const outputs = frame(createAskIp(CLIENT.port), CLIENT);

    expect(outputs).toHaveLength(1);
    expect(sentPacket(outputs[0])).toEqual({ to: CLIENT, packet: createAskIpAck(null) });
    expect(cluster.list()).toEqual([]);
  });

  test("dns master registry", () => {
    const { cluster, frame } = setup();

    expect(frame(createNotify("master", MASTER.port), MASTER)).toEqual([]);
    const outputs = frame(createAskIp(CLIENT.port), CLIENT);

    expect(sentPacket(outputs[0]).packet).toEqual(createAskIpAck(MASTER));
    expect(cluster.list()).toEqual([{ address: MASTER, role: "master", lastSeen: NOW }]);
  });

  test("reply routing to advertised port", () => {
    const { frame } = setup();

    const outputs = frame(createAskIp(9999), CLIENT);

    expect(sentPacket(outputs[0]).to).toEqual({ ip: CLIENT.ip, port: 9999 });
  });

  test("dns ignores non-master notify", () => {
    const { cluster, frame } = setup();

    const first = frame(createNotify("data", DATA.port), DATA);
    expect(first).toMatchObject([{ kind: "log", level: "debug", message: "dns ignored notify" }]);
    expect(sentPacket(frame(createAskIp(CLIENT.port), CLIENT)[0]).packet).toEqual(
      createAskIpAck(null),
    );

    frame(createNotify("master", MASTER.port), MASTER);
    frame(createNotify("data", DATA.port), DATA);

    expect(sentPacket(frame(createAskIp(CLIENT.port), CLIENT)[0]).packet).toEqual(
      createAskIpAck(MASTER),
    );
    expect(cluster.list()).toHaveLength(1);
  });

  test("a new master replaces the old one", () => {
    const { cluster, frame } = setup();
    const next: NodeAddress = { ip: "10.0.0.6", port: 7002 };

    frame(createNotify("master", MASTER.port), MASTER);
    frame(createNotify("master", next.port), next, NOW + 10);

    expect(cluster.list()).toEqual([{ address: next, role: "master", lastSeen: NOW + 10 }]);
  });

  test("dns survives malformed frame", () => {
    const { dns, frame } = setup();

    const dropped = dns.handle({ kind: "frame", bytes: Uint8Array.of(6, 0), peer: CLIENT, now: NOW });
    const outputs = frame(createAskIp(CLIENT.port), CLIENT);

    expect(dropped).toMatchObject([
      { kind: "log", level: "warn", message: "dropped undecodable frame" },
    ]);
    expect(sentPacket(outputs[0]).packet).toEqual(createAskIpAck(null));
  });

  test("has no startup send and no deadline", () => {
    const { dns } = setup();

    expect(dns.handle({ kind: "started", port: 53, now: NOW })).toEqual([]);
    expect(dns.nextDeadline()).toBeNull();
  });
});
