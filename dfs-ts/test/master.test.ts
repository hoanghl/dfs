import { describe, expect, test } from "bun:test";

import { createCluster } from "../src/core/cluster.ts";
import { DnsProcessor } from "../src/core/dns.ts";
import { MasterProcessor } from "../src/core/master.ts";
import { decode, encode } from "../src/protocol/codec.ts";
import { createAskIp, createAskIpAck, createNotify, type Packet } from "../src/protocol/packets.ts";
import type { NodeAddress } from "../src/types.ts";

const DNS: NodeAddress = { ip: "10.0.0.1", port: 53 };
const MASTER: NodeAddress = { ip: "10.0.0.4", port: 7000 };
const DATA: NodeAddress = { ip: "10.0.0.5", port: 7001 };
const PEER_MASTER: NodeAddress = { ip: "10.0.0.6", port: 7002 };
const STRANGER: NodeAddress = { ip: "10.0.0.7", port: 7003 };
const NOW = 1_000;

function setup() {
  const cluster = createCluster();
  const master = new MasterProcessor(DNS, cluster);
  const frame = (packet: Packet, peer: NodeAddress, now = NOW) =>
    master.handle({ kind: "frame", bytes: encode(packet), peer, now });
  return { cluster, master, frame };
}

describe("MasterProcessor", () => {
  test("master registers with dns on start", () => {
    const { master } = setup();

    const outputs = master.handle({ kind: "started", port: MASTER.port, now: NOW });

    expect(outputs).toHaveLength(2);
    expect(outputs[0]).toEqual({
      kind: "send",
      to: DNS,
      bytes: encode(createNotify("master", MASTER.port)),
    });
    expect(outputs[1]).toMatchObject({
      kind: "log",
      level: "info",
      message: "master notified dns",
    });
  });

  test("the started notify registers the master on a DnsProcessor", () => {
    const { master } = setup();
    const dnsCluster = createCluster();
    const dns = new DnsProcessor(dnsCluster);

    const [notify] = master.handle({ kind: "started", port: MASTER.port, now: NOW });
    if (notify?.kind !== "send") throw new Error("expected a send output");
    const socketPeer: NodeAddress = { ip: MASTER.ip, port: 51_234 };
    dns.handle({ kind: "frame", bytes: notify.bytes, peer: socketPeer, now: NOW });

    // The ephemeral socket port never reaches membership; the payload port does.
    expect(dnsCluster.list()).toEqual([{ address: MASTER, role: "master", lastSeen: NOW }]);
    const [reply] = dns.handle({
      kind: "frame",
      bytes: encode(createAskIp(9999)),
      peer: DATA,
      now: NOW,
    });
    if (reply?.kind !== "send") throw new Error("expected a send output");
    expect(decode(reply.bytes)).toEqual({ ok: true, value: createAskIpAck(MASTER) });
  });

  test("master records data member", () => {
    const { cluster, frame } = setup();

    const outputs = frame(createNotify("data", DATA.port), DATA);

    expect(cluster.get(DATA)).toEqual({ address: DATA, role: "data", lastSeen: NOW });
    expect(cluster.list()).toHaveLength(1);
    expect(outputs).toMatchObject([
      { kind: "log", level: "info", message: "master recorded member" },
    ]);
  });

  test("master records peer master and ignores other roles", () => {
    const { cluster, frame } = setup();

    frame(createNotify("master", PEER_MASTER.port), PEER_MASTER);
    const dnsNotify = frame(createNotify("dns", STRANGER.port), STRANGER);
    const clientNotify = frame(createNotify("client", STRANGER.port), STRANGER);

    // Rust would file the peer master as Data; the role byte wins here.
    expect(cluster.list()).toHaveLength(1);
    expect(cluster.get(PEER_MASTER)?.role).toBe("master");
    for (const outputs of [dnsNotify, clientNotify]) {
      expect(outputs).toMatchObject([
        { kind: "log", level: "warn", message: "master ignored notify" },
      ]);
    }
  });

  test("records the payload port, not the socket port", () => {
    const { cluster, frame } = setup();

    frame(createNotify("data", DATA.port), { ip: DATA.ip, port: 61_000 });

    expect(cluster.list().map((member) => member.address)).toEqual([DATA]);
  });

  test("re-notify refreshes lastSeen on the same key", () => {
    const { cluster, frame } = setup();

    frame(createNotify("data", DATA.port), DATA, NOW);
    frame(createNotify("data", DATA.port), DATA, NOW + 5000);

    expect(cluster.get(DATA)?.lastSeen).toBe(NOW + 5000);
    expect(cluster.list()).toHaveLength(1);
  });

  test("drops an undecodable frame and has no deadline", () => {
    const { master } = setup();

    const outputs = master.handle({
      kind: "frame",
      bytes: Uint8Array.of(15, 0),
      peer: DATA,
      now: NOW,
    });

    expect(outputs).toMatchObject([
      { kind: "log", level: "warn", message: "dropped undecodable frame" },
    ]);
    expect(master.nextDeadline()).toBeNull();
  });
});
