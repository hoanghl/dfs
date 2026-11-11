import { describe, expect, test } from "bun:test";

import { createCluster, type Cluster } from "../src/cluster.ts";
import { createNode, type Node } from "../src/node.ts";
import type { Transport } from "../src/platform.ts";
import { encode } from "../src/protocol/codec.ts";
import { createNotify } from "../src/protocol/packets.ts";
import { createDnsRole } from "../src/roles/dns.ts";
import { createMasterRole } from "../src/roles/master.ts";
import type { NodeAddress, Role } from "../src/types.ts";
import {
  createManualClock,
  createMemoryNetwork,
  createRecordingLogger,
  settle,
  type ManualClock,
  type MemoryNetwork,
  type RecordingLogger,
} from "./memory-platform.ts";

const DNS_IP = "10.0.0.1";
const MASTER_IP = "10.0.0.4";
const DATA_IP = "10.0.0.5";
const PEER_MASTER_IP = "10.0.0.6";
const STRANGER_IP = "10.0.0.7";
const NO_DNS: NodeAddress = { ip: DNS_IP, port: 0 };

interface Endpoint {
  readonly transport: Transport;
  readonly address: NodeAddress;
}

interface Dns {
  readonly node: Node;
  readonly cluster: Cluster;
}

interface Master {
  readonly node: Node;
  readonly cluster: Cluster;
  readonly logger: RecordingLogger;
  readonly clock: ManualClock;
  readonly address: NodeAddress;
}

async function endpoint(network: MemoryNetwork, ip: string): Promise<Endpoint> {
  const transport = network.createTransport(ip);
  const server = await transport.listen(0, () => {});
  return { transport, address: { ip, port: server.port } };
}

async function startDns(network: MemoryNetwork): Promise<Dns> {
  const transport = network.createTransport(DNS_IP);
  const clock = createManualClock();
  const cluster = createCluster(clock);
  const node = await createNode(
    { role: "dns", listen: { ip: DNS_IP, port: 0 }, dns: { ip: DNS_IP, port: 0 } },
    { transport, clock, logger: createRecordingLogger() },
    createDnsRole(cluster),
  );
  return { node, cluster };
}

async function startMaster(
  network: MemoryNetwork,
  listenPort: number,
  dns: NodeAddress = NO_DNS,
): Promise<Master> {
  const transport = network.createTransport(MASTER_IP);
  const clock = createManualClock();
  const logger = createRecordingLogger();
  const cluster = createCluster(clock);
  const role = createMasterRole(cluster);
  const node = await createNode(
    { role: "master", listen: { ip: MASTER_IP, port: listenPort }, dns },
    { transport, clock, logger },
    role,
  );
  return { node, cluster, logger, clock, address: { ip: MASTER_IP, port: node.context.port } };
}

function notify(
  from: Endpoint,
  to: NodeAddress,
  role: Role,
  advertise = from.address.port,
): Promise<void> {
  return from.transport.send(to, encode(createNotify(role, advertise)));
}

async function pump(node: Node): Promise<void> {
  await settle();
  await node.idle();
}

describe("master role", () => {
  test("master registers with dns on start", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const master = await startMaster(network, 7000, {
      ip: DNS_IP,
      port: dns.node.context.port,
    });
    await pump(dns.node);

    expect(dns.cluster.list()).toHaveLength(1);
    expect(dns.cluster.list()[0]?.address).toEqual({
      ip: MASTER_IP,
      port: 7000,
    });
    expect(dns.cluster.list()[0]?.role).toBe("master");
    expect(master.address.port).toBe(7000);
  });

  test("master records data member", async () => {
    const network = createMemoryNetwork();
    const master = await startMaster(network, 0);
    const data = await endpoint(network, DATA_IP);

    await notify(data, master.address, "data");
    await pump(master.node);

    expect(master.cluster.get(data.address)).toMatchObject({
      address: data.address,
      role: "data",
    });
    expect(master.cluster.list()).toHaveLength(1);
  });

  test("master records peer master and ignores other roles", async () => {
    const network = createMemoryNetwork();
    const master = await startMaster(network, 0);
    const peer = await endpoint(network, PEER_MASTER_IP);
    const stranger = await endpoint(network, STRANGER_IP);

    await notify(peer, master.address, "master");
    await notify(stranger, master.address, "dns");
    await notify(stranger, master.address, "client");
    await pump(master.node);

    // Rust would file the peer master as Data; the role byte wins here.
    expect(master.cluster.list()).toHaveLength(1);
    expect(master.cluster.get(peer.address)?.role).toBe("master");
    expect(
      master.logger.entries.filter(
        (entry) => entry.message === "master ignored notify",
      ),
    ).toHaveLength(2);
  });

  test("uses bound port when listen port is 0", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const master = await startMaster(network, 0, {
      ip: DNS_IP,
      port: dns.node.context.port,
    });
    await pump(dns.node);

    expect(master.node.context.port).not.toBe(0);
    expect(dns.cluster.list()[0]?.address).toEqual({
      ip: MASTER_IP,
      port: master.node.context.port,
    });
  });

  test("re-notify refreshes lastSeen on the same key", async () => {
    const network = createMemoryNetwork();
    const master = await startMaster(network, 0);
    const data = await endpoint(network, DATA_IP);

    await notify(data, master.address, "data");
    await pump(master.node);
    const first = master.cluster.get(data.address);

    master.clock.advance(5000);
    await notify(data, master.address, "data");
    await pump(master.node);
    const again = master.cluster.get(data.address);

    expect(again?.lastSeen).toBe((first?.lastSeen ?? NaN) + 5000);
    expect(master.cluster.list()).toHaveLength(1);
  });
});
