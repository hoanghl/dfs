import { describe, expect, test } from "bun:test";

import { createCluster } from "../src/cluster.ts";
import { createNode, type Node } from "../src/node.ts";
import type { Transport } from "../src/platform.ts";
import { decode, encode } from "../src/protocol/codec.ts";
import {
  createAskIp,
  createAskIpAck,
  createNotify,
  type Packet,
} from "../src/protocol/packets.ts";
import { createDnsRole } from "../src/roles/dns.ts";
import type { NodeAddress } from "../src/types.ts";
import {
  createManualClock,
  createMemoryNetwork,
  createRecordingLogger,
  settle,
  type MemoryNetwork,
  type RecordingLogger,
} from "./memory-platform.ts";

const DNS_IP = "10.0.0.1";
const MASTER_IP = "10.0.0.4";
const DATA_IP = "10.0.0.5";
const CLIENT_IP = "10.0.0.9";

interface Endpoint {
  readonly transport: Transport;
  readonly address: NodeAddress;
  readonly inbox: Packet[];
}

interface Dns {
  readonly node: Node;
  readonly cluster: ReturnType<typeof createCluster>;
  readonly logger: RecordingLogger;
  readonly address: NodeAddress;
}

async function endpoint(network: MemoryNetwork, ip: string): Promise<Endpoint> {
  const transport = network.createTransport(ip);
  const inbox: Packet[] = [];
  const server = await transport.listen(0, (frame) => {
    const result = decode(frame);
    if (result.ok) inbox.push(result.value);
  });
  return { transport, address: { ip, port: server.port }, inbox };
}

async function startDns(network: MemoryNetwork): Promise<Dns> {
  const transport = network.createTransport(DNS_IP);
  const clock = createManualClock();
  const logger = createRecordingLogger();
  const cluster = createCluster(clock);
  const node = await createNode(
    { role: "dns", listen: { ip: DNS_IP, port: 0 }, dns: { ip: DNS_IP, port: 0 } },
    { transport, clock, logger },
    createDnsRole(cluster),
  );
  return { node, cluster, logger, address: { ip: DNS_IP, port: node.context.port } };
}

async function pump(dns: Dns): Promise<void> {
  await settle();
  await dns.node.idle();
}

function ask(from: Endpoint, dns: Dns, advertise = from.address.port): Promise<void> {
  return from.transport.send(dns.address, encode(createAskIp(advertise)));
}

function notify(
  from: Endpoint,
  dns: Dns,
  role: "master" | "data",
  advertise = from.address.port,
): Promise<void> {
  return from.transport.send(dns.address, encode(createNotify(role, advertise)));
}

describe("dns role", () => {
  test("empty askIpAck", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const client = await endpoint(network, CLIENT_IP);

    await ask(client, dns);
    await pump(dns);

    expect(client.inbox).toEqual([createAskIpAck(null)]);
    expect(dns.cluster.list()).toEqual([]);
  });

  test("dns master registry", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const master = await endpoint(network, MASTER_IP);
    const client = await endpoint(network, CLIENT_IP);

    await notify(master, dns, "master");
    await pump(dns);
    await ask(client, dns);
    await pump(dns);

    expect(client.inbox).toEqual([
      createAskIpAck({ ip: MASTER_IP, port: master.address.port }),
    ]);
    expect(dns.cluster.list()).toHaveLength(1);
    expect(dns.cluster.list()[0]?.role).toBe("master");
    expect(dns.cluster.list()[0]?.address).toEqual(master.address);
  });

  test("reply routing to advertised port", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const sender = await endpoint(network, CLIENT_IP);
    const advertised = await endpoint(network, CLIENT_IP);

    await ask(sender, dns, advertised.address.port);
    await pump(dns);

    expect(advertised.inbox).toEqual([createAskIpAck(null)]);
    expect(sender.inbox).toEqual([]);
  });

  test("dns ignores non-master notify", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const data = await endpoint(network, DATA_IP);
    const master = await endpoint(network, MASTER_IP);
    const client = await endpoint(network, CLIENT_IP);

    await notify(data, dns, "data");
    await pump(dns);
    await ask(client, dns);
    await pump(dns);
    expect(client.inbox).toEqual([createAskIpAck(null)]);

    await notify(master, dns, "master");
    await pump(dns);
    await notify(data, dns, "data");
    await pump(dns);
    await ask(client, dns);
    await pump(dns);

    expect(client.inbox.at(-1)).toEqual(
      createAskIpAck({ ip: MASTER_IP, port: master.address.port }),
    );
    expect(dns.cluster.list()).toHaveLength(1);
  });

  test("dns survives malformed frame", async () => {
    const network = createMemoryNetwork();
    const dns = await startDns(network);
    const client = await endpoint(network, CLIENT_IP);

    await client.transport.send(dns.address, Uint8Array.of(6, 0));
    await ask(client, dns);
    await pump(dns);

    expect(client.inbox).toEqual([createAskIpAck(null)]);
    expect(
      dns.logger.entries.some(
        (entry) => entry.message === "dropped undecodable frame",
      ),
    ).toBe(true);
  });
});
