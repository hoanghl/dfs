import { describe, expect, test } from "bun:test";

import { Cluster } from "../src/core/cluster.ts";
import { DataProcessor, DEFAULT_RETRY_MS } from "../src/core/data.ts";
import { DnsProcessor } from "../src/core/dns.ts";
import { MasterProcessor } from "../src/core/master.ts";
import type { Processor } from "../src/core/processor.ts";
import { NodeDriver } from "../src/driver/driver.ts";
import type { NodeAddress } from "../src/types.ts";
import { createManualClock, createMemoryNetwork, createRecordingLogger, settle } from "./memory-platform.ts";

const DNS: NodeAddress = { ip: "10.0.0.1", port: 53 };
const MASTER: NodeAddress = { ip: "10.0.0.4", port: 7000 };
const DATA: NodeAddress = { ip: "10.0.0.5", port: 7001 };

/** DNS, Master and Data on one memory network; each node has its own clock. */
function setup() {
  const network = createMemoryNetwork();
  const dnsCluster = new Cluster();
  const masterCluster = new Cluster();
  const dataClock = createManualClock();
  const node = (processor: Processor, address: NodeAddress, clock = createManualClock()) =>
    new NodeDriver(processor, network.createTransport(address.ip), clock, createRecordingLogger());
  return {
    dnsCluster,
    masterCluster,
    dataClock,
    dns: node(new DnsProcessor(dnsCluster), DNS),
    master: node(new MasterProcessor(DNS, masterCluster), MASTER),
    data: node(new DataProcessor(DNS), DATA, dataClock),
  };
}

describe("Data joins the cluster", () => {
  test("DNS, Master and Data initialize (F3)", async () => {
    const { dns, master, data, dnsCluster, masterCluster } = setup();

    await dns.start(DNS.port);
    await master.start(MASTER.port);
    await settle();
    await data.start(DATA.port);
    await settle();

    expect(dnsCluster.list().map(({ address, role }) => ({ address, role }))).toEqual([
      { address: MASTER, role: "master" },
    ]);
    expect(masterCluster.list().map(({ address, role }) => ({ address, role }))).toEqual([
      { address: DATA, role: "data" },
    ]);
    await Promise.all([dns.stop(), master.stop(), data.stop()]);
  });

  test("Data started before the Master retries on the clock, then joins (F7)", async () => {
    const { dns, master, data, dataClock, masterCluster } = setup();

    await dns.start(DNS.port);
    await data.start(DATA.port);
    await settle();
    // DNS has no master yet: the ack was empty and nothing joined.
    expect(masterCluster.list()).toEqual([]);

    // The retry finds nothing again, so Data keeps its schedule.
    dataClock.advance(DEFAULT_RETRY_MS);
    await settle();
    expect(masterCluster.list()).toEqual([]);

    await master.start(MASTER.port);
    await settle();
    dataClock.advance(DEFAULT_RETRY_MS);
    await settle();

    expect(masterCluster.list().map(({ address, role }) => ({ address, role }))).toEqual([
      { address: DATA, role: "data" },
    ]);
    // Joined: no further retry is pending.
    dataClock.advance(10 * DEFAULT_RETRY_MS);
    await settle();
    expect(masterCluster.list()).toHaveLength(1);
    await Promise.all([dns.stop(), master.stop(), data.stop()]);
  });
});
