/**
 * Public entry point of `dfs-core`.
 *
 * Only this module is exported by the package. It publishes the driver, the
 * three role processors of M1.1 (initialization) and the types they are built
 * on. `createClient` is not here yet: it lands with M1.3, the first
 * sub-milestone with a client packet.
 */

export type { NodeAddress, NodeConfig, Role } from "./types.js";

export { Cluster, memberKey } from "./core/cluster.js";
export type { ClusterMember } from "./core/cluster.js";
export { DataProcessor, DEFAULT_RETRY_MS } from "./core/data.js";
export { DnsProcessor } from "./core/dns.js";
export { MasterProcessor } from "./core/master.js";
export type { Input, Output, Processor } from "./core/processor.js";

export { NodeDriver } from "./driver/driver.js";
export type { BlobStore, Clock, Logger, Server, Timer, Transport } from "./driver/platform.js";
