/**
 * Public entry point of `dfs-core`.
 *
 * Only this module is exported by the package. `createNode` and `createClient`
 * land with M1.1 (initialization packets); until then the package publishes the
 * shared types and the platform interfaces they are built on.
 */

export type { NodeAddress, NodeConfig, Role } from "./types.js";
export type { BlobStore, Clock, Interval, Logger, Server, Transport } from "./platform.js";
