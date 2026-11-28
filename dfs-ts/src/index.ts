/**
 * Public entry point of `dfs-core`.
 *
 * Only this module is exported by the package. `NodeDriver`, the role
 * processors and `createClient` land with M1.1 (initialization packets); until
 * then the package publishes the shared types and the platform interfaces the
 * driver is built on.
 */

export type { NodeAddress, NodeConfig, Role } from "./types.js";
export type { BlobStore, Clock, Logger, Server, Timer, Transport } from "./driver/platform.js";
