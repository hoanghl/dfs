/**
 * The frame loop every role shares: listen, decode, queue, dispatch.
 *
 * One frame at a time through a promise chain; decode and handler failures are
 * logged, never thrown, so the node keeps listening.
 */

import type { Clock, Logger, Server, Transport } from "./platform.js";
import { decode } from "./protocol/codec.js";
import type { Packet } from "./protocol/packets.js";
import type { NodeAddress, NodeConfig } from "./types.js";

/** I/O the node reaches outside through; tests pass the memory versions. */
export interface NodeDependencies {
  readonly transport: Transport;
  readonly clock: Clock;
  readonly logger: Logger;
}

/** Read-only view a handler gets: enough to reply, schedule or log. */
export interface NodeContext {
  readonly config: NodeConfig;
  /** Port actually bound; differs from `config.listen.port` when that was `0`. */
  readonly port: number;
  readonly transport: Transport;
  readonly clock: Clock;
  readonly logger: Logger;
}

/** Handle one decoded packet; a rejection is logged and the queue moves on. */
export type PacketHandler = (
  packet: Packet,
  peer: NodeAddress,
  context: NodeContext,
) => void | Promise<void>;

export interface Node {
  /** Port actually bound. */
  readonly port: number;
  /** Resolves once every frame queued so far has been handled. */
  idle(): Promise<void>;
  /** Stop listening, then let in-flight frames finish. Safe to call twice. */
  stop(): Promise<void>;
}

/** Start listening and dispatch frames to `onPacket`, one at a time. */
export async function createNode(
  config: NodeConfig,
  { transport, clock, logger }: NodeDependencies,
  onPacket: PacketHandler,
): Promise<Node> {
  let tail: Promise<void> = Promise.resolve();
  let stopping = false;

  const server: Server = await transport.listen(config.listen.port, (frame, peer) => {
    if (stopping) return;
    tail = tail.then(() => dispatch(frame, peer));
  });
  const context: NodeContext = { config, port: server.port, transport, clock, logger };

  async function dispatch(frame: Uint8Array, peer: NodeAddress): Promise<void> {
    const result = decode(frame);
    if (!result.ok) {
      logger.warn("dropped undecodable frame", {
        code: result.error.code,
        message: result.error.message,
        peer,
      });
      return;
    }
    try {
      await onPacket(result.value, peer, context);
    } catch (error) {
      logger.error("packet handler failed", {
        packetId: result.value.id,
        peer,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    port: server.port,
    idle: () => tail,
    async stop() {
      if (stopping) return;
      stopping = true;
      await server.close();
      await tail;
    },
  };
}
