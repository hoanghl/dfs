/**
 * In-memory platform for tests: fake network, manual clock, blob store and a
 * recording logger. Test-only; nothing under `src/` imports this.
 */

import type { BlobStore, Clock, Interval, Logger, Server, Transport } from "../src/platform.js";
import type { NodeAddress } from "../src/types.js";

export interface MemoryNetwork {
  createTransport(ip: string): Transport;
}

export function createMemoryNetwork(firstEphemeralPort = 41_000): MemoryNetwork {
  const listeners = new Map<number, (frame: Uint8Array, peer: NodeAddress) => void>();
  let nextPort = firstEphemeralPort;

  function allocate(): number {
    while (listeners.has(nextPort)) nextPort += 1;
    return nextPort++;
  }

  return {
    createTransport(ip) {
      let local: NodeAddress | null = null;
      return {
        async listen(port, onFrame): Promise<Server> {
          const bound = port === 0 ? allocate() : port;
          if (listeners.has(bound)) throw new Error(`port ${bound} is in use`);
          listeners.set(bound, onFrame);
          local = { ip, port: bound };
          return { port: bound, close: async () => void listeners.delete(bound) };
        },
        async send(to, frame) {
          if (local === null) throw new Error("send before listen");
          const onFrame = listeners.get(to.port);
          if (onFrame === undefined) return; // peer gone: drop like a closed socket
          queueMicrotask(() => onFrame(frame, local as NodeAddress));
        },
      };
    },
  };
}

/** Let queued deliveries and handler chains run; one macrotask flushes them. */
export function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

export interface ManualClock extends Clock {
  advance(ms: number): void;
}

interface Timer {
  due: number;
  readonly intervalMs: number;
  readonly tick: () => void;
}

export function createManualClock(start = 1_000): ManualClock {
  let now = start;
  const timers = new Set<Timer>();

  return {
    now: () => now,
    every(intervalMs, tick): Interval {
      if (intervalMs <= 0) throw new RangeError("interval must be positive");
      const timer: Timer = { due: now + intervalMs, intervalMs, tick };
      timers.add(timer);
      return { cancel: () => void timers.delete(timer) };
    },
    advance(ms) {
      const target = now + ms;
      for (;;) {
        let next: Timer | undefined;
        for (const timer of timers) {
          if (timer.due <= target && (next === undefined || timer.due < next.due)) next = timer;
        }
        if (next === undefined) break;
        now = next.due;
        next.due += next.intervalMs;
        next.tick();
      }
      now = target;
    },
  };
}

export function createMemoryBlobStore(): BlobStore {
  const blobs = new Map<string, Uint8Array>();
  return {
    async write(name, data) {
      blobs.set(name, data.slice());
    },
    async read(name) {
      const data = blobs.get(name);
      if (data === undefined) throw new Error(`no blob named ${name}`);
      return data.slice();
    },
    async has(name) {
      return blobs.has(name);
    },
  };
}

export interface LogEntry {
  readonly level: "debug" | "info" | "warn" | "error";
  readonly message: string;
  readonly meta?: Record<string, unknown>;
}

export interface RecordingLogger extends Logger {
  readonly entries: readonly LogEntry[];
}

export function createRecordingLogger(): RecordingLogger {
  const entries: LogEntry[] = [];
  const record = (level: LogEntry["level"]) => (message: string, meta?: Record<string, unknown>) =>
    void entries.push({ level, message, meta });
  return {
    entries,
    debug: record("debug"),
    info: record("info"),
    warn: record("warn"),
    error: record("error"),
  };
}
