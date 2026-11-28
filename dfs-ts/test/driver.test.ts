import { describe, expect, test } from "bun:test";

import type { Input, Output, Processor } from "../src/core/processor.ts";
import { NodeDriver } from "../src/driver/driver.ts";
import type { Transport } from "../src/platform.ts";
import type { NodeAddress } from "../src/types.ts";
import {
  createManualClock,
  createMemoryNetwork,
  createRecordingLogger,
  settle,
  type ManualClock,
} from "./memory-platform.ts";

const NODE_IP = "10.0.0.2";
const PEER: NodeAddress = { ip: "10.0.0.3", port: 7001 };
const OTHER: NodeAddress = { ip: "10.0.0.4", port: 7002 };
const BYTES = new Uint8Array([1, 2, 3]);

/** A processor that records every input and answers from a script. */
class ScriptedProcessor implements Processor {
  readonly inputs: Input[] = [];
  deadline: number | null = null;
  /** True while `handle` runs; a second `handle` starting inside one is a bug. */
  inside = false;
  reentered = false;
  constructor(private readonly script: (input: Input) => Output[] = () => []) {}

  handle(input: Input): Output[] {
    if (this.inside) this.reentered = true;
    this.inside = true;
    try {
      this.inputs.push(input);
      return this.script(input);
    } finally {
      this.inside = false;
    }
  }

  nextDeadline(): number | null {
    return this.deadline;
  }

  get kinds(): string[] {
    return this.inputs.map((input) => input.kind);
  }
}

const send = (to: NodeAddress): Output => ({ kind: "send", to, bytes: BYTES });

function setup(processor: Processor, wrap: (inner: Transport) => Transport = (inner) => inner) {
  const network = createMemoryNetwork();
  const inner = network.createTransport(NODE_IP);
  const clock: ManualClock = createManualClock();
  const logger = createRecordingLogger();
  const driver = new NodeDriver(processor, wrap(inner), clock, logger);
  return { network, clock, logger, driver };
}

describe("NodeDriver", () => {
  test("feeds started with the bound port before any frame", async () => {
    const processor = new ScriptedProcessor();
    const { driver } = setup(processor, (inner) => ({
      send: inner.send,
      // A frame lands while listen is still resolving.
      async listen(port, onFrame) {
        const server = await inner.listen(port, onFrame);
        onFrame(BYTES, PEER);
        return server;
      },
    }));

    await driver.start(0);

    expect(processor.kinds).toEqual(["started", "frame"]);
    expect(processor.inputs[0]).toMatchObject({ kind: "started", port: 41_000 });
    await driver.stop();
  });

  test("starts every send of one input without awaiting any", async () => {
    const processor = new ScriptedProcessor((input) =>
      input.kind === "started" ? [send(PEER), send(OTHER), send({ ip: "10.0.0.5", port: 7003 })] : [],
    );
    const started: NodeAddress[] = [];
    const { driver } = setup(processor, (inner) => ({
      listen: inner.listen,
      send(to) {
        started.push(to);
        return new Promise<void>(() => {}); // never settles
      },
    }));

    await driver.start(0);

    expect(started).toHaveLength(3);
    await driver.stop();
  });

  test("feeds sendFailed when a send is rejected", async () => {
    const processor = new ScriptedProcessor((input) =>
      input.kind === "started" ? [send(PEER), send(OTHER)] : [],
    );
    const { clock, driver } = setup(processor, (inner) => ({
      listen: inner.listen,
      send: (to) => (to.port === OTHER.port ? Promise.reject(new Error("refused")) : Promise.resolve()),
    }));

    await driver.start(0);
    await settle();

    expect(processor.inputs.slice(1)).toEqual([{ kind: "sendFailed", to: OTHER, now: clock.now() }]);
    await driver.stop();
  });

  test("re-arms one timer from nextDeadline and feeds timeout when it passes", async () => {
    const processor = new ScriptedProcessor();
    const { clock, network, driver } = setup(processor);
    const start = clock.now();
    processor.deadline = start + 500;
    await driver.start(0);

    clock.advance(499);
    expect(processor.kinds).toEqual(["started"]);

    // A frame moves the deadline later; the earlier timer must not fire.
    processor.deadline = start + 2_000;
    const peer = network.createTransport(PEER.ip);
    await peer.listen(PEER.port, () => {});
    await peer.send({ ip: NODE_IP, port: 41_000 }, BYTES);
    await settle();
    clock.advance(1);
    expect(processor.kinds).toEqual(["started", "frame"]);

    processor.deadline = null;
    clock.advance(1_500);
    expect(processor.kinds).toEqual(["started", "frame", "timeout"]);
    expect(processor.inputs[2]).toEqual({ kind: "timeout", now: start + 2_000 });

    // No deadline left, so nothing is armed.
    clock.advance(10_000);
    expect(processor.kinds).toHaveLength(3);
    await driver.stop();
  });

  test("drains an input fed during handle in order, after the current one returns", async () => {
    let deliver: ((bytes: Uint8Array, peer: NodeAddress) => void) | undefined;
    const second = new Uint8Array([9]);
    const processor = new ScriptedProcessor((input) => {
      if (input.kind === "frame" && input.bytes === BYTES) deliver?.(second, OTHER);
      return [];
    });
    const { driver } = setup(processor, (inner) => ({
      send: inner.send,
      listen(port, onFrame) {
        deliver = onFrame;
        return inner.listen(port, onFrame);
      },
    }));
    await driver.start(0);

    deliver?.(BYTES, PEER);

    expect(processor.kinds).toEqual(["started", "frame", "frame"]);
    expect(processor.inputs.map((input) => (input.kind === "frame" ? input.peer : null))).toEqual([
      null,
      PEER,
      OTHER,
    ]);
    expect(processor.reentered).toBe(false);
    await driver.stop();
  });

  test("stop closes the listener and the timer, and is safe to call twice", async () => {
    const processor = new ScriptedProcessor();
    const { clock, network, driver } = setup(processor);
    processor.deadline = clock.now() + 100;
    await driver.start(0);
    const peer = network.createTransport(PEER.ip);
    await peer.listen(PEER.port, () => {});

    await driver.stop();
    await driver.stop();
    clock.advance(1_000);
    await peer.send({ ip: NODE_IP, port: 41_000 }, BYTES);
    await settle();

    expect(processor.kinds).toEqual(["started"]);
    // The port is free again.
    await network.createTransport(NODE_IP).listen(41_000, () => {});
  });
});
