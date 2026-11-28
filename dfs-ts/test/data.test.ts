import { describe, expect, test } from "bun:test";

import { DataProcessor, DEFAULT_RETRY_MS } from "../src/core/data.ts";
import type { Output } from "../src/core/processor.ts";
import { decode, encode } from "../src/protocol/codec.ts";
import { createAskIp, createAskIpAck, createNotify, type Packet } from "../src/protocol/packets.ts";
import type { NodeAddress } from "../src/types.ts";

const DNS: NodeAddress = { ip: "10.0.0.1", port: 53 };
const MASTER: NodeAddress = { ip: "10.0.0.4", port: 7000 };
const DATA_PORT = 7001;
const NOW = 1_000;

function setup(retryMs?: number) {
  const data = new DataProcessor(DNS, retryMs);
  const start = () => data.handle({ kind: "started", port: DATA_PORT, now: NOW });
  const ack = (master: NodeAddress | null, now = NOW) =>
    data.handle({ kind: "frame", bytes: encode(createAskIpAck(master)), peer: DNS, now });
  const timeout = (now: number) => data.handle({ kind: "timeout", now });
  return { data, start, ack, timeout };
}

/** The packet inside a `send` output, decoded; fails the test for anything else. */
function sentPacket(output: Output | undefined): { to: NodeAddress; packet: Packet } {
  if (output?.kind !== "send") throw new Error("expected a send output");
  const result = decode(output.bytes);
  if (!result.ok) throw new Error("send output does not decode");
  return { to: output.to, packet: result.value };
}

describe("DataProcessor", () => {
  test("asks dns with the bound port on start", () => {
    const { data, start } = setup();

    const outputs = start();

    expect(sentPacket(outputs[0])).toEqual({ to: DNS, packet: createAskIp(DATA_PORT) });
    expect(data.nextDeadline()).toBeNull();
  });

  test("an empty ack sets the retry deadline", () => {
    const { data, start, ack } = setup();
    start();

    const outputs = ack(null);

    expect(outputs.every((output) => output.kind === "log")).toBe(true);
    expect(data.nextDeadline()).toBe(NOW + DEFAULT_RETRY_MS);
  });

  test("the retry interval is configurable", () => {
    const { data, start, ack } = setup(250);
    start();

    ack(null);

    expect(data.nextDeadline()).toBe(NOW + 250);
  });

  test("a timeout at the deadline asks dns again and clears it", () => {
    const { data, start, ack, timeout } = setup();
    start();
    ack(null);

    const outputs = timeout(NOW + DEFAULT_RETRY_MS);

    expect(sentPacket(outputs[0])).toEqual({ to: DNS, packet: createAskIp(DATA_PORT) });
    expect(data.nextDeadline()).toBeNull();
  });

  test("a timeout before the deadline does nothing", () => {
    const { data, start, ack, timeout } = setup();
    start();
    ack(null);

    expect(timeout(NOW + DEFAULT_RETRY_MS - 1)).toEqual([]);
    expect(data.nextDeadline()).toBe(NOW + DEFAULT_RETRY_MS);
  });

  test("a stray timeout with no retry pending does nothing", () => {
    const { timeout } = setup();

    expect(timeout(NOW)).toEqual([]);
  });

  test("an ack with a master notifies it with the bound port", () => {
    const { data, start, ack } = setup();
    start();

    const outputs = ack(MASTER);

    expect(sentPacket(outputs[0])).toEqual({ to: MASTER, packet: createNotify("data", DATA_PORT) });
    expect(data.nextDeadline()).toBeNull();
  });

  test("a master found on a retry clears the deadline", () => {
    const { data, start, ack, timeout } = setup();
    start();
    ack(null);
    timeout(NOW + DEFAULT_RETRY_MS);

    const outputs = ack(MASTER, NOW + DEFAULT_RETRY_MS);

    expect(sentPacket(outputs[0]).to).toEqual(MASTER);
    expect(data.nextDeadline()).toBeNull();
  });

  test("a late ack after joining is ignored", () => {
    const { data, start, ack } = setup();
    start();
    ack(MASTER);

    expect(ack(MASTER)).toEqual([]);
    expect(ack(null)).toEqual([]);
    expect(data.nextDeadline()).toBeNull();
  });

  test("an ack before start is ignored", () => {
    const { data, ack } = setup();

    expect(ack(MASTER)).toEqual([]);
    expect(data.nextDeadline()).toBeNull();
  });

  test("drops an undecodable frame and ignores other packets", () => {
    const { data, start } = setup();
    start();

    const bad = data.handle({ kind: "frame", bytes: Uint8Array.of(15, 0), peer: DNS, now: NOW });
    const other = data.handle({
      kind: "frame",
      bytes: encode(createNotify("data", 1)),
      peer: DNS,
      now: NOW,
    });

    expect(bad).toMatchObject([{ kind: "log", level: "warn", message: "dropped undecodable frame" }]);
    expect(other).toEqual([]);
    expect(data.nextDeadline()).toBeNull();
  });
});
