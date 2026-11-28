/**
 * The one place that runs a {@link Processor}: it owns the listener, the
 * timer and the sends, and turns each of them into an input.
 *
 * Inputs are handled one at a time, in order. Sends start without `await`, so
 * one slow peer never holds up the others; a rejected send comes back as a
 * `sendFailed` input.
 */

import type { Input, Output, Processor } from "../core/processor.js";
import type { Clock, Logger, Server, Timer, Transport } from "./platform.js";

/**
 * Runs one {@link Processor} on a transport and a clock.
 *
 * It is the only code that touches I/O: frames, timer expiry and failed sends
 * become inputs, and the processor's outputs become log lines and sends.
 */
export class NodeDriver {
  /** Frames that arrived while `listen` was still resolving; fed after `started`. */
  private readonly early: Input[] = [];
  private ready = false;
  private stopped = false;
  private timer: Timer | null = null;
  private server: Server | null = null;

  /**
   * @param processor Role logic; receives every input, one at a time.
   * @param transport Where frames arrive from and sends go to.
   * @param clock Source of `now` for inputs and of the single timer.
   * @param logger Destination for `log` outputs.
   */
  constructor(
    private readonly processor: Processor,
    private readonly transport: Transport,
    private readonly clock: Clock,
    private readonly logger: Logger,
  ) {}

  /**
   * Bind `port`, feed `started` with the bound port, then deliver frames.
   *
   * Frames that arrive while `listen` is still resolving are held and fed
   * after `started`, so no frame ever precedes it. Resolves once startup is
   * done; the node keeps running on events until {@link stop}.
   *
   * @param port Port to listen on; `0` lets the transport pick one.
   */
  async start(port: number): Promise<void> {
    const server = await this.transport.listen(port, (bytes, peer) => {
      const input: Input = { kind: "frame", bytes, peer, now: this.clock.now() };
      if (this.ready) this.feed(input);
      else this.early.push(input);
    });
    if (this.stopped) {
      await server.close();
      return;
    }
    this.server = server;
    this.ready = true;
    this.feed({ kind: "started", port: server.port, now: this.clock.now() });
    for (const input of this.early.splice(0)) this.feed(input);
  }

  /**
   * Close the listener and the timer. Safe to call twice.
   *
   * After this, inputs from late frames or failed sends are dropped and the
   * timer is never re-armed.
   */
  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.timer?.cancel();
    this.timer = null;
    await this.server?.close();
  }

  /**
   * Run one input through the processor, carry out its outputs, then re-arm
   * the timer.
   *
   * Calls never nest, so inputs need no queue and are handled in the order
   * they arrive: {@link Transport} never calls `onFrame` from inside `send`,
   * and timer and send-failure callbacks always run later.
   */
  private feed(input: Input): void {
    if (this.stopped) return;
    for (const output of this.processor.handle(input)) this.execute(output);
    this.rearmTimer();
  }

  /**
   * Carry out one processor output.
   *
   * A `log` goes to the logger. A `send` starts without `await`, so peers do
   * not wait on each other; a rejection comes back as a `sendFailed` input.
   */
  private execute(output: Output): void {
    if (output.kind === "log") {
      this.logger[output.level](output.message, output.meta);
      return;
    }
    void this.transport.send(output.to, output.bytes).catch(() => {
      this.feed({ kind: "sendFailed", to: output.to, now: this.clock.now() });
    });
  }

  /**
   * Replace the pending timer with one for `processor.nextDeadline()`.
   *
   * No deadline means no timer. A deadline already past fires at once. When
   * the timer fires it feeds a `timeout` input.
   */
  private rearmTimer(): void {
    this.timer?.cancel();
    this.timer = null;
    const deadline = this.processor.nextDeadline();
    if (deadline === null) return;
    this.timer = this.clock.after(Math.max(0, deadline - this.clock.now()), () => {
      this.timer = null;
      this.feed({ kind: "timeout", now: this.clock.now() });
    });
  }
}
