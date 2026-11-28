/**
 * The frame step every processor shares: decode the bytes, or say why not.
 *
 * Decoding is total, so a bad frame never throws; it becomes a `warn` log
 * output and the node keeps serving. Kept in one place so every role words
 * and shapes that log the same way.
 */

import { decode } from "../protocol/codec.js";
import type { Packet } from "../protocol/packets.js";
import type { Input, Output } from "./processor.js";

/** An {@link Input} that carries received bytes. */
export type FrameInput = Extract<Input, { readonly kind: "frame" }>;

/**
 * Decode `input` and hand the packet to `handle`.
 *
 * @param input The frame to decode.
 * @param handle Role logic for one decoded packet; not called when decoding fails.
 * @returns `handle`'s outputs, or one `warn` log when the frame is undecodable.
 */
export function onFrame(input: FrameInput, handle: (packet: Packet) => Output[]): Output[] {
  const result = decode(input.bytes);
  if (!result.ok) {
    return [
      {
        kind: "log",
        level: "warn",
        message: "dropped undecodable frame",
        meta: { code: result.error.code, message: result.error.message, peer: input.peer },
      },
    ];
  }
  return handle(result.value);
}
