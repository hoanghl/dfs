/**
 * Big-endian integers and IPv4 conversion over plain `Uint8Array`.
 *
 * Core never touches `Buffer` or other Node globals: the same module has to run
 * unchanged on the mobile runtime. Reads throw `RangeError` outside the buffer,
 * so callers parsing untrusted bytes validate the frame size first (see
 * `codec.ts`); a throw from here is a programming error, not a protocol error.
 */

function u8At(bytes: Uint8Array, offset: number): number {
  const value = bytes[offset];
  if (value === undefined) {
    throw new RangeError(
      `Cannot read 1 byte at offset ${offset}: buffer is ${bytes.length} bytes`,
    );
  }
  return value;
}

function writeUintBE(
  bytes: Uint8Array,
  offset: number,
  value: number,
  size: number,
): void {
  const max = 2 ** (8 * size) - 1;
  if (!Number.isInteger(value) || value < 0 || value > max) {
    throw new RangeError(`Value ${value} does not fit in uint${8 * size}`);
  }
  if (offset < 0 || offset + size > bytes.length) {
    throw new RangeError(
      `Cannot write ${size} bytes at offset ${offset}: buffer is ${bytes.length} bytes`,
    );
  }

  let remaining = value;
  for (let index = size - 1; index >= 0; index -= 1) {
    bytes[offset + index] = remaining % 256;
    remaining = Math.floor(remaining / 256);
  }
}

/** Read a big-endian `u8`. */
export function readU8(bytes: Uint8Array, offset: number): number {
  return u8At(bytes, offset);
}

/** Read a big-endian `u16`. */
export function readU16BE(bytes: Uint8Array, offset: number): number {
  return (u8At(bytes, offset) << 8) | u8At(bytes, offset + 1);
}

/** Read a big-endian `u32`. */
export function readU32BE(bytes: Uint8Array, offset: number): number {
  return (
    ((u8At(bytes, offset) << 24) |
      (u8At(bytes, offset + 1) << 16) |
      (u8At(bytes, offset + 2) << 8) |
      u8At(bytes, offset + 3)) >>>
    0
  );
}

/** Write a big-endian `u8`. */
export function writeU8(bytes: Uint8Array, offset: number, value: number): void {
  writeUintBE(bytes, offset, value, 1);
}

/** Write a big-endian `u16`. */
export function writeU16BE(
  bytes: Uint8Array,
  offset: number,
  value: number,
): void {
  writeUintBE(bytes, offset, value, 2);
}

/** Write a big-endian `u32`. */
export function writeU32BE(
  bytes: Uint8Array,
  offset: number,
  value: number,
): void {
  writeUintBE(bytes, offset, value, 4);
}

/** Parse a dotted-quad IPv4 address into four bytes. */
export function ipv4ToBytes(ip: string): Uint8Array {
  const parts = ip.split(".");
  if (parts.length !== 4) {
    throw new RangeError(`Invalid IPv4 address: ${ip}`);
  }

  const bytes = new Uint8Array(4);
  for (const [index, part] of parts.entries()) {
    if (!/^\d{1,3}$/.test(part)) {
      throw new RangeError(`Invalid IPv4 address: ${ip}`);
    }
    const octet = Number(part);
    if (octet > 255) {
      throw new RangeError(`Invalid IPv4 address: ${ip}`);
    }
    bytes[index] = octet;
  }
  return bytes;
}

/** Write a dotted-quad IPv4 address as four bytes. */
export function writeIpv4(
  bytes: Uint8Array,
  offset: number,
  ip: string,
): void {
  if (offset < 0 || offset + 4 > bytes.length) {
    throw new RangeError(
      `Cannot write 4 bytes at offset ${offset}: buffer is ${bytes.length} bytes`,
    );
  }
  bytes.set(ipv4ToBytes(ip), offset);
}

/** Read four bytes as a dotted-quad IPv4 address. */
export function readIpv4(bytes: Uint8Array, offset: number): string {
  return [
    u8At(bytes, offset),
    u8At(bytes, offset + 1),
    u8At(bytes, offset + 2),
    u8At(bytes, offset + 3),
  ].join(".");
}
