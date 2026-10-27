# Step 1 — Wire protocol and codec

Ticket: [TOM-33](https://linear.app/tommymindpalace/issue/TOM-33/step-1-wire-protocol-and-codec) · Parent: [TOM-23](https://linear.app/tommymindpalace/issue/TOM-23/m11-initialization-askip-askipack-notify) · Analysis: [1027-analysis.md](./1027-analysis.md)

## 1. Purpose — why this step matters

- **Position:** Step 1 of 6 sub-issues under TOM-23 (M1.1 initialization: `AskIp` → `AskIpAck` → `Notify`). It is the foundation step; nothing runs yet.
- **Job:** port the byte-level wire contract from Rust (`dfs-rust/src/components/packets.rs`) into TS — byte helpers, packet models, encode/decode. No node loop, roles, membership, or platform (those are steps 3–5).
- **Contribution to the plan:**
  - Gives the whole M1.1 chain a single, tested source of truth for frames — every later step (node loop, DNS/Master/Data roles, membership) only exchanges `Uint8Array` and never re-parses bytes.
  - Establishes **byte-for-byte Rust compatibility** so future Rust↔TS interop (deferred to the CLI/platform ticket) is not a rewrite.
  - Introduces the `Result<Packet, ProtocolError>` discipline ("never throw to the loop"), which the N3/N4/F8 resilience tests later depend on.
  - Unblocks M1.2 (heartbeat): framing, packet ids, and role codes are already defined.
- **Explicitly out of scope:** `dfs-cli`, sockets/Node platform, retries, shutdown, `createClient`, real-network interop.

## 2. Implementation plan

- **Order of work:** confirm the golden hex against the Rust build **first**, then implement TS.
- **`src/protocol/bytes.ts`** — big-endian `u16`/`u32` read/write, IPv4 parse (`"192.168.0.10"` ↔ 4 bytes) / format. Plain `Uint8Array` only: no `Buffer`, no Node globals. UTF-8 helpers deferred to M1.3.
- **`src/protocol/packets.ts`** — packet models + role codes
  - Frame: `packetId (u8) + payloadSize (u32 BE) + payload`, header 5 bytes.
  - `AskIp` id `6`, payload `port` (2 BE), requires length 2.
  - `AskIpAck` id `7`, payload empty (0) **or** `ipv4(4) + port(2)` (6); empty ⇒ `master: null` (Rust instead returns an error and the Data node never recovers — TS models it as valid).
  - `Notify` id `15`, payload `role(1) + port(2 BE)`, requires length 3; the IP always comes from the socket peer, never the payload.
  - Role byte: `0 default, 1 master, 2 data, 3 dns, 4 client`; anything else is an error.
- **`src/protocol/codec.ts`** — `encode(packet): Uint8Array`, `decode(frame): Result<Packet, ProtocolError>`
  - Validate `frame.length === 5 + payloadSize` exactly (both short and trailing bytes).
  - Distinguish errors: truncated header, size mismatch, wrong payload size per packet, unknown role byte, unknown packet id.
  - Decode must be total — never throws; the caller (node loop) logs and survives.
- **Golden frames to assert:**
  - `AskIp 9000` → `06 00 00 00 02 23 28`
  - `AskIpAck` empty → `07 00 00 00 00`; `AskIpAck 192.168.0.10:7888` → `07 00 00 00 06 C0 A8 00 0A 1E D0`
  - `Notify master 7888` → `0F 00 00 00 03 01 1E D0`; `Notify data 9000` → `0F 00 00 00 03 02 23 28`
- **Open decisions carried into this step:** tests are not typechecked today (add `@types/bun` + `tsconfig.test.json`); packet id `0` (`Default`) must be rejected as unknown.

## 3. Test cases — `test/codec.test.ts`, C1–C9

Each golden asserts both encode and decode (no per-direction duplicates).

- **C1 — `AskIp` golden:** bytes `06 00 00 00 02 23 28`, plus port `1` to prove BE order.
- **C2 — `AskIpAck` golden:** empty `07 00 00 00 00` and master `07 00 00 00 06 C0 A8 00 0A 1E D0`.
- **C3 — `Notify` golden:** master `0F 00 00 00 03 01 1E D0` and data `0F 00 00 00 03 02 23 28`.
- **C4 — Round trip:** arbitrary values (ports `1`/`65535`, both roles) → bytes in = bytes out.
- **C5 — Short frame:** `< 5` bytes → truncated-header error.
- **C6 — Length mismatch:** declared size ≠ actual, both short payload and trailing bytes → size-mismatch error.
- **C7 — Wrong payload size (table-driven):** `AskIp` 0/1/3, `AskIpAck` 1/5/7, `Notify` 2/4.
- **C8 — Unknown role byte:** `0`, `5`, `255` → unknown-role error.
- **C9 — Unknown packet id:** `0`, `18`, `255` → unknown-packet error.

**Acceptance for this step:** C1–C9 green, goldens traceable to the Rust build, no panics/exceptions escaping decode.
