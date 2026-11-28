# `dfs-ts` — TypeScript rewrite

Home of the DFS rewrite. The directory is `dfs-ts/`; the package it publishes is **`dfs-core`**, the library imported by `dfs-cli` and `xbutler-fe`.

Status: **skeleton** (M1.0). Types and platform interfaces only — protocol code starts with M1.1.

## Commands

```bash
bun install
bun run typecheck     # tsc --noEmit
bun test              # bun test
bun run build         # tsc -> dist/
bun run check:package # publint + attw
```

## Layout

```
dfs-ts/
├── package.json      "type": "module", exports (types + default), files: ["dist"], sideEffects: false
├── tsconfig.json     strict, lib ES2022, types: [] (no Node globals)
├── src/
│   ├── index.ts      the only public entry
│   ├── types.ts      NodeAddress, Role, NodeConfig
│   ├── protocol/     packets, codec, bytes: pure
│   ├── core/         sans-IO processors (`handle(input) → outputs`); never imports `driver/`
│   └── driver/       NodeDriver and platform.ts (Transport, BlobStore, Clock, Logger)
└── test/             bun test
```

Lands with M1.1 and later, per file: `core/` (`catalog.ts`, `data.ts`, `storage.ts`), `driver/outbox.ts` and `client.ts`.

## Rules

- One package, no workspaces: `dfs-cli` and `xbutler-fe` consume `dfs-core`, they are not part of it.
- `src/` stays runtime-agnostic, because the same code runs on Node.js and on React Native: use no `Buffer`, `node:*` modules, `setTimeout`, or `console`. `tsconfig.json` sets `"types": []`, so importing Node.js APIs fails the build instead of being caught in review.
- I/O arrives through `driver/platform.ts` only, and only `NodeDriver` calls it; `core/` returns outputs instead of doing I/O.
- Time arrives as `now` in each input and as `nextDeadline()`, so timers are testable without mocks.
- Where an older design note and `1128-redesign.md` disagree, the redesign wins: roles are `Processor` classes composed by the driver, never subclassed; `core/` never imports `driver/` (`test/layering.test.ts`).
- Build with `tsc` (JS + `.d.ts`, no bundler); test with `bun test`.
