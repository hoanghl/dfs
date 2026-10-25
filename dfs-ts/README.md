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
│   ├── platform.ts   Transport, BlobStore, Clock, Logger
│   └── types.ts      NodeAddress, Role, NodeConfig
└── test/             bun test
```

Lands with M1.1 and later, per file: `protocol/` (`packets.ts`, `codec.ts`, `bytes.ts`), `cluster.ts`, `catalog.ts`, `roles/` (`storage.ts`, `master.ts`, `data.ts`, `dns.ts`), `node.ts`, `client.ts`, plus the memory platform and real tests.

## Rules

- One package, no workspaces: `dfs-cli` and `xbutler-fe` consume `dfs-core`, they are not part of it.
- No `Buffer`, `node:*`, `setTimeout`, or `console` in `src/` — `tsconfig` keeps Node types out so those fail to compile.
- I/O arrives through `platform.ts` only; core never implements it.
- Time comes from `Clock`, so timers are testable.
- Build with `tsc` (JS + `.d.ts`, no bundler); test with `bun test`.
