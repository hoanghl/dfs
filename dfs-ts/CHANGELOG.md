# Changelog

## Unreleased

- Package skeleton: `package.json`, strict `tsconfig`, `src/index.ts`, `platform.ts`, `types.ts`, `bun test` harness, CI workflow.
- `DnsProcessor` and `MasterProcessor` on `NodeDriver`; `cluster.ts` moves to `core/` and takes `now`, `platform.ts` moves to `driver/`, and `Clock.every`, `node.ts` and `roles/` are removed.
- `Cluster` is a class; `test/layering.test.ts` fails if `core/` imports `driver/`.
- `core/frame.ts` `onFrame` decodes a frame once for every role; processors `switch` on `packet.id`.
- `DataProcessor`: asks DNS for the Master on start, retries every `retryMs` (default 1 s) while DNS has none, then notifies it; unlike Rust, which gives up.
- `index.ts` exports `NodeDriver`, `DnsProcessor`, `MasterProcessor`, `DataProcessor`, `Cluster` and the core types; README status is M1.1.
