# Changelog

## Unreleased

- Package skeleton: `package.json`, strict `tsconfig`, `src/index.ts`, `platform.ts`, `types.ts`, `bun test` harness, CI workflow.
- `DnsProcessor` and `MasterProcessor` on `NodeDriver`; `cluster.ts` moves to `core/` and takes `now`, `platform.ts` moves to `driver/`, and `Clock.every`, `node.ts` and `roles/` are removed.
