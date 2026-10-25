# Distributed File System

One repository, two implementations of the same protocol:

| Directory | Language | Status |
| --- | --- | --- |
| [`dfs-rust/`](dfs-rust/) | Rust | Original implementation. Stays as the protocol reference (reference bytes, interop) while the rewrite is in progress |
| `dfs-ts/` | TypeScript | Rewrite (M1 and later). `dfs-core` is consumed by `dfs-cli` and `xbutler-fe` |

- Start with [`dfs-rust/README.md`](dfs-rust/README.md) for build and run instructions.
- Design and protocol documents live in the `xbutler-document` repo, including `docs/1024-repo-structure.md`.
- M1 planning notes live in [`docs/1025-m1.0-prepare-dfs-ts/`](docs/1025-m1.0-prepare-dfs-ts/).
