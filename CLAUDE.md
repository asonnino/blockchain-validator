# blockchain-validator

A validator node composing the [mysticeti](https://github.com/asonnino/mysticeti)
consensus replica (sister repo at `~/GitHub/mysticeti`) with an execution engine
and a checkpoint engine. Mysticeti deliberately scopes out application
execution; this repo adds it on top, in the spirit of a minimal sui-node.

## Memory

Store memories for this project in the default location
(`~/.claude/projects/`), not the Obsidian vault.

## Workspace

- `crates/execution` — `ExecutionEngine` trait consumed by the checkpoint and
  validator crates.
- `crates/checkpoint` — checkpoint engine modeled on Sui's
  Builder/Aggregator/Executor pipeline, collapsed to a single self-certifying
  flow; tracks `highest_synced` / `highest_executed` watermarks.
- `crates/validator` — glue: builds the replica, feeds transactions through
  `TransactionClient`, wires the commit consumer into checkpoint + execution.

Mysticeti crates (`dag`, `replica`, `simulator`) are git dependencies pinned to
a rev in the workspace `Cargo.toml`; bump the rev everywhere at once.

## Design constraints

- Everything must stay generic over `dag::context::Ctx` so the whole validator
  runs under the mysticeti discrete-event simulator: no threads, disk, or
  wall-clock outside `Ctx`, no ambient RNG (take explicit seeds), and spawn
  simulated tasks via `Ctx::spawn`.
- Commit stream semantics (from mysticeti's embedding contract): commits arrive
  in order via a bounded channel — a slow consumer backpressures the replica.
  Recovered commits are NOT re-emitted on restart; catch up via
  `Storage::iter_commits` before subscribing.
- Deterministic tests drive the full validator through `SimulationRunner`.

## Conventions

- Toolchain pinned in `rust-toolchain.toml` (Rust 1.97, edition 2024).
- Format with `cargo fmt` (repo `rustfmt.toml`); lint with `cargo clippy`.
- A pre-commit hook chain (`.pre-commit-config.yaml`) runs on every commit:
  whitespace/EOF fixers, editorconfig, yamlfmt, taplo, typos, licensesnip,
  shellcheck, cargo fmt/clippy/nextest. Install with
  `pip install pre-commit && pre-commit install`. If a hook fails, fix the
  underlying issue rather than bypassing the hook.
- Commit messages and PR titles follow Conventional Commits (enforced in CI).
- Lines are capped at 100 characters (`.editorconfig`).
- When mysticeti internals are unclear, read the sister repo directly —
  `docs/architecture.md` there documents the crate seams and the
  transaction-input / commit-output contract.
