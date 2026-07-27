# blockchain-validator

A validator node composing the [mysticeti](https://github.com/asonnino/mysticeti)
consensus replica with an execution engine and a checkpoint engine. Mysticeti
deliberately scopes out application execution; this repo adds it on top,
consuming the mysticeti crates as git dependencies pinned to a single rev.

## Crates

- `execution` — `ExecutionEngine` trait; fake engine first, move-native backend later.
- `checkpoint` — checkpoint engine modeled on Sui's Builder/Aggregator/Executor,
  collapsed into a single flow with `highest_synced`/`highest_executed` watermarks.
- `validator` — sui-node-analogue glue: replica + transaction input + commit
  consumer + engines, plus a SimulationRunner for deterministic tests.

## Design constraints

- Every component is generic over mysticeti's `Ctx` trait: no threads, disk, or
  wall-clock outside it, so the whole validator runs under the simulator.
- `Ctx` exposes no RNG; components take explicit seeds/config instead.
- All mysticeti dependencies stay on one pinned rev (see the workspace
  `Cargo.toml`); bump them together.

## Build

```
cargo check --workspace --all-targets
cargo test --workspace
```
