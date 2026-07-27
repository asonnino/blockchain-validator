//! Validator node.
//!
//! Glue in the spirit of sui-node: builds the mysticeti replica, feeds client
//! transactions through its `TransactionClient`, and wires the bounded commit
//! consumer (`ReplicaBuilder::with_commit_consumer`) into the checkpoint and
//! execution engines. A SimulationRunner over the public simulator crate
//! drives the whole validator deterministically in tests.
