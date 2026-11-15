// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! N-version execution: each validator runs one of several versions of the same contract, and the
//! validators vote on results through consensus. This crate holds version assignment, synthetic
//! versions, the digests votes carry, the decision rule, the commit processor, and the engine
//! that wires them into the validator.

mod assignment;
mod decision;
mod digest;
mod engine;
mod processor;
mod report;
mod synthetic;
mod vote;

pub use assignment::{VersionId, assign};
pub use decision::{Decider, Decision, Rejection};
pub use digest::{InputDigest, ResultDigest};
pub use engine::{ClusterConfig, CostConfig, NMoveConfig, Rule, Threshold};
pub use processor::{
    BlockRecord, CostModel, NVersionProcessor, Outcome, Records, TransactionRecord,
};
pub use synthetic::{Cluster, SyntheticConfig, SyntheticEngine};
pub use vote::{Position, Vote};
