// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! N-version execution: each validator runs one of several versions of the same contract, and the
//! validators vote on results through consensus. This crate holds the deterministic parts, free of
//! I/O: version assignment, synthetic versions, the digests votes carry, the decision rule, and
//! the commit processor.

mod assignment;
mod decision;
mod digest;
mod processor;
mod synthetic;
mod vote;

pub use assignment::{VersionId, assign};
pub use decision::{Decider, Decision, Rejection};
pub use digest::{InputDigest, ResultDigest};
pub use processor::{
    BlockRecord, CostModel, NVersionProcessor, Outcome, Records, TransactionRecord,
};
pub use synthetic::{Cluster, SyntheticConfig, SyntheticEngine};
pub use vote::{Position, Vote};
