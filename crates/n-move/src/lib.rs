// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! N-version execution: each validator runs one of several versions of the same contract, and the
//! validators vote on results through consensus. This crate holds the versions and who runs which,
//! the digests votes carry, the decision rule, Byzantine voters, the commit processor, and the
//! engine that wires them into the validator from its config.

mod byzantine;
mod config;
mod decision;
mod digest;
mod engine;
mod processor;
mod report;
mod versions;
mod vote;

pub use byzantine::{Byzantine, Selective, Strategy};
pub use config::{
    ByzantineConfig, ClusterConfig, CostConfig, NMoveConfig, Rule, SelectiveConfig, StrategyConfig,
    Threshold,
};
pub use decision::{Decider, Decision, Rejection};
pub use digest::{InputDigest, ResultDigest};
pub use processor::NVersionProcessor;
pub use report::{BlockRecord, Outcome, Records, TransactionRecord};
pub use versions::{
    Cluster, CostModel, SyntheticConfig, SyntheticEngine, VersionId, Versions, assign,
};
pub use vote::{Position, Vote};
