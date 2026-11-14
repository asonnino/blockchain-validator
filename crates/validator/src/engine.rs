// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a validator runs, as one value: the commit processor and the workload its load
//! generator submits.

use std::{path::Path, time::Duration};

use dag::authority::Authority;
use execution::{engine::ExecutionEngine, workload::Workload};
use replica::config::PublicReplicaConfig;
use serde_json::{Map, Value};

use crate::processor::CommitProcessor;

pub trait Engine {
    type Processor: CommitProcessor;
    type Workload: Workload;
    /// The execution engine of the sequential counterpart a simulated run compares against.
    type Reference: ExecutionEngine + Send + 'static;

    /// `authority`'s commit processor.
    fn processor(
        &self,
        authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> eyre::Result<Self::Processor>;

    fn workload(&self) -> Self::Workload;

    /// Set when simulated runs also run the same workload under the default processor.
    fn reference(&self) -> Option<Self::Reference> {
        None
    }

    /// Writes the engine's own files for a simulated run into `dir` and returns its summary
    /// fields. `start` is the simulated time the load started at.
    fn report(
        &self,
        _processors: Vec<Self::Processor>,
        _start: Duration,
        _dir: &Path,
    ) -> eyre::Result<Map<String, Value>> {
        Ok(Map::new())
    }
}
