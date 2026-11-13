// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a validator runs, as one value: the commit processor and the workload its load
//! generator submits.

use dag::authority::Authority;
use execution::workload::Workload;
use replica::config::PublicReplicaConfig;

use crate::processor::CommitProcessor;

pub trait Engine {
    type Processor: CommitProcessor;
    type Workload: Workload;

    /// `authority`'s commit processor.
    fn processor(
        &self,
        authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> eyre::Result<Self::Processor>;

    fn workload(&self) -> Self::Workload;
}
