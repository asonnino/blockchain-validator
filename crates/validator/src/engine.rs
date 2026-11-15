// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a validator runs, as one value: the commit processor and the workload its load
//! generator submits.

use std::{path::Path, time::Duration};

use dag::authority::Authority;
use execution::{engine::ExecutionEngine, workload::Workload};
use replica::config::PublicReplicaConfig;
use serde_json::{Map, Value};

use crate::{generator::LoadGeneratorConfig, processor::CommitProcessor};

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

    /// Checks the engine can run `load` on `public_config`'s committee: its workload accepts the
    /// load's transaction size, and a validator's processor builds.
    fn validate_run(
        &self,
        load: &LoadGeneratorConfig,
        public_config: &PublicReplicaConfig,
    ) -> eyre::Result<()> {
        load.validate(&self.workload())?;
        self.processor(Authority::from(0usize), public_config)?;
        Ok(())
    }

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

#[cfg(test)]
mod tests {
    use dag::authority::Authority;
    use fake_engine::{FakeExecutor, FakeWorkload};
    use replica::config::PublicReplicaConfig;

    use super::Engine;
    use crate::{generator::LoadGeneratorConfig, processor::SequentialProcessor};

    /// Builds no processor, as an engine whose settings don't fit the committee.
    struct Unfit;

    impl Engine for Unfit {
        type Processor = SequentialProcessor<FakeExecutor>;
        type Workload = FakeWorkload;
        type Reference = FakeExecutor;

        fn processor(
            &self,
            _authority: Authority,
            _public_config: &PublicReplicaConfig,
        ) -> eyre::Result<Self::Processor> {
            eyre::bail!("the settings don't fit the committee")
        }

        fn workload(&self) -> FakeWorkload {
            FakeWorkload::default()
        }
    }

    #[test]
    fn runs_validate_the_processor() {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let load = LoadGeneratorConfig::new_for_test();
        let error = Unfit.validate_run(&load, &public_config).unwrap_err();
        assert!(
            error.to_string().contains("don't fit the committee"),
            "{error}"
        );
    }
}
