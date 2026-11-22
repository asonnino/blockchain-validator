// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! n-move as an engine: the processor and workload its config gives each validator, and its
//! report on a simulated run.

use std::{path::Path, sync::Arc, time::Duration};

use dag::{authority::Authority, committee::Stake};
use eyre::{Result, ensure, eyre};
use fake_engine::{FakeExecutor, FakeWorkload};
use replica::config::PublicReplicaConfig;
use serde_json::{Map, Value};
use validator::engine::Engine;

use crate::{
    config::NMoveConfig,
    processor::NVersionProcessor,
    report::RunRecords,
    versions::{SyntheticConfig, SyntheticEngine, VersionId, Versions, assign},
};

impl Engine for NMoveConfig {
    type Processor = NVersionProcessor<SyntheticEngine>;
    type Workload = FakeWorkload;
    type Reference = FakeExecutor;

    fn processor(
        &self,
        authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> Result<Self::Processor> {
        let committee = public_config.committee();
        let versions = self.versions.unwrap_or(committee.len());
        ensure!(
            versions <= committee.len(),
            "{versions} versions need as many validators, not {}",
            committee.len()
        );
        self.validate_versions(versions)?;
        let reach = 1..=committee.total_stake();
        let threshold = self
            .threshold()
            .filter(|threshold| reach.contains(threshold));
        let threshold =
            threshold.ok_or_else(|| eyre!("the threshold is out of the committee's reach"))?;

        let synthetic = Arc::new(self.synthetic(versions));
        let engines = (0..versions)
            .map(|version| SyntheticEngine::new(VersionId(version), synthetic.clone()))
            .collect();
        let correct = SyntheticConfig {
            seed: self.seed,
            failure: vec![0.0],
            clusters: Vec::new(),
        };
        let reference = SyntheticEngine::new(VersionId(0), Arc::new(correct));
        let assignment = assign(&committee, versions, self.seed);
        let versions = Versions::new(engines, reference, assignment).with_cost(self.cost.model());
        let processor = NVersionProcessor::new(
            authority,
            versions,
            threshold,
            self.cutoff_rounds,
            public_config,
        )?;
        let processor = match self.recording {
            true => processor.with_records(),
            false => processor,
        };
        let Some(byzantine) = &self.byzantine else {
            return Ok(processor);
        };
        let authorities = byzantine.authorities.iter();
        ensure!(
            authorities.clone().all(|index| *index < committee.len()),
            "Byzantine validators beyond the {} validators",
            committee.len()
        );
        let stake: Stake = authorities
            .filter_map(|index| committee.get_stake(Authority::from(*index)))
            .sum();
        ensure!(
            stake < threshold,
            "Byzantine validators hold {stake} stake, reaching the threshold of {threshold}"
        );
        Ok(match byzantine.authorities.contains(&authority.index()) {
            true => processor.with_byzantine(byzantine.byzantine()),
            false => processor,
        })
    }

    fn workload(&self) -> FakeWorkload {
        FakeWorkload::default().with_updates(self.update_share)
    }

    /// The synthetic versions are fake-engine executions, wrong at times.
    fn reference(&self) -> Option<FakeExecutor> {
        Some(FakeExecutor)
    }

    fn report(
        &self,
        processors: Vec<Self::Processor>,
        start: Duration,
        dir: &Path,
    ) -> Result<Map<String, Value>> {
        ensure!(
            self.recording,
            "reports need the engine built `recording()`"
        );
        let records = RunRecords::new(processors, start);
        records.write_transactions(dir)?;
        records.write_blocks(dir)?;
        Ok(records.summary(self))
    }
}

#[cfg(test)]
mod tests {
    use dag::authority::Authority;
    use replica::config::PublicReplicaConfig;
    use validator::engine::Engine;

    use crate::config::NMoveConfig;

    fn config(yaml: &str) -> NMoveConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn processors_check_what_depends_on_the_committee() {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let authority = Authority::from(0usize);
        let two_byzantine = "{byzantine: {authorities: [0, 1], strategy: {fixed: 7}}}";
        for fit in ["{}", "{threshold: {t: 4}}", two_byzantine] {
            assert!(
                config(fit).processor(authority, &public_config).is_ok(),
                "{fit}"
            );
        }
        for unfit in [
            "{versions: 5}",
            "{threshold: {t: 5}}",
            "{threshold: {t: 0}}",
            "{faults: 2}",
            "{faults: 9223372036854775808}",
            // One version per validator, unless set.
            "{failure: [0.0, 0.0, 0.0, 0.0, 0.0]}",
            "{clusters: [{versions: [4], probability: 1.0}]}",
            "{byzantine: {authorities: [4], strategy: withhold}}",
            // Byzantine stake reaching the threshold of 3, even for an honest validator.
            "{byzantine: {authorities: [1, 2, 3], strategy: {fixed: 7}}}",
        ] {
            let processor = config(unfit).processor(authority, &public_config);
            assert!(processor.is_err(), "{unfit} fits 4 validators");
        }
        // Stake counts, not validators: validator 0 holds 2 of 5.
        let mut weighted = public_config.clone();
        weighted.identifiers[0].stake = 2;
        assert!(
            config(two_byzantine)
                .processor(authority, &weighted)
                .is_err()
        );
    }

    #[test]
    fn validators_share_fewer_versions() {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let two = config("{versions: 2}");
        for authority in 0..4usize {
            let processor = two
                .processor(Authority::from(authority), &public_config)
                .unwrap();
            assert!(processor.version().0 < 2);
        }
    }
}
