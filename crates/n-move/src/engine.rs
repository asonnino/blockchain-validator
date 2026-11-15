// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! n-move as an engine: its config, the processor and workload it gives each validator, and its
//! report on a simulated run.

use std::{path::Path, sync::Arc, time::Duration};

use dag::{authority::Authority, committee::Stake, config::ImportExport};
use eyre::{Result, ensure, eyre};
use fake_engine::{FakeExecutor, FakeWorkload};
use replica::config::PublicReplicaConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use validator::engine::Engine;

use crate::{
    assignment::{VersionId, assign},
    processor::{CostModel, NVersionProcessor},
    report::RunRecords,
    synthetic::{Cluster, SyntheticConfig, SyntheticEngine},
};

/// The n-move engine's parameters: the synthetic versions, how validators decide on their results,
/// and what executing costs.
#[derive(Serialize, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct NMoveConfig {
    /// Seeds the version assignment and the synthetic versions' wrongness.
    pub seed: u64,
    /// f, for the threshold rules.
    pub faults: u64,
    /// W, the wrong versions the threshold rules budget for.
    pub wrong: u64,
    /// `{t: <stake>}`, `{rule: simple_majority}`, or `{rule: honest_majority}`.
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub threshold: Threshold,
    /// Rounds a transaction may stay pending after its commit executes.
    pub cutoff_rounds: u64,
    /// The versions spread over the committee; one per validator if absent.
    pub versions: Option<usize>,
    /// Each version's failure probability; missing versions are never wrong on their own.
    pub failure: Vec<f64>,
    pub clusters: Vec<ClusterConfig>,
    pub cost: CostConfig,
    /// The share of transactions updating a recent creation rather than creating an object.
    pub update_share: f64,
    #[serde(skip)]
    recording: bool,
}

impl Default for NMoveConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            faults: 1,
            wrong: 0,
            threshold: Threshold::Rule(Rule::HonestMajority),
            cutoff_rounds: 50,
            versions: None,
            failure: Vec::new(),
            clusters: Vec::new(),
            cost: CostConfig::default(),
            update_share: 0.0,
            recording: false,
        }
    }
}

impl ImportExport for NMoveConfig {}

impl NMoveConfig {
    /// Validators keep the records [`Engine::report`] needs, which grow with every transaction.
    pub fn recording(self) -> Self {
        Self {
            recording: true,
            ..self
        }
    }

    /// The stake a result needs, unless a rule's arithmetic overflows.
    pub fn threshold(&self) -> Option<Stake> {
        let (faults, wrong) = (self.faults, self.wrong);
        match self.threshold {
            Threshold::T(stake) => Some(stake),
            Threshold::Rule(Rule::SimpleMajority) => faults.checked_add(wrong)?.checked_add(1),
            Threshold::Rule(Rule::HonestMajority) => {
                faults.checked_mul(2)?.checked_add(wrong)?.checked_add(1)
            }
        }
    }

    /// Checks what doesn't depend on the committee; [`Engine::processor`] checks the rest.
    pub fn validate(&self) -> Result<()> {
        let probabilities = self.failure.iter();
        let clusters = self.clusters.iter().map(|cluster| &cluster.probability);
        ensure!(
            probabilities
                .chain(clusters)
                .chain([&self.update_share])
                .all(|probability| (0.0..=1.0).contains(probability)),
            "probabilities and shares lie in [0, 1]"
        );
        match self.versions {
            Some(versions) => self.validate_versions(versions),
            None => Ok(()),
        }
    }

    fn validate_versions(&self, versions: usize) -> Result<()> {
        ensure!(versions > 0, "a run needs a version");
        ensure!(
            self.failure.len() <= versions,
            "more failure probabilities than the {versions} versions"
        );
        let mut named = self.clusters.iter().flat_map(|cluster| &cluster.versions);
        ensure!(
            named.all(|version| *version < versions),
            "clusters name versions beyond the {versions} versions"
        );
        Ok(())
    }

    fn synthetic(&self, versions: usize) -> SyntheticConfig {
        let mut failure = self.failure.clone();
        failure.resize(versions, 0.0);
        let clusters = self.clusters.iter().map(|cluster| Cluster {
            versions: cluster.versions.iter().copied().map(VersionId).collect(),
            probability: cluster.probability,
            transactions: None,
        });
        SyntheticConfig {
            seed: self.seed,
            failure,
            clusters: clusters.collect(),
        }
    }
}

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
        let processor = NVersionProcessor::new(
            authority,
            engines,
            reference,
            assignment,
            threshold,
            self.cutoff_rounds,
            public_config,
        )?
        .with_cost(self.cost.model());
        Ok(match self.recording {
            true => processor.with_records(),
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

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Threshold {
    T(Stake),
    Rule(Rule),
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Rule {
    /// For n = 2f + 2W + 1.
    SimpleMajority,
    /// For n = 3f + 2W + 1.
    HonestMajority,
}

/// Versions wrong together, with the same wrong result, by index.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub versions: Vec<usize>,
    pub probability: f64,
}

/// [`CostModel`] in suite units.
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct CostConfig {
    pub per_execution_us: u64,
    pub per_gas_ns: u64,
}

impl CostConfig {
    fn model(&self) -> CostModel {
        CostModel {
            per_execution: Duration::from_micros(self.per_execution_us),
            per_gas: Duration::from_nanos(self.per_gas_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use dag::authority::Authority;
    use replica::config::PublicReplicaConfig;
    use validator::engine::Engine;

    use super::NMoveConfig;

    fn config(yaml: &str) -> NMoveConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn thresholds_take_a_stake_or_a_rule() {
        let threshold =
            |form| config(&format!("{{faults: 2, wrong: 1, threshold: {form}}}")).threshold();
        assert_eq!(threshold("{t: 3}"), Some(3));
        assert_eq!(threshold("{rule: simple_majority}"), Some(4));
        assert_eq!(threshold("{rule: honest_majority}"), Some(6));
        // Overflowing rules have no threshold, rather than a wrapped one.
        let honest = config("{faults: 9223372036854775808}");
        assert_eq!(honest.threshold(), None);
        let simple = config("{faults: 18446744073709551615, threshold: {rule: simple_majority}}");
        assert_eq!(simple.threshold(), None);
    }

    #[test]
    fn validation_splits_at_the_committee() {
        config("{versions: 4, failure: [0.1], clusters: [{versions: [3], probability: 1.0}]}")
            .validate()
            .unwrap();
        for invalid in [
            "{failure: [1.5]}",
            "{update_share: -0.1}",
            "{clusters: [{versions: [0], probability: 2.0}]}",
            "{versions: 0}",
            "{versions: 2, failure: [0.0, 0.0, 0.0]}",
            "{versions: 2, clusters: [{versions: [2], probability: 1.0}]}",
        ] {
            assert!(config(invalid).validate().is_err(), "{invalid} validates");
        }

        // What depends on the committee fails when building a processor for it.
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let authority = Authority::from(0usize);
        for fit in ["{}", "{threshold: {t: 4}}"] {
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
        ] {
            let processor = config(unfit).processor(authority, &public_config);
            assert!(processor.is_err(), "{unfit} fits 4 validators");
        }
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
