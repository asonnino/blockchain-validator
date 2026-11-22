// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! n-move's config, as suites give it, and its conversions into what the processor runs.

use std::time::Duration;

use dag::{authority::Authority, committee::Stake, config::ImportExport};
use eyre::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    byzantine::{Byzantine, Selective, Strategy},
    digest::ResultDigest,
    versions::{Cluster, CostModel, SyntheticConfig, VersionId},
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
    /// Validators voting as Byzantine ones; none if absent.
    pub byzantine: Option<ByzantineConfig>,
    #[serde(skip)]
    pub(crate) recording: bool,
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
            byzantine: None,
            recording: false,
        }
    }
}

impl ImportExport for NMoveConfig {}

impl NMoveConfig {
    /// Validators keep the records [`Engine::report`] needs, which grow with every transaction.
    ///
    /// [`Engine::report`]: validator::engine::Engine::report
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
    ///
    /// [`Engine::processor`]: validator::engine::Engine::processor
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
        if let Some(byzantine) = &self.byzantine {
            byzantine.validate()?;
        }
        match self.versions {
            Some(versions) => self.validate_versions(versions),
            None => Ok(()),
        }
    }

    pub(crate) fn validate_versions(&self, versions: usize) -> Result<()> {
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

    pub(crate) fn synthetic(&self, versions: usize) -> SyntheticConfig {
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

/// Validators voting as Byzantine ones, by index, all with one strategy.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ByzantineConfig {
    pub authorities: Vec<usize>,
    /// `collude`, `withhold`, `{fixed: <label>}`, or `{late: <rounds>}`.
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub strategy: StrategyConfig,
    /// Byzantine on this share of transactions only; on every transaction if absent.
    pub selective: Option<SelectiveConfig>,
}

impl ByzantineConfig {
    fn validate(&self) -> Result<()> {
        let mut authorities = self.authorities.clone();
        authorities.sort_unstable();
        authorities.dedup();
        ensure!(
            authorities.len() == self.authorities.len(),
            "Byzantine validators repeat"
        );
        if let StrategyConfig::Late(rounds) = self.strategy {
            ensure!(rounds > 0, "late votes are at least a round late");
        }
        if let Some(selective) = self.selective {
            let share = selective.share;
            ensure!(
                (0.0..=1.0).contains(&share),
                "selective share {share} is not in [0, 1]"
            );
        }
        Ok(())
    }

    pub(crate) fn byzantine(&self) -> Byzantine {
        Byzantine {
            strategy: self.strategy.strategy(),
            selective: self.selective.map(|selective| Selective {
                share: selective.share,
                seed: selective.seed,
            }),
            authorities: self
                .authorities
                .iter()
                .copied()
                .map(Authority::from)
                .collect(),
        }
    }
}

/// [`Strategy`] in suite terms.
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StrategyConfig {
    Collude,
    Withhold,
    /// The [`ResultDigest::fixed`] digest with this label.
    Fixed(u64),
    /// Votes this many rounds late.
    Late(u64),
}

impl StrategyConfig {
    fn strategy(self) -> Strategy {
        match self {
            Self::Collude => Strategy::Collude,
            Self::Withhold => Strategy::Withhold,
            Self::Fixed(label) => Strategy::Fixed(ResultDigest::fixed(label)),
            Self::Late(rounds) => Strategy::Late { rounds },
        }
    }
}

/// [`Selective`] in suite terms.
#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct SelectiveConfig {
    pub share: f64,
    pub seed: u64,
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
    pub(crate) fn model(&self) -> CostModel {
        CostModel {
            per_execution: Duration::from_micros(self.per_execution_us),
            per_gas: Duration::from_nanos(self.per_gas_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use dag::authority::Authority;

    use super::NMoveConfig;
    use crate::byzantine::{Selective, Strategy};

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
    fn validation_checks_what_needs_no_committee() {
        config("{versions: 4, failure: [0.1], clusters: [{versions: [3], probability: 1.0}]}")
            .validate()
            .unwrap();
        let section = "{authorities: [3], strategy: {late: 3}, selective: {share: 0.5, seed: 1}}";
        let late = config(&format!("{{byzantine: {section}}}"));
        late.validate().unwrap();
        let byzantine = late.byzantine.unwrap().byzantine();
        assert_eq!(byzantine.strategy, Strategy::Late { rounds: 3 });
        let selective = Selective {
            share: 0.5,
            seed: 1,
        };
        assert_eq!(byzantine.selective, Some(selective));
        assert_eq!(byzantine.authorities, [Authority::from(3usize)]);
        for invalid in [
            "{failure: [1.5]}",
            "{update_share: -0.1}",
            "{clusters: [{versions: [0], probability: 2.0}]}",
            "{versions: 0}",
            "{versions: 2, failure: [0.0, 0.0, 0.0]}",
            "{versions: 2, clusters: [{versions: [2], probability: 1.0}]}",
            "{byzantine: {authorities: [1, 1], strategy: withhold}}",
            "{byzantine: {authorities: [0], strategy: {late: 0}}}",
            "{byzantine: {authorities: [0], strategy: collude, selective: {share: 1.5, seed: 0}}}",
        ] {
            assert!(config(invalid).validate().is_err(), "{invalid} validates");
        }
    }
}
