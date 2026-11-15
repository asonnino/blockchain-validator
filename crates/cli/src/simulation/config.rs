// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The suite format: a YAML list of runs, each falling back to [`SimulationConfig::default`] for
//! the fields it omits. Enums are single-key maps, as in mysticeti's suites:
//! `latency: {uniform: {range_ms: {start: 50, end: 100}}}`.

use std::{collections::HashSet, time::Duration};

use eyre::{Context, Result, bail, ensure};
use replica::config::PublicReplicaConfig;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use simulator::{ExponentialJitter, LatencyModel};
use validator::generator::LoadGeneratorConfig;

use crate::engines::Engine;

/// Half a generator tick (100 ms): no tick then lands on the stop, a whole number of seconds in.
const GENERATOR_OFFSET: Duration = Duration::from_millis(50);

pub fn parse(suite: &str) -> Result<Vec<SimulationConfig>> {
    let deserializer = serde_yaml::Deserializer::from_str(suite);
    serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
        .wrap_err("Failed to parse the suite")
}

/// A suite of one run with every default.
pub fn default_suite() -> Result<String> {
    to_yaml(&[SimulationConfig::default().resolved()?])
}

/// `value` in the suite format.
pub fn to_yaml<T: Serialize>(value: &T) -> Result<String> {
    let mut yaml = Vec::new();
    let mut serializer = serde_yaml::Serializer::new(&mut yaml);
    serde_yaml::with::singleton_map_recursive::serialize(value, &mut serializer)?;
    Ok(String::from_utf8(yaml)?)
}

/// Checks every run, and that no two runs share an output directory.
pub fn validate(runs: &[SimulationConfig]) -> Result<()> {
    let mut directories = HashSet::new();
    for (index, run) in runs.iter().enumerate() {
        run.validate()
            .wrap_err_with(|| format!("Run {index} is invalid"))?;
        let directory = run.directory(index);
        if !directories.insert(directory.clone()) {
            bail!("two runs write to the directory {directory}");
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct SimulationConfig {
    /// Names the run's output directory, which is its index otherwise.
    pub name: Option<String>,
    /// Seeds the simulator, and through it the network and the load generators.
    pub seed: u64,
    /// Simulated time the load generators submit for.
    pub duration_secs: u64,
    /// Validators, one stake each.
    pub committee_size: usize,
    /// The simulator's link latency model.
    pub latency: LatencyModel,
    pub jitter: Option<ExponentialJitter>,
    pub load: LoadConfig,
    pub engine: Engine,
    /// The engine's parameters, as `--engine-config-path` holds them; its defaults if absent.
    pub engine_config: Option<Value>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            name: None,
            seed: 0,
            duration_secs: 10,
            committee_size: 4,
            latency: LatencyModel::default(),
            jitter: None,
            load: LoadConfig::default(),
            engine: Engine::default(),
            engine_config: None,
        }
    }
}

impl SimulationConfig {
    pub fn directory(&self, index: usize) -> String {
        self.name.clone().unwrap_or_else(|| index.to_string())
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.name {
            let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_');
            ensure!(
                !name.is_empty() && name.chars().all(allowed),
                "run names use letters, digits, '-', and '_'"
            );
        }
        // Mysticeti's committees hold at most 128 authorities (`Committee::new` asserts it).
        ensure!(
            (1..=128).contains(&self.committee_size),
            "committees have 1 to 128 validators"
        );
        let public_config = PublicReplicaConfig::new_for_tests(self.committee_size);
        let committee = public_config.committee();
        public_config.parameters.consensus.to_protocol(&committee)?;
        ensure!(self.duration_secs > 0, "a run submits for some time");
        self.latency.validate()?;
        let engine_config = self.engine_config.clone();
        self.engine
            .validate_run(engine_config, &self.load_generator(), &public_config)
    }

    /// Every validator's load generator.
    pub fn load_generator(&self) -> LoadGeneratorConfig {
        LoadGeneratorConfig {
            load: self.load.load,
            transaction_size: self.load.transaction_size,
            initial_delay: GENERATOR_OFFSET,
        }
    }

    /// The run with its engine config filled in.
    pub fn resolved(&self) -> Result<Self> {
        let engine_config = self.engine.resolved_config(self.engine_config.clone())?;
        Ok(Self {
            engine_config: Some(engine_config),
            ..self.clone()
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct LoadConfig {
    /// Transactions per second, per validator.
    pub load: usize,
    /// Bytes per transaction.
    pub transaction_size: usize,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            load: 100,
            transaction_size: 512,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SimulationConfig, parse, to_yaml, validate};

    fn suite(yaml: &str) -> Vec<SimulationConfig> {
        parse(yaml).unwrap()
    }

    #[test]
    fn suites_round_trip_through_the_suite_format() {
        let runs = suite(
            "- {name: a, seed: 3, jitter: {mean_ms: 5, cap_ms: 50}, engine: fake,
                engine_config: {update_share: 0.5},
                latency: {uniform: {range_ms: {start: 10, end: 20}}}}",
        );
        let yaml = to_yaml(&runs).unwrap();
        assert_eq!(to_yaml(&parse(&yaml).unwrap()).unwrap(), yaml);
        let default = super::default_suite().unwrap();
        assert_eq!(to_yaml(&parse(&default).unwrap()).unwrap(), default);
    }

    #[test]
    fn validation_rejects_invalid_suites() {
        validate(&suite("- {name: a}\n- {}")).unwrap();
        for invalid in [
            "- {duration_secs: 0}",
            "- {load: {load: 0}}",
            "- {committee_size: 0}",
            "- {committee_size: 1}",
            "- {committee_size: 129}",
            "- {engine_config: {update_share: 2}}",
            "- {engine_config: {updates: 1}}",
            "- {load: {transaction_size: 1}}",
            "- {load: {transaction_size: 13}}",
            "- {load: {transaction_size: 2000000}}",
            "- {latency: {uniform: {range_ms: {start: 100, end: 50}}}}",
            "- {name: a}\n- {name: a}",
            "- {name: '1'}\n- {}",
            "- {name: ../escape}",
            "- {name: ''}",
        ] {
            assert!(validate(&suite(invalid)).is_err(), "{invalid} validates");
        }
        assert!(parse("- {engines: fake}").is_err());
        #[cfg(not(feature = "move"))]
        assert!(validate(&suite("- {engine: move}")).is_err());
    }
}
