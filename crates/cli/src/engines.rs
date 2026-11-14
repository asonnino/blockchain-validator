// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The engines `--engine` names, each built from its YAML parameters (`--engine-config-path`).

use std::path::Path;

use clap::ValueEnum;
use dag::{authority::Authority, config::ImportExport};
use eyre::{Context, Result, ensure};
use fake_engine::{FakeExecutor, FakeWorkload};
use replica::config::PublicReplicaConfig;
use serde::{Deserialize, Serialize};
use validator::{engine::Engine as ValidatorEngine, processor::SequentialProcessor};

#[derive(Clone, Copy, ValueEnum)]
pub enum Engine {
    /// Creates and updates opaque objects, ignoring the call arguments.
    Fake,
    /// Runs the bundled counter Move package through Sui's execution layer.
    Move,
}

impl Engine {
    /// The engine's config at `path`, validated and re-serialized.
    pub fn normalized_config(self, path: &Path) -> Result<String> {
        Ok(match self {
            Self::Fake => FakeConfig::load_or_default(Some(path))?.to_yaml(),
            Self::Move => MoveConfig::load_or_default(Some(path))?.to_yaml(),
        })
    }
}

/// An engine's parameters, as an `--engine-config-path` file holds them.
pub trait EngineConfig: ImportExport + Default {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    /// The config at `path`, or the defaults without one.
    fn load_or_default(path: Option<&Path>) -> Result<Self> {
        let config = match path {
            Some(path) => Self::load(path)
                .wrap_err_with(|| format!("Failed to load the engine config {}", path.display()))?,
            None => Self::default(),
        };
        config.validate()?;
        Ok(config)
    }
}

/// The fake engine's parameters.
#[derive(Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct FakeConfig {
    /// The share of transactions updating a recent creation rather than creating an object.
    pub update_share: f64,
}

impl ImportExport for FakeConfig {}

impl EngineConfig for FakeConfig {
    fn validate(&self) -> Result<()> {
        let share = self.update_share;
        ensure!(
            (0.0..=1.0).contains(&share),
            "update_share {share} is not in [0, 1]"
        );
        Ok(())
    }
}

impl ValidatorEngine for FakeConfig {
    type Processor = SequentialProcessor<FakeExecutor>;
    type Workload = FakeWorkload;

    fn processor(
        &self,
        _authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> Result<Self::Processor> {
        SequentialProcessor::new(FakeExecutor, public_config)
    }

    fn workload(&self) -> FakeWorkload {
        FakeWorkload::default().with_updates(self.update_share)
    }
}

/// The Move engine's parameters: none yet.
#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MoveConfig {}

impl ImportExport for MoveConfig {}

impl EngineConfig for MoveConfig {}

#[cfg(feature = "move")]
impl ValidatorEngine for MoveConfig {
    type Processor = SequentialProcessor<move_engine::MoveEngine>;
    type Workload = move_engine::CounterWorkload;

    fn processor(
        &self,
        _authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> Result<Self::Processor> {
        SequentialProcessor::new(move_engine::MoveEngine::counter(), public_config)
    }

    /// Derived from an engine of its own: every engine publishes the same genesis.
    fn workload(&self) -> Self::Workload {
        move_engine::CounterWorkload::new(&move_engine::MoveEngine::counter())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use execution::{transaction::AccessMode, workload::Workload};
    use rand::{SeedableRng, rngs::StdRng};
    use validator::engine::Engine as _;

    use super::{Engine, EngineConfig, FakeConfig};

    #[test]
    fn fake_configs_load_validated() {
        assert_eq!(FakeConfig::load_or_default(None).unwrap().update_share, 0.0);
        let dir = tempfile::tempdir().unwrap();
        let write = |yaml: &str| {
            let path = dir.path().join("engine.yaml");
            fs::write(&path, yaml).unwrap();
            path
        };
        let half = write("update_share: 0.5");
        assert_eq!(
            FakeConfig::load_or_default(Some(&half))
                .unwrap()
                .update_share,
            0.5
        );
        assert_eq!(
            Engine::Fake.normalized_config(&half).unwrap(),
            "update_share: 0.5\n"
        );

        // The share reaches the workload: with every transaction an update, the second one
        // updates the first's creation.
        let all = FakeConfig::load_or_default(Some(&write("update_share: 1"))).unwrap();
        let mut workload = all.workload();
        let mut rng = StdRng::seed_from_u64(0);
        let [created, updated] =
            [0, 1].map(|counter| workload.transaction(0, counter, 128, &mut rng));
        let [(id, AccessMode::WriteOnly)] = created.inputs() else {
            panic!("a creation first: {:?}", created.inputs());
        };
        assert_eq!(updated.inputs(), [(*id, AccessMode::ReadWrite)]);

        for invalid in ["update_share: 1.5", "update_share: -0.5", "updates: 0.5"] {
            let path = write(invalid);
            assert!(
                FakeConfig::load_or_default(Some(&path)).is_err(),
                "{invalid} loads"
            );
            assert!(
                Engine::Fake.normalized_config(&path).is_err(),
                "{invalid} normalizes"
            );
        }
    }
}
