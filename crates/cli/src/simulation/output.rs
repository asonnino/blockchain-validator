// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A run's outputs, in the layout pinned in MystenLabs/n-version-move#17.

use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use eyre::{Context, Result, ensure};
use serde::Serialize;
use serde_json::{Map, Value};
use validator::{engine::Engine, processor::CommitProcessor};

use crate::simulation::{
    config::{self, SimulationConfig},
    runner::Run,
};

/// A suite's output directory: its copy of the suite, and one directory per run.
pub struct SuiteDirectory {
    path: PathBuf,
}

impl SuiteDirectory {
    pub fn create(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path)
            .wrap_err_with(|| format!("Failed to create {}", path.display()))?;
        Ok(Self { path })
    }

    /// Claims the directory for `suite`, whose runs are `runs`: writes the suite's copy unless the
    /// same suite is already there (as when the directory holds it), then reserves every run's
    /// directory. Both claims are atomic, so a concurrent invocation fails rather than mixing its
    /// outputs in, and a failed claim undoes the others.
    pub fn claim(&self, suite: &str, runs: &[SimulationConfig]) -> Result<Vec<RunDirectory>> {
        let copy = self.path.join("suite.yaml");
        let created = match OpenOptions::new().write(true).create_new(true).open(&copy) {
            Ok(mut file) => {
                file.write_all(suite.as_bytes())?;
                true
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = fs::read_to_string(&copy)?;
                ensure!(
                    existing == suite,
                    "{} holds another suite: choose another output directory",
                    copy.display()
                );
                false
            }
            Err(error) => return Err(error).wrap_err("Failed to copy the suite"),
        };
        let mut directories = Vec::with_capacity(runs.len());
        for (index, run) in runs.iter().enumerate() {
            match RunDirectory::reserve(self.path.join(run.directory(index))) {
                Ok(directory) => directories.push(directory),
                Err(error) => {
                    for directory in directories {
                        directory.release()?;
                    }
                    if created {
                        fs::remove_file(&copy)?;
                    }
                    return Err(error.wrap_err("choose another output directory"));
                }
            }
        }
        Ok(directories)
    }
}

/// A run's output directory.
pub struct RunDirectory {
    path: PathBuf,
}

impl RunDirectory {
    /// Creates the directory, which must not exist yet: creating it atomically keeps a rerun, or a
    /// concurrent one, from mixing its files with an earlier run's.
    pub fn reserve(path: PathBuf) -> Result<Self> {
        fs::create_dir(&path)
            .and_then(|()| fs::create_dir(path.join("metrics")))
            .wrap_err_with(|| format!("Failed to create {}", path.display()))?;
        Ok(Self { path })
    }

    /// Removes the directory before anything wrote to it.
    fn release(self) -> Result<()> {
        fs::remove_dir_all(&self.path)
            .wrap_err_with(|| format!("Failed to remove {}", self.path.display()))
    }

    /// `config.yaml`: the run with every default filled in, and its provenance.
    pub fn write_config(&self, config: &SimulationConfig) -> Result<()> {
        #[derive(Serialize)]
        struct Resolved<'a> {
            #[serde(flatten)]
            config: &'a SimulationConfig,
            provenance: Provenance,
        }
        let resolved = Resolved {
            config: &config.resolved()?,
            provenance: Provenance::CURRENT,
        };
        self.write("config.yaml", &config::to_yaml(&resolved)?)
    }

    /// `metrics/<authority>.prom`.
    pub fn write_metrics<P>(&self, run: &Run<P>) -> Result<()> {
        for (authority, text) in run.metrics.iter().enumerate() {
            self.write(&format!("metrics/{authority}.prom"), text)?;
        }
        Ok(())
    }

    /// `summary.json`, extended by the engine's report on the run.
    pub fn write_summary<En: Engine>(
        &self,
        config: &SimulationConfig,
        engine: &En,
        run: Run<En::Processor>,
    ) -> Result<()> {
        let summary = Summary {
            name: config.name.as_deref(),
            seed: config.seed,
            provenance: Provenance::CURRENT,
            parameters: Parameters {
                committee_size: config.committee_size,
            },
            measurements: Measurements::new(&run),
        };
        let Value::Object(mut fields) = serde_json::to_value(summary)? else {
            unreachable!("a summary is a JSON object");
        };
        Summary::merge(
            &mut fields,
            engine.report(run.processors, run.start, &self.path)?,
        );
        self.write_json("summary.json", &fields)
    }

    /// `baseline/summary.json`.
    pub fn write_baseline<P: CommitProcessor>(&self, baseline: &Run<P>) -> Result<()> {
        fs::create_dir_all(self.path.join("baseline"))?;
        self.write_json("baseline/summary.json", &Measurements::new(baseline))
    }

    fn write_json(&self, file: &str, value: &impl Serialize) -> Result<()> {
        self.write(file, &serde_json::to_string_pretty(value)?)
    }

    fn write(&self, file: &str, contents: &str) -> Result<()> {
        let path = self.path.join(file);
        fs::write(&path, contents).wrap_err_with(|| format!("Failed to write {}", path.display()))
    }
}

/// The commit and the mysticeti rev the binary was built from.
#[derive(Serialize)]
struct Provenance {
    blockchain_validator_commit: &'static str,
    mysticeti_rev: &'static str,
}

impl Provenance {
    const CURRENT: Self = Self {
        blockchain_validator_commit: env!("BLOCKCHAIN_VALIDATOR_COMMIT"),
        mysticeti_rev: env!("MYSTICETI_REV"),
    };
}

#[derive(Serialize)]
struct Summary<'a> {
    name: Option<&'a str>,
    seed: u64,
    provenance: Provenance,
    parameters: Parameters,
    #[serde(flatten)]
    measurements: Measurements,
}

impl Summary<'_> {
    /// Adds an engine's report to the summary's `fields`: objects merge one level deep, anything
    /// else replaces the field.
    fn merge(fields: &mut Map<String, Value>, report: Map<String, Value>) {
        for (key, value) in report {
            match (fields.get_mut(&key), value) {
                (Some(Value::Object(fields)), Value::Object(own)) => fields.extend(own),
                (_, value) => {
                    fields.insert(key, value);
                }
            }
        }
    }
}

#[derive(Serialize)]
struct Parameters {
    committee_size: usize,
}

/// The transactions one validator executed, their throughput and their latency over every
/// validator, and each validator's busy time.
#[derive(Serialize)]
struct Measurements {
    transactions: u64,
    duration_ms: u64,
    throughput_tps: f64,
    latency_ms: Latencies,
    busy_ms: Vec<u64>,
}

impl Measurements {
    fn new<P: CommitProcessor>(run: &Run<P>) -> Self {
        let transactions = run.processors[0].executed();
        let last_ms = run.timings.iter().map(|timings| timings.last_ms).max();
        let duration_ms = last_ms
            .unwrap_or_default()
            .saturating_sub(run.start.as_millis() as u64);
        let throughput_tps = match duration_ms {
            0 => 0.0,
            duration_ms => transactions as f64 * 1000.0 / duration_ms as f64,
        };
        let latencies = run.timings.iter().flat_map(|timings| &timings.latencies_ms);
        Self {
            transactions,
            duration_ms,
            throughput_tps,
            latency_ms: Latencies {
                executed: Percentiles::new(latencies.copied().collect()),
            },
            busy_ms: run
                .timings
                .iter()
                .map(|timings| timings.busy.as_millis() as u64)
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct Latencies {
    executed: Percentiles,
}

/// By nearest rank; none without samples.
#[derive(Serialize, PartialEq, Debug)]
struct Percentiles {
    p50: Option<u64>,
    p90: Option<u64>,
    p99: Option<u64>,
}

impl Percentiles {
    fn new(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        let rank = |p: f64| {
            let rank = (p * samples.len() as f64).ceil() as usize;
            samples.get(rank.max(1) - 1).copied()
        };
        Self {
            p50: rank(0.5),
            p90: rank(0.9),
            p99: rank(0.99),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use std::fs;

    use super::{Percentiles, SuiteDirectory, Summary};
    use crate::simulation::config::SimulationConfig;

    #[test]
    fn percentiles_take_the_nearest_rank() {
        let none = Percentiles {
            p50: None,
            p90: None,
            p99: None,
        };
        assert_eq!(Percentiles::new(Vec::new()), none);
        let ten = Percentiles {
            p50: Some(5),
            p90: Some(9),
            p99: Some(10),
        };
        assert_eq!(Percentiles::new((1..=10).rev().collect()), ten);
    }

    #[test]
    fn reports_merge_objects_one_level_deep() {
        let object = |value: Value| match value {
            Value::Object(fields) => fields,
            _ => unreachable!(),
        };
        let mut fields = object(json!({
            "parameters": {"committee_size": 4},
            "transactions": 10,
            "latency_ms": {"executed": {"p50": 1}},
        }));
        let report = object(json!({
            "parameters": {"threshold": 3},
            "transactions": 8,
            "latency_ms": {"executed": {"p50": 2}, "decided": {"p50": 3}},
        }));
        Summary::merge(&mut fields, report);
        assert_eq!(
            Value::Object(fields),
            json!({
                "parameters": {"committee_size": 4, "threshold": 3},
                "transactions": 8,
                "latency_ms": {"executed": {"p50": 2}, "decided": {"p50": 3}},
            })
        );
    }

    #[test]
    fn failed_claims_undo_themselves() {
        let dir = tempfile::tempdir().unwrap();
        let named = |name: &str| SimulationConfig {
            name: Some(name.into()),
            ..SimulationConfig::default()
        };
        let runs = [named("a"), named("b")];
        fs::create_dir(dir.path().join("b")).unwrap();
        let suite = SuiteDirectory::create(dir.path().to_path_buf()).unwrap();
        assert!(suite.claim("runs a and b", &runs).is_err());
        assert!(!dir.path().join("suite.yaml").exists());
        assert!(!dir.path().join("a").exists());

        fs::remove_dir(dir.path().join("b")).unwrap();
        assert_eq!(suite.claim("runs a and b", &runs).unwrap().len(), 2);
        let content = fs::read_to_string(dir.path().join("suite.yaml")).unwrap();
        assert_eq!(content, "runs a and b");
    }
}
