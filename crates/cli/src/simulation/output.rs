// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A run's outputs, in the layout pinned in MystenLabs/n-version-move#17.

use std::{fs, path::PathBuf};

use eyre::{Context, Result};
use serde::Serialize;
use serde_json::{Map, Value};
use validator::{engine::Engine, processor::CommitProcessor};

use crate::simulation::{
    config::{self, SimulationConfig},
    runner::Run,
};

/// A run's output directory.
pub struct RunDirectory {
    path: PathBuf,
}

impl RunDirectory {
    pub fn create(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(path.join("metrics"))
            .wrap_err_with(|| format!("Failed to create {}", path.display()))?;
        Ok(Self { path })
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

    use super::{Percentiles, Summary};

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
}
