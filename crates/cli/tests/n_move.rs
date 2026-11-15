// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `validator simulate` with the n-move engine: its records agree with its summary, its baseline
//! runs the same workload, and unfit settings fail before anything runs.

use std::{
    collections::HashMap,
    fs,
    path::Path,
    process::{Command, ExitStatus},
};

use serde_json::Value;

/// `correct` costs 1 ms per execution; `mixed` clusters give every outcome: three versions
/// agreeing on a wrong result finalize it, two against two reject the transaction.
const SUITE: &str = "
- {name: correct, duration_secs: 2, load: {load: 10}, engine: n-move,
    engine_config: {cost: {per_execution_us: 1000}}}
- {name: mixed, duration_secs: 2, load: {load: 10}, engine: n-move,
    engine_config: {update_share: 0.5, clusters: [
        {versions: [0, 1, 2], probability: 0.3}, {versions: [0, 1], probability: 0.5}]}}
";

fn simulate(suite: &str, dir: &Path) -> ExitStatus {
    let path = dir.join("suite.yaml");
    fs::write(&path, suite).unwrap();
    Command::new(env!("CARGO_BIN_EXE_validator"))
        .arg("simulate")
        .arg("--config-path")
        .arg(&path)
        .arg("--output-dir")
        .arg(dir.join("out"))
        .status()
        .unwrap()
}

fn json(path: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn lines(path: &Path) -> Vec<Value> {
    let text = fs::read_to_string(path).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The value of `metric` in a Prometheus text snapshot.
fn metric(text: &str, metric: &str) -> f64 {
    let line = text.lines().find(|line| line.starts_with(metric)).unwrap();
    line.split_whitespace().last().unwrap().parse().unwrap()
}

#[test]
fn records_agree_with_their_summary() {
    let dir = tempfile::tempdir().unwrap();
    assert!(simulate(SUITE, dir.path()).success());
    let output = dir.path().join("out");

    for run in ["correct", "mixed"] {
        let run = output.join(run);
        let summary = json(&run.join("summary.json"));
        let count = |section: &str, name: &str| summary[section][name].as_u64().unwrap();
        let transactions = summary["transactions"].as_u64().unwrap();
        let outcomes = ["final_correct", "final_wrong", "rejected"];
        assert!(transactions > 0);

        let records = lines(&run.join("transactions.jsonl"));
        assert_eq!(records.len() as u64, 4 * transactions);
        let own = || records.iter().filter(|record| record["authority"] == 0);
        let with = |field: &str, value: &str| own().filter(|r| r[field] == value).count() as u64;
        for outcome in outcomes {
            assert_eq!(count("outcomes", outcome), with("outcome", outcome));
        }
        for reason in ["impossible", "cutoff", "earlier_transaction"] {
            assert_eq!(count("rejections", reason), with("reason", reason));
        }
        // Every validator decides every transaction the same way.
        let decisions: HashMap<_, _> = own()
            .map(|r| {
                (
                    (r["commit"].to_string(), r["index"].to_string()),
                    &r["outcome"],
                )
            })
            .collect();
        for record in &records {
            assert_eq!(record["reason"].is_null(), record["outcome"] != "rejected");
            let position = (record["commit"].to_string(), record["index"].to_string());
            assert_eq!(decisions[&position], &record["outcome"]);
        }
        let sum = |field: &str| -> u64 {
            let values = records.iter().map(|record| match &record[field] {
                Value::Bool(flag) => u64::from(*flag),
                value => value.as_u64().unwrap(),
            });
            values.sum()
        };
        assert_eq!(summary["repairs"], sum("repairs"));
        assert_eq!(summary["losses"]["lost"], sum("lost"));
        assert_eq!(summary["losses"]["lost_to_earlier"], sum("lost_to_earlier"));
        // The summary's percentiles and vote bytes come from the records.
        for stage in ["committed", "executed", "voted", "decided"] {
            let mut latencies: Vec<_> = records
                .iter()
                .filter_map(|record| {
                    let at = record[format!("{stage}_ms")].as_u64()?;
                    Some(at - record["submitted_ms"].as_u64().unwrap())
                })
                .collect();
            latencies.sort_unstable();
            let p50 = (!latencies.is_empty()).then(|| latencies[latencies.len().div_ceil(2) - 1]);
            assert_eq!(summary["latency_ms"][stage]["p50"].as_u64(), p50, "{stage}");
        }
        let blocks = lines(&run.join("blocks.jsonl"));
        let vote_bytes = blocks
            .iter()
            .map(|block| block["vote_bytes"].as_u64().unwrap());
        let largest = vote_bytes.max().unwrap();
        assert!(largest > 0);
        assert_eq!(summary["vote_bytes_per_block"]["max"], largest);

        // The baseline runs the same workload under the default processor.
        let baseline = json(&run.join("baseline").join("summary.json"));
        assert_eq!(baseline["transactions"], summary["transactions"]);
        // The processors feed the validator metrics that remote benchmarks read.
        for authority in 0..4 {
            let path = run.join("metrics").join(format!("{authority}.prom"));
            let text = fs::read_to_string(path).unwrap();
            assert!(metric(&text, "checkpoint_certification_latency_s_count") > 0.0);
        }

        let config: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(run.join("config.yaml")).unwrap()).unwrap();
        assert_eq!(
            config["engine_config"]["threshold"]["rule"],
            "honest_majority"
        );
    }

    let correct = json(&output.join("correct").join("summary.json"));
    assert_eq!(
        correct["outcomes"]["final_correct"],
        correct["transactions"]
    );
    // One 1 ms execution per transaction and validator, and nothing to repair.
    let transactions = correct["transactions"].as_u64().unwrap();
    for busy_ms in correct["busy_ms"].as_array().unwrap() {
        assert_eq!(busy_ms.as_u64().unwrap(), transactions);
    }
    let mixed = json(&output.join("mixed").join("summary.json"));
    assert!(mixed["outcomes"]["final_wrong"].as_u64().unwrap() > 0);
    assert!(mixed["rejections"]["impossible"].as_u64().unwrap() > 0);
    // Updates reach the workload: some executions read what an earlier decision changed.
    assert!(mixed["losses"]["lost_to_earlier"].as_u64().unwrap() > 0);
}

#[test]
fn unfit_settings_run_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let suite = "- {name: fine, duration_secs: 1}
- {engine: n-move, engine_config: {threshold: {t: 5}}}
";
    assert!(!simulate(suite, dir.path()).success());
    assert!(!dir.path().join("out").exists());
}
