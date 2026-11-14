// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! `validator simulate` on small suites: the output layout, provenance, same-seed runs, and
//! validation before anything runs.

use std::{
    fs,
    path::Path,
    process::{Command, ExitStatus},
};

use serde_json::Value;
use validator::metrics::SUBMITTED_TRANSACTIONS;

/// `again` repeats `updates` under another name, and `reseeded` with another seed.
const SUITE: &str = "
- {name: plain, duration_secs: 2, load: {load: 10}}
- {name: updates, duration_secs: 2, load: {load: 10}, engine_config: {update_share: 0.5}}
- {name: again, duration_secs: 2, load: {load: 10}, engine_config: {update_share: 0.5}}
- {name: reseeded, seed: 1, duration_secs: 2, load: {load: 10}, engine_config: {update_share: 0.5}}
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

#[test]
fn suites_run_into_the_pinned_layout() {
    let dir = tempfile::tempdir().unwrap();
    assert!(simulate(SUITE, dir.path()).success());
    let output = dir.path().join("out");
    assert_eq!(
        fs::read_to_string(output.join("suite.yaml")).unwrap(),
        SUITE
    );

    for run in ["plain", "updates", "again", "reseeded"] {
        let run = output.join(run);
        let summary = json(&run.join("summary.json"));
        assert!(summary["duration_ms"].as_u64().unwrap() >= 2000);
        assert!(
            !run.join("baseline").exists(),
            "the fake engine has no baseline"
        );
        // Every submitted transaction executed, once.
        let submitted: f64 = (0..4)
            .map(|authority| {
                let metrics = run.join("metrics").join(format!("{authority}.prom"));
                let text = fs::read_to_string(metrics).unwrap();
                let line = text
                    .lines()
                    .find(|line| line.starts_with(SUBMITTED_TRANSACTIONS));
                let value = line.unwrap().split_whitespace().last().unwrap();
                value.parse::<f64>().unwrap()
            })
            .sum();
        assert!(submitted > 0.0);
        assert_eq!(summary["transactions"].as_f64().unwrap(), submitted);

        let config: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(run.join("config.yaml")).unwrap()).unwrap();
        assert_eq!(config["committee_size"], 4);
        assert!(config["engine_config"]["update_share"].is_f64());
        let provenance = &summary["provenance"];
        assert_eq!(
            &serde_json::to_value(&config["provenance"]).unwrap(),
            provenance
        );
        let rev = provenance["mysticeti_rev"].as_str().unwrap();
        assert!(rev.len() == 40 && rev.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // The same seed and config give the same run, and another seed another run.
    let anonymous = |run| {
        let mut summary = json(&output.join(run).join("summary.json"));
        let fields = summary.as_object_mut().unwrap();
        fields.remove("name");
        fields.remove("seed");
        summary
    };
    assert_eq!(anonymous("updates"), anonymous("again"));
    assert_ne!(anonymous("updates"), anonymous("reseeded"));
}

#[test]
fn invalid_suites_run_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let suite = "- {name: fine, duration_secs: 1}\n- {duration_secs: 0}\n";
    assert!(!simulate(suite, dir.path()).success());
    assert!(!dir.path().join("out").exists());
}
