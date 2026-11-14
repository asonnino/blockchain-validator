// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Embeds the provenance simulated runs record: the commit and the mysticeti rev.

use std::{fs, path::Path, process::Command};

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets the manifest dir");
    let root = Path::new(&manifest).join("../..");
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
    let workspace = fs::read_to_string(root.join("Cargo.toml")).unwrap_or_default();
    let mysticeti = workspace
        .lines()
        .find(|line| line.starts_with("dag = "))
        .and_then(|line| line.split("rev = \"").nth(1))
        .and_then(|rev| rev.split('"').next())
        .map(str::to_owned);
    let unknown = || "unknown".to_owned();
    println!(
        "cargo:rustc-env=BLOCKCHAIN_VALIDATOR_COMMIT={}",
        commit.unwrap_or_else(unknown)
    );
    println!(
        "cargo:rustc-env=MYSTICETI_REV={}",
        mysticeti.unwrap_or_else(unknown)
    );

    let git = root.join(".git");
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
    println!("cargo:rerun-if-changed={}", git.join("HEAD").display());
    println!(
        "cargo:rerun-if-changed={}",
        git.join("packed-refs").display()
    );
    let head = fs::read_to_string(git.join("HEAD")).unwrap_or_default();
    if let Some(reference) = head.strip_prefix("ref: ") {
        println!(
            "cargo:rerun-if-changed={}",
            git.join(reference.trim()).display()
        );
    }
}
