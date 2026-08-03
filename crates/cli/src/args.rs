// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::IpAddr, path::PathBuf};

use clap::Parser;
use dag::authority::Authority;
use tracing_subscriber::filter::LevelFilter;

/// Blockchain validator: a mysticeti consensus replica composed with execution and
/// self-certifying checkpoints.
#[derive(Parser)]
#[command(author, version, propagate_version = true)]
pub struct Args {
    /// Log level (trace, debug, info, warn, error). Overrides the per-command default.
    /// RUST_LOG env var takes precedence over this.
    #[arg(long, global = true)]
    pub log_level: Option<LevelFilter>,

    /// Write logs to this file instead of stderr.
    #[arg(long, global = true, value_name = "FILE")]
    pub log_file: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Parser)]
pub enum Command {
    /// Generate test genesis files: one public replica config (identities, stakes, and
    /// parameters) plus a private config per validator (keys and storage paths). Keys are
    /// written in plaintext.
    TestGenesis(TestGenesisArgs),

    /// Run a single validator from config files.
    Run(RunArgs),
}

#[derive(clap::Args)]
pub struct TestGenesisArgs {
    /// IP addresses of all validators.
    #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(3..))]
    pub ips: Vec<IpAddr>,
    /// Working directory where files will be generated.
    #[arg(long, value_name = "DIR", default_value = "genesis")]
    pub working_directory: PathBuf,
    /// Path to custom replica parameters (YAML). Uses defaults if omitted.
    #[arg(long, value_name = "FILE")]
    pub replica_parameters_path: Option<PathBuf>,
}

#[derive(clap::Args)]
pub struct RunArgs {
    /// Authority index of this node.
    #[arg(long, value_name = "INT")]
    pub authority: Authority,
    /// Path to the public replica config file (YAML: identities, stakes, and parameters).
    #[arg(long, value_name = "FILE")]
    pub public_config_path: String,
    /// Path to the private replica config file (YAML, includes keys).
    #[arg(long, value_name = "FILE")]
    pub private_config_path: String,
    /// Path to the load generator config file (YAML). Omit to run without the built-in load
    /// generator.
    #[arg(long, value_name = "FILE")]
    pub load_generator_config_path: Option<String>,
}
