// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf};

use eyre::{Context, Result, ensure};
use tracing_subscriber::filter::LevelFilter;
use validator::engine::Engine as ValidatorEngine;

use crate::{
    args::SimulateArgs,
    engines::{Engine, EngineConfig, FakeConfig},
    simulation::{
        config::{self, SimulationConfig},
        output::RunDirectory,
        runner::{Baseline, Runner},
    },
    tracing::ValidatorTracing,
};

pub async fn simulate(
    args: SimulateArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let SimulateArgs {
        config_path,
        output_dir,
        dump_config,
    } = args;
    if dump_config {
        print!("{}", config::default_suite()?);
        return Ok(());
    }
    let (Some(config_path), Some(output_dir)) = (config_path, output_dir) else {
        unreachable!("clap requires --config-path and --output-dir without --dump-config");
    };
    // The simulator's tracing is verbose: warnings only by default.
    let _guard = ValidatorTracing::new(log_level.unwrap_or(LevelFilter::WARN))
        .with_log_file(log_file)
        .setup()?;

    let suite = fs::read_to_string(&config_path)
        .wrap_err_with(|| format!("Failed to read the suite {}", config_path.display()))?;
    let runs = config::parse(&suite)?;
    config::validate(&runs)?;
    // A rerun would leave the earlier run's files (a larger committee's metrics) beside its own.
    for (index, run) in runs.iter().enumerate() {
        let dir = output_dir.join(run.directory(index));
        ensure!(
            !dir.exists(),
            "{} exists: choose another output directory",
            dir.display()
        );
    }
    fs::create_dir_all(&output_dir)
        .wrap_err_with(|| format!("Failed to create {}", output_dir.display()))?;
    // Written from the text already read: the output directory may hold the suite itself.
    fs::write(output_dir.join("suite.yaml"), &suite).wrap_err("Failed to copy the suite")?;
    let total = runs.len();
    for (index, run) in runs.into_iter().enumerate() {
        let directory = run.directory(index);
        println!("[{}/{total}] {directory}", index + 1);
        let dir = output_dir.join(&directory);
        tokio::task::spawn_blocking(move || simulate_run(&run, dir))
            .await
            .wrap_err("The simulation panicked")?
            .wrap_err_with(|| format!("Run {directory} failed"))?;
    }
    Ok(())
}

/// Runs a valid `run` with its engine, and writes its outputs into `dir`.
fn simulate_run(run: &SimulationConfig, dir: PathBuf) -> Result<()> {
    let engine_config = run.engine_config.clone();
    match run.engine {
        Engine::Fake => simulate_with(run, FakeConfig::parse(engine_config)?, dir),
        #[cfg(feature = "move")]
        Engine::Move => simulate_with(run, crate::engines::MoveConfig::parse(engine_config)?, dir),
        #[cfg(not(feature = "move"))]
        Engine::Move => unreachable!("validation rejects the move engine without the feature"),
    }
}

fn simulate_with<En>(run: &SimulationConfig, engine: En, dir: PathBuf) -> Result<()>
where
    En: ValidatorEngine + Clone + Send + 'static,
{
    let runner = Runner::new(run.clone());
    let simulated = runner.run(engine.clone())?;
    let baseline = Baseline::of(engine.clone())
        .map(|baseline| runner.run(baseline))
        .transpose()?;

    let directory = RunDirectory::create(dir)?;
    directory.write_config(run)?;
    directory.write_metrics(&simulated)?;
    directory.write_summary(run, &engine, simulated)?;
    if let Some(baseline) = baseline {
        directory.write_baseline(&baseline)?;
    }
    Ok(())
}
