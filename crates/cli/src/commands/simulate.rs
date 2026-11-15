// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf};

use eyre::{Context, Result};
use tracing_subscriber::filter::LevelFilter;
use validator::engine::Engine as ValidatorEngine;

use crate::{
    args::SimulateArgs,
    engines::{Engine, EngineConfig, FakeConfig},
    simulation::{
        config::{self, SimulationConfig},
        output::{RunDirectory, SuiteDirectory},
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
    let directories = SuiteDirectory::create(output_dir)?.claim(&suite, &runs)?;

    let total = runs.len();
    for (index, (run, directory)) in runs.into_iter().zip(directories).enumerate() {
        let name = run.directory(index);
        println!("[{}/{total}] {name}", index + 1);
        tokio::task::spawn_blocking(move || simulate_run(&run, directory))
            .await
            .wrap_err("The simulation panicked")?
            .wrap_err_with(|| format!("Run {name} failed"))?;
    }
    Ok(())
}

/// Runs a valid `run` with its engine, and writes its outputs into `directory`.
fn simulate_run(run: &SimulationConfig, directory: RunDirectory) -> Result<()> {
    let engine_config = run.engine_config.clone();
    match run.engine {
        Engine::Fake => simulate_with(run, FakeConfig::parse(engine_config)?, directory),
        #[cfg(feature = "move")]
        Engine::Move => simulate_with(
            run,
            crate::engines::MoveConfig::parse(engine_config)?,
            directory,
        ),
        #[cfg(not(feature = "move"))]
        Engine::Move => unreachable!("validation rejects the move engine without the feature"),
    }
}

fn simulate_with<En>(run: &SimulationConfig, engine: En, directory: RunDirectory) -> Result<()>
where
    En: ValidatorEngine + Clone + Send + 'static,
{
    let runner = Runner::new(run.clone());
    let simulated = runner.run(engine.clone())?;
    let baseline = Baseline::of(engine.clone())
        .map(|baseline| runner.run(baseline))
        .transpose()?;

    directory.write_config(run)?;
    directory.write_metrics(&simulated)?;
    directory.write_summary(run, &engine, simulated)?;
    if let Some(baseline) = baseline {
        directory.write_baseline(&baseline)?;
    }
    Ok(())
}
