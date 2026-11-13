// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf};

use dag::{authority::Authority, config::ImportExport, context::TokioCtx};
use eyre::{Result, WrapErr, eyre};
use replica::{
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    prometheus::{MetricsRegistry, PrometheusServer},
};
use tracing_subscriber::filter::LevelFilter;
use validator::{
    engine::Engine as ValidatorEngine, generator::LoadGeneratorConfig, validator::ValidatorBuilder,
};

use crate::{
    args::RunArgs,
    engines::{Engine, EngineConfig, FakeConfig},
    tracing::ValidatorTracing,
};

pub async fn run(
    args: RunArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let _guard = match log_level {
        Some(level) => ValidatorTracing::new(level),
        None => ValidatorTracing::default(),
    }
    .with_log_file(log_file)
    .setup()?;

    let RunArgs {
        authority,
        public_config_path,
        private_config_path,
        load_generator_config_path,
        engine,
        engine_config_path,
    } = args;
    tracing::info!("Starting validator {authority}");

    let public_config = PublicReplicaConfig::load(&public_config_path)?;
    let private_config = PrivateReplicaConfig::load(&private_config_path)?;
    let load_generator_config = load_generator_config_path
        .map(|path| LoadGeneratorConfig::load(&path))
        .transpose()?;

    let config_path = engine_config_path.as_deref();
    match engine {
        Engine::Fake => {
            run_with(
                FakeConfig::load_or_default(config_path)?,
                authority,
                public_config,
                private_config,
                load_generator_config,
            )
            .await
        }
        #[cfg(feature = "move")]
        Engine::Move => {
            run_with(
                crate::engines::MoveConfig::load_or_default(config_path)?,
                authority,
                public_config,
                private_config,
                load_generator_config,
            )
            .await
        }
        #[cfg(not(feature = "move"))]
        Engine::Move => Err(eyre!("--engine move needs a build with the `move` feature")),
    }
}

async fn run_with<En: ValidatorEngine>(
    engine: En,
    authority: Authority,
    public_config: PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    load_generator_config: Option<LoadGeneratorConfig>,
) -> Result<()> {
    // Resolve this authority's network and metrics addresses from the public config.
    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or_else(|| eyre!("No metrics address for authority {authority}"))?;
    let network_address = public_config
        .network_address(authority)
        .ok_or_else(|| eyre!("No network address for authority {authority}"))?;

    // The orchestrator wipes storage between runs and re-runs genesis, but a configuration
    // skip must not crash the node on a missing directory.
    fs::create_dir_all(&private_config.storage_path).wrap_err_with(|| {
        format!(
            "Failed to create storage directory '{}'",
            private_config.storage_path.display()
        )
    })?;

    // Build and start the validator on tokio with the defaults a real deployment wants: real
    // TCP, real crypto, WAL storage. The registry is shared between the replica's and the
    // validator's metrics and served over HTTP below.
    let registry = MetricsRegistry::new();
    let processor = engine.processor(authority, &public_config)?;
    let handle =
        ValidatorBuilder::with_processor(processor, authority, public_config, private_config)
            .with_registry(registry.clone())
            .build()
            .start::<TokioCtx>()
            .await?;
    // Keep the generator task bound to the command's scope so it isn't dropped mid-run.
    let load_generator =
        load_generator_config.map(|config| handle.start_load_generator(config, engine.workload()));

    // Expose metrics over HTTP on all interfaces for external scraping.
    let metrics_server = PrometheusServer::new(metrics_address, &registry)
        .bind_all_interfaces()
        .start()
        .await?;

    tracing::info!("Metrics server listening on {metrics_address}");
    tracing::info!("Validator {authority} listening on {network_address}");

    // Wait for whichever component finishes first; all of them finishing is an error.
    tokio::select! {
        result = handle.await_completion() => result,
        result = metrics_server => result.map_err(|error| eyre!("Metrics server crashed: {error}")),
        result = async {
            match load_generator {
                Some(task) => match task.await {
                    Ok(()) => Err(eyre!("Load generator stopped unexpectedly")),
                    Err(error) => Err(eyre!("Load generator crashed: {error}")),
                },
                None => std::future::pending::<Result<()>>().await,
            }
        } => result,
    }
}
