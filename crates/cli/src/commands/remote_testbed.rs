// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use eyre::{Context, Result};
use orchestrator::{
    provider::{aws::AwsClient, custom::CustomClient},
    settings::{CloudProvider, Settings},
};
use tracing_subscriber::filter::LevelFilter;

use crate::{args::RemoteTestbedArgs, remote::RemoteTestbedDriver, tracing::ValidatorTracing};

pub async fn remote_testbed(
    args: RemoteTestbedArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let level = log_level.unwrap_or(LevelFilter::INFO);
    let _guard = ValidatorTracing::new(level)
        .with_log_file(log_file)
        .setup()?;

    let settings_path = args.settings_path.display().to_string();
    let settings = Settings::load(&settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws(_) => {
            let client = AwsClient::new(settings.clone()).await;
            RemoteTestbedDriver::new(settings, client)
                .await?
                .run(args.command)
                .await
        }
        CloudProvider::Custom(_) => {
            let client = CustomClient::new(settings.clone());
            RemoteTestbedDriver::new(settings, client)
                .await?
                .run(args.command)
                .await
        }
    }
}
