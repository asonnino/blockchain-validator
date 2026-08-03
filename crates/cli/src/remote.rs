// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod benchmark;
pub mod protocol;

use std::fmt::Display;

use eyre::{Context, Result};
use orchestrator::{provider::ServerProviderClient, settings::Settings, testbed::Testbed};

use crate::args::RemoteTestbedCommand;

use benchmark::RemoteBenchmarkDriver;

pub(crate) struct RemoteTestbedDriver<C> {
    testbed: Testbed<C>,
    settings: Settings,
}

impl<C: ServerProviderClient> RemoteTestbedDriver<C> {
    pub(crate) async fn new(settings: Settings, client: C) -> Result<Self> {
        let testbed = Testbed::new(settings.clone(), client)
            .await
            .wrap_err("Failed to create testbed")?;
        Ok(Self { testbed, settings })
    }
}

impl<C: ServerProviderClient + Display> RemoteTestbedDriver<C> {
    pub(crate) async fn run(mut self, command: RemoteTestbedCommand) -> Result<()> {
        match command {
            RemoteTestbedCommand::Status => {
                let status = self.testbed.status();
                println!("{} ({} active)", status.client_summary, status.active_count);
                println!(
                    "repository: {} @ {}",
                    status.repository_url, status.repository_commit
                );
                for region in status.regions {
                    println!("{}:", region.region);
                    for instance in region.instances {
                        let state = if instance.active { "active" } else { "stopped" };
                        println!("  [{state}] {}", instance.connect_command);
                    }
                }
                Ok(())
            }
            RemoteTestbedCommand::Create { instances, region } => {
                tracing::info!("Creating instances ({instances} per region)");
                self.testbed
                    .create(instances, region)
                    .await
                    .wrap_err("Failed to create testbed")
            }
            RemoteTestbedCommand::Start { instances } => {
                tracing::info!("Booting instances");
                self.testbed
                    .start(instances)
                    .await
                    .wrap_err("Failed to start testbed")
            }
            RemoteTestbedCommand::Stop => {
                tracing::info!("Stopping instances");
                self.testbed.stop().await.wrap_err("Failed to stop testbed")
            }
            RemoteTestbedCommand::Destroy => {
                tracing::info!("Destroying testbed");
                self.testbed
                    .destroy()
                    .await
                    .wrap_err("Failed to destroy testbed")
            }
            RemoteTestbedCommand::Benchmark {
                committee,
                loads,
                skip_testbed_update,
                skip_testbed_configuration,
            } => {
                let instances = self.testbed.instances();
                let setup_commands = self
                    .testbed
                    .setup_commands()
                    .await
                    .wrap_err("Failed to load testbed setup commands")?;
                let username = self.testbed.username().to_string();
                RemoteBenchmarkDriver::new(self.settings, username)
                    .benchmark(
                        instances,
                        setup_commands,
                        committee,
                        loads,
                        skip_testbed_update,
                        skip_testbed_configuration,
                    )
                    .await
            }
        }
    }
}
