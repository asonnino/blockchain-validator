// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The orchestrator's view of the validator: how to configure, run, and scrape it.

use std::{
    fmt::{self, Debug},
    net::IpAddr,
    ops::Deref,
    path::PathBuf,
};

use clap::ValueEnum;
use dag::authority::Authority;
use orchestrator::{
    benchmark::BenchmarkParameters,
    collector::{MetricKind, MetricSpec},
    protocol::{BINARY_PATH, Protocol, ProtocolCommands, ProtocolMetrics, ProtocolParameters},
    provider::Instance,
    settings::Settings,
};
use replica::config::{PublicReplicaConfig, ReplicaParameters};
use serde::{Deserialize, Serialize};
use validator::generator::LoadGeneratorConfig;

use crate::engines::Engine;

const PUBLIC_REPLICA_CONFIG_FILENAME: &str = PublicReplicaConfig::DEFAULT_FILENAME;
const LOAD_GENERATOR_CONFIG_FILENAME: &str = LoadGeneratorConfig::DEFAULT_FILENAME;
const ENGINE_CONFIG_FILENAME: &str = "engine-config.yaml";

/// The replica parameters distributed to every validator. The `Debug` output keys the
/// benchmark's measurement files, so it must stay short and filesystem-safe.
#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct NodeParameters(ReplicaParameters);

impl Deref for NodeParameters {
    type Target = ReplicaParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for NodeParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.consensus)
    }
}

impl ProtocolParameters for NodeParameters {}

/// The load generator config distributed to every validator; the benchmark's total load is
/// split across the committee before upload.
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct ClientParameters(LoadGeneratorConfig);

impl Deref for ClientParameters {
    type Target = LoadGeneratorConfig;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for ClientParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.transaction_size)
    }
}

impl ProtocolParameters for ClientParameters {}

pub struct ValidatorProtocol {
    working_dir: PathBuf,
    engine: Engine,
    /// The engine's parameters as YAML, uploaded at genesis; the engine's defaults if `None`.
    engine_config: Option<String>,
}

impl ValidatorProtocol {
    pub fn new(settings: &Settings, engine: Engine, engine_config: Option<String>) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
            engine,
            engine_config,
        }
    }
}

impl Protocol for ValidatorProtocol {
    type NodeParameters = NodeParameters;
    type ClientParameters = ClientParameters;
}

impl ProtocolCommands for ValidatorProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec![]
    }

    fn db_directories(&self) -> Vec<PathBuf> {
        vec![self.working_dir.join("storage-*")]
    }

    async fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let ips = instances
            .map(|x| x.main_ip.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        let replica_parameters = parameters.node_parameters.clone();
        let replica_parameters_string = serde_yaml::to_string(&replica_parameters).unwrap();
        let replica_parameters_path = self.working_dir.join("replica-parameters.yaml");
        let upload_replica_parameters = format!(
            "echo -e '{replica_parameters_string}' > {}",
            replica_parameters_path.display()
        );

        let mut load_generator_config = parameters.client_parameters.clone();
        load_generator_config.0.load = parameters.load / parameters.nodes;
        let load_generator_config_string = serde_yaml::to_string(&load_generator_config).unwrap();
        let load_generator_config_path = self.working_dir.join(LOAD_GENERATOR_CONFIG_FILENAME);
        let upload_load_generator_config = format!(
            "echo -e '{load_generator_config_string}' > {}",
            load_generator_config_path.display()
        );

        let genesis = [
            &format!("./{BINARY_PATH}/validator"),
            "test-genesis",
            &format!(
                "--ips {ips} --working-directory {} --replica-parameters-path {}",
                self.working_dir.display(),
                replica_parameters_path.display(),
            ),
        ]
        .join(" ");

        let upload_engine_config = self.engine_config.as_ref().map(|engine_config| {
            let path = self.working_dir.join(ENGINE_CONFIG_FILENAME);
            format!("echo -e '{engine_config}' > {}", path.display())
        });

        [
            "source $HOME/.cargo/env",
            &upload_replica_parameters,
            &upload_load_generator_config,
        ]
        .into_iter()
        .chain(upload_engine_config.as_deref())
        .chain([genesis.as_str()])
        .collect::<Vec<_>>()
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // The per-node share of the total load; the generator rejects a zero load, so nodes
        // without one to bear run without the flag (and without a generator).
        let load = parameters.load / parameters.nodes;
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let authority = Authority::from(i);
                let public_config_path = self.working_dir.join(PUBLIC_REPLICA_CONFIG_FILENAME);
                let private_config_path = self
                    .working_dir
                    .join(format!("private-replica-config-{authority}.yaml"));
                let load_generator_config_path =
                    self.working_dir.join(LOAD_GENERATOR_CONFIG_FILENAME);

                let engine = self.engine.to_possible_value().expect("engines are named");
                let mut run = vec![
                    format!("./{BINARY_PATH}/validator"),
                    "run".into(),
                    format!("--authority {}", authority.index()),
                    format!("--public-config-path {}", public_config_path.display()),
                    format!("--private-config-path {}", private_config_path.display()),
                    format!("--engine {}", engine.get_name()),
                ];
                if self.engine_config.is_some() {
                    let path = self.working_dir.join(ENGINE_CONFIG_FILENAME);
                    run.push(format!("--engine-config-path {}", path.display()));
                }
                if load > 0 {
                    run.push(format!(
                        "--load-generator-config-path {}",
                        load_generator_config_path.display()
                    ));
                }
                let run = run.join(" ");

                let command = ["source $HOME/.cargo/env", &run].join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // The load generator runs inside the validator process.
        vec![]
    }
}

impl ProtocolMetrics for ValidatorProtocol {
    fn metrics(&self) -> Vec<MetricSpec> {
        vec![
            MetricSpec {
                // Exposed as a counter but represents elapsed seconds — querying
                // `rate(benchmark_duration[1m])` always returns ~1.0, useless.
                // The cumulative value is what aggregation actually needs.
                name: dag::metrics::BENCHMARK_DURATION.into(),
                kind: MetricKind::Gauge,
            },
            MetricSpec {
                name: dag::metrics::LATENCY_S.into(),
                kind: MetricKind::Histogram,
            },
            MetricSpec {
                name: format!("{}_count", dag::metrics::LATENCY_S),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: format!("{}_sum", dag::metrics::LATENCY_S),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: dag::metrics::LATENCY_SQUARED_S.into(),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: validator::metrics::END_TO_END_LATENCY_S.into(),
                kind: MetricKind::Histogram,
            },
            MetricSpec {
                name: format!("{}_count", validator::metrics::END_TO_END_LATENCY_S),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: format!("{}_sum", validator::metrics::END_TO_END_LATENCY_S),
                kind: MetricKind::Counter,
            },
            MetricSpec {
                name: validator::metrics::CHECKPOINT_CERTIFICATION_LATENCY_S.into(),
                kind: MetricKind::Histogram,
            },
            MetricSpec {
                name: validator::metrics::SUBDAG_EXECUTION_LATENCY_S.into(),
                kind: MetricKind::Histogram,
            },
            MetricSpec {
                name: validator::metrics::SUBMITTED_TRANSACTIONS.into(),
                kind: MetricKind::Counter,
            },
        ]
    }

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let (ips, instances): (_, Vec<_>) = instances
            .into_iter()
            .map(|x| (IpAddr::V4(x.main_ip), x))
            .unzip();

        // Recomputing the deterministic genesis assigns each instance the same metrics
        // address the on-instance configs hold.
        let public_config = PublicReplicaConfig::new_for_benchmarks(ips);
        let metrics_paths = public_config
            .all_metric_addresses()
            .map(|x| format!("{x}{}", replica::prometheus::METRICS_ROUTE));

        instances.into_iter().zip(metrics_paths).collect()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<Self::NodeParameters, Self::ClientParameters>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // The load generators live inside the nodes, which are scraped already.
        self.nodes_metrics_path(instances, parameters)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use dag::authority::Authority;
    use orchestrator::{
        benchmark::BenchmarkParameters, protocol::ProtocolCommands, provider::Instance,
        settings::Settings,
    };

    use super::{ClientParameters, ENGINE_CONFIG_FILENAME, NodeParameters, ValidatorProtocol};
    use crate::{
        args::{Args, Command},
        engines::Engine,
    };

    // Todo: use `Instance::new_for_test` once it leaves `cfg(test)` (asonnino/mysticeti#225).
    fn instance() -> Instance {
        serde_yaml::from_str(
            "{id: '0', region: '', main_ip: 127.0.0.1, tags: [], specs: '', status: Active}",
        )
        .unwrap()
    }

    fn parameters(load: usize) -> BenchmarkParameters<NodeParameters, ClientParameters> {
        BenchmarkParameters {
            settings: Settings::default(),
            node_parameters: NodeParameters::default(),
            client_parameters: ClientParameters::default(),
            nodes: 4,
            load,
        }
    }

    #[test]
    fn validators_parse_their_node_commands() {
        let uploaded = Settings::default().working_dir.join(ENGINE_CONFIG_FILENAME);
        for engine_config in [None, Some("update_share: 0.5\n".to_string())] {
            let configured = engine_config.is_some();
            let protocol =
                ValidatorProtocol::new(&Settings::default(), Engine::Move, engine_config);
            // Below one transaction per node, validators run without a load generator.
            for (load, generates) in [(400, true), (3, false)] {
                let commands = protocol.node_command(vec![instance(); 4], &parameters(load));
                assert_eq!(commands.len(), 4);
                for (i, (_, command)) in commands.into_iter().enumerate() {
                    let run = command.rsplit(" && ").next().unwrap();
                    let parsed = Args::try_parse_from(run.split_whitespace()).unwrap();
                    let Command::Run(args) = parsed.command else {
                        panic!("not a run command: {run}");
                    };
                    assert_eq!(args.authority, Authority::from(i));
                    assert!(matches!(args.engine, Engine::Move));
                    assert_eq!(args.load_generator_config_path.is_some(), generates);
                    let expected = configured.then(|| uploaded.clone());
                    assert_eq!(args.engine_config_path, expected);
                }
            }
        }
    }

    #[tokio::test]
    async fn genesis_uploads_the_engine_parameters() {
        let engine_config = "update_share: 0.5\n";
        let protocol = ValidatorProtocol::new(
            &Settings::default(),
            Engine::Fake,
            Some(engine_config.to_string()),
        );
        let instances = vec![instance(); 4];
        let genesis = protocol
            .genesis_command(instances.iter(), &parameters(400))
            .await;
        // Where `node_command` points `--engine-config-path`.
        let uploaded = Settings::default().working_dir.join(ENGINE_CONFIG_FILENAME);
        let upload = format!("echo -e '{engine_config}' > {}", uploaded.display());
        assert!(
            genesis.split(" && ").any(|step| step == upload),
            "{genesis}"
        );
    }
}
