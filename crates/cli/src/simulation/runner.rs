// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One run: a committee under the simulator, from the first submission to quiescence.

use std::{path::Path, pin::pin, time::Duration};

use dag::{
    authority::Authority, consensus::CommittedSubDag, context::Ctx, crypto::AsBytes,
    metrics::Metrics,
};
use eyre::{Result, bail};
use futures::future::{Either, select};
use prometheus::{Encoder, Registry, TextEncoder};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use simulator::{SimulatedNetwork, SimulatorContext, SimulatorExecutor};
use validator::{
    engine::Engine,
    envelope::{Envelope, NoMessage, Payload},
    metrics::ValidatorMetrics,
    processor::{CommitProcessor, Processed, SequentialProcessor},
    validator::ValidatorBuilder,
};

use crate::simulation::config::SimulationConfig;

const DRAIN: Duration = Duration::from_secs(5);
/// Simulated time any wait may take before the run counts as stuck.
const LIMIT: Duration = Duration::from_secs(600);

/// What a run measured, by authority.
pub struct Run<P> {
    pub processors: Vec<P>,
    pub timings: Vec<Timings>,
    /// Prometheus text.
    pub metrics: Vec<String>,
    /// Simulated time the load started at.
    pub start: Duration,
}

/// One validator's executions, as [`Timed`] saw them.
#[derive(Default, PartialEq, Debug)]
pub struct Timings {
    /// Each executed transaction's latency from submission.
    pub latencies_ms: Vec<u64>,
    /// When it last executed a transaction.
    pub last_ms: u64,
    pub busy: Duration,
}

/// Runs one suite entry's committee under the simulator, once per engine it is given.
pub struct Runner {
    config: SimulationConfig,
}

impl Runner {
    pub fn new(config: SimulationConfig) -> Self {
        Self { config }
    }

    /// Runs the committee with `engine` until every validator executed the same transactions
    /// and certified them.
    pub fn run<En>(&self, engine: En) -> Result<Run<En::Processor>>
    where
        En: Engine + Clone + Send + 'static,
    {
        let config = self.config.clone();
        SimulatorExecutor::run(StdRng::seed_from_u64(config.seed), async move {
            let size = config.committee_size;
            let public_config = PublicReplicaConfig::new_for_tests(size);
            let (network, node_networks) =
                SimulatedNetwork::new(&public_config.committee(), config.latency.clone());
            let network = network.with_jitter(config.jitter);
            let private_configs =
                PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), size);

            let mut validators = Vec::with_capacity(size);
            let mut sinks = Vec::with_capacity(size);
            for (index, (node_network, private_config)) in
                node_networks.into_iter().zip(private_configs).enumerate()
            {
                let authority = Authority::from(index);
                let timed = Timed {
                    inner: engine.processor(authority, &public_config)?,
                    timings: Timings::default(),
                };
                let (registry, metrics) = (Registry::new(), Metrics::new_for_test(size));
                let builder = ValidatorBuilder::with_processor(
                    timed,
                    authority,
                    public_config.clone(),
                    private_config,
                );
                let validator = builder
                    .with_registry(registry.clone())
                    .with_storage(StorageKind::Ephemeral)
                    .with_simulated_crypto()
                    // The default metrics spawn their reporter on tokio, absent here.
                    .with_metrics(metrics.clone())
                    .with_network(node_network)
                    .build()
                    .start::<SimulatorContext>()
                    .await?;
                validators.push(validator);
                sinks.push((registry, metrics));
            }
            network.connect_all().await;

            let start = SimulatorContext::timestamp_utc();
            let load = config.load_generator();
            let generators: Vec<_> = validators
                .iter()
                .map(|validator| validator.start_load_generator(load.clone(), engine.workload()))
                .collect();
            SimulatorContext::sleep(Duration::from_secs(config.duration_secs)).await;
            for generator in &generators {
                SimulatorContext::abort(generator);
            }
            // Queued transactions keep committing after the generators stop: wait until every
            // validator executed every counted submission, then holds the same count for a whole
            // window (a batch the abort interrupted may be submitted but uncounted).
            let submitted = validators
                .iter()
                .map(|v| v.metrics().submitted_transactions().get())
                .sum();
            for validator in &mut validators {
                within(validator.wait_for_transactions(submitted)).await?;
            }
            loop {
                let target = validators.iter().map(|v| v.executed()).max().unwrap_or(0);
                for validator in &mut validators {
                    within(validator.wait_for_transactions(target)).await?;
                }
                SimulatorContext::sleep(DRAIN).await;
                if validators.iter().all(|v| v.executed() == target) {
                    break;
                }
            }
            for validator in &mut validators {
                within(validator.wait_for_certified()).await?;
            }

            let metrics = sinks
                .iter()
                .map(|(registry, metrics)| {
                    let mut text = Vec::new();
                    TextEncoder::new()
                        .encode(&registry.gather(), &mut text)
                        .expect("encoding to memory succeeds");
                    String::from_utf8(text).expect("Prometheus text is UTF-8")
                        + &metrics.collect().to_prometheus_text()
                })
                .collect();
            let mut processors = Vec::with_capacity(size);
            let mut timings = Vec::with_capacity(size);
            for validator in validators {
                let Timed {
                    inner,
                    timings: own,
                } = validator.shutdown().await;
                processors.push(inner);
                timings.push(own);
            }
            Ok(Run {
                processors,
                timings,
                metrics,
                start,
            })
        })
    }
}

/// The sequential run a simulated run compares against: the default processor over the
/// engine's reference, submitting the engine's own workload.
#[derive(Clone)]
pub struct Baseline<En>(En);

impl<En: Engine> Baseline<En> {
    /// `engine`'s baseline, if it has a reference.
    pub fn of(engine: En) -> Option<Self> {
        engine.reference().is_some().then_some(Self(engine))
    }
}

impl<En: Engine> Engine for Baseline<En> {
    type Processor = SequentialProcessor<En::Reference>;
    type Workload = En::Workload;
    type Reference = En::Reference;

    fn processor(
        &self,
        _authority: Authority,
        public_config: &PublicReplicaConfig,
    ) -> Result<Self::Processor> {
        let reference = self
            .0
            .reference()
            .expect("baselines are of engines with a reference");
        SequentialProcessor::new(reference, public_config)
    }

    fn workload(&self) -> En::Workload {
        self.0.workload()
    }
}

async fn within<T>(future: impl Future<Output = T>) -> Result<T> {
    match select(pin!(future), pin!(SimulatorContext::sleep(LIMIT))).await {
        Either::Left((output, _)) => Ok(output),
        Either::Right(_) => bail!("the committee made no progress for {LIMIT:?}"),
    }
}

/// A processor, recording when each transaction it is handed was submitted and the time it is
/// busy for.
struct Timed<P> {
    inner: P,
    timings: Timings,
}

impl<P: CommitProcessor> CommitProcessor for Timed<P> {
    type Message = P::Message;

    fn process(
        &mut self,
        subdag: CommittedSubDag,
        now: Duration,
        metrics: &ValidatorMetrics,
    ) -> Processed<P::Message> {
        // Engine messages don't decode without their type, which leaves the executions.
        let payloads = subdag.blocks.iter().flat_map(|block| block.transactions());
        let submitted_ms: Vec<_> = payloads
            .filter_map(|payload| Envelope::<NoMessage>::from_bytes(payload.as_bytes()).ok())
            .filter_map(|envelope| {
                let submitted_ms = envelope.timestamp_ms();
                matches!(envelope.into_payload(), Payload::Execute(_)).then_some(submitted_ms)
            })
            .collect();
        let processed = self.inner.process(subdag, now, metrics);
        self.timings.busy += processed.busy;
        // The processor is done once its busy time is over.
        let done_ms = (now + processed.busy).as_millis() as u64;
        let latencies = submitted_ms
            .iter()
            .map(|submitted| done_ms.saturating_sub(*submitted));
        self.timings.latencies_ms.extend(latencies);
        if !submitted_ms.is_empty() {
            self.timings.last_ms = done_ms;
        }
        processed
    }

    fn after_replay(&mut self) -> Vec<Payload<P::Message>> {
        self.inner.after_replay()
    }

    fn executed(&self) -> u64 {
        self.inner.executed()
    }

    fn is_certified(&self) -> bool {
        self.inner.is_certified()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dag::{authority::Authority, consensus::CommittedSubDag};
    use fake_engine::{FakeExecutor, FakeWorkload};
    use replica::config::PublicReplicaConfig;
    use validator::{
        engine::Engine,
        envelope::{NoMessage, Payload},
        metrics::ValidatorMetrics,
        processor::{CommitProcessor, Processed, SequentialProcessor},
    };

    use super::{Baseline, Runner};
    use crate::{
        engines::FakeConfig,
        simulation::config::{LoadConfig, SimulationConfig},
    };

    const BUSY: Duration = Duration::from_millis(30);

    /// The fake engine, busy with every sub-dag, with the plain one as its reference.
    #[derive(Clone)]
    struct Slow;

    struct Busy(SequentialProcessor<FakeExecutor>);

    impl CommitProcessor for Busy {
        type Message = NoMessage;

        fn process(
            &mut self,
            subdag: CommittedSubDag,
            now: Duration,
            metrics: &ValidatorMetrics,
        ) -> Processed<NoMessage> {
            Processed {
                busy: BUSY,
                ..self.0.process(subdag, now + BUSY, metrics)
            }
        }

        fn after_replay(&mut self) -> Vec<Payload<NoMessage>> {
            self.0.after_replay()
        }

        fn executed(&self) -> u64 {
            self.0.executed()
        }

        fn is_certified(&self) -> bool {
            self.0.is_certified()
        }
    }

    impl Engine for Slow {
        type Processor = Busy;
        type Workload = FakeWorkload;
        type Reference = FakeExecutor;

        fn processor(
            &self,
            _authority: Authority,
            public_config: &PublicReplicaConfig,
        ) -> eyre::Result<Busy> {
            Ok(Busy(SequentialProcessor::new(FakeExecutor, public_config)?))
        }

        fn workload(&self) -> FakeWorkload {
            FakeWorkload::default()
        }

        fn reference(&self) -> Option<FakeExecutor> {
            Some(FakeExecutor)
        }
    }

    #[test]
    fn baselines_rerun_the_workload_under_the_default_processor() {
        let config = SimulationConfig {
            duration_secs: 2,
            load: LoadConfig {
                load: 10,
                ..LoadConfig::default()
            },
            ..SimulationConfig::default()
        };
        let runner = Runner::new(config);
        let slow = runner.run(Slow).unwrap();
        let baseline = runner
            .run(Baseline::of(Slow).expect("a reference"))
            .unwrap();
        assert_eq!(
            slow.processors[0].executed(),
            baseline.processors[0].executed()
        );
        assert!(slow.timings.iter().all(|timings| timings.busy > BUSY));
        assert!(
            baseline
                .timings
                .iter()
                .all(|timings| timings.busy.is_zero())
        );
        assert!(Baseline::of(FakeConfig::default()).is_none());
        // `Timed` records exactly the executions, once their processing is done.
        let fastest = slow
            .timings
            .iter()
            .flat_map(|timings| &timings.latencies_ms)
            .min();
        assert!(*fastest.unwrap() >= BUSY.as_millis() as u64);
        for run in [&slow.timings, &baseline.timings] {
            for (timings, processor) in run.iter().zip(&slow.processors) {
                assert_eq!(timings.latencies_ms.len() as u64, processor.executed());
            }
        }
    }
}
