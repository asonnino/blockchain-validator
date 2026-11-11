// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator node: a mysticeti replica wired to a commit processor.

use std::{path::PathBuf, sync::Arc, time::Duration};

use dag::{
    authority::Authority, block::transaction::Transaction as ConsensusTransaction,
    consensus::CommittedSubDag, context::Ctx, metrics::Metrics, storage::Storage,
    sync::network::Network,
};
use execution::{engine::ExecutionEngine, transaction::Transaction};
use prometheus::Registry;
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::{Replica, ReplicaHandle},
};
use serde::Serialize;
use tokio::sync::{mpsc, watch};

#[cfg(any(test, feature = "benchmark"))]
use crate::generator::{LoadGeneratorConfig, TransactionGenerator};
use crate::{
    envelope::{Envelope, Payload},
    metrics::ValidatorMetrics,
    processor::{CommitProcessor, SequentialProcessor},
};

/// Capacity of the commit stream; a bounded channel is mandatory — a slow consumer must
/// backpressure the replica rather than drop or reorder commits.
const COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// Builds a [`Validator`], delegating replica options to the underlying [`ReplicaBuilder`].
///
/// Under the simulator, `with_network` and `with_metrics` are mandatory: the replica defaults
/// bind real TCP and spawn onto the tokio runtime, neither of which exists there.
pub struct ValidatorBuilder<P> {
    processor: P,
    authority: Authority,
    public_config: PublicReplicaConfig,
    /// The consensus WAL to replay on start; `None` for ephemeral storage.
    wal: Option<PathBuf>,
    registry: Registry,
    replica: ReplicaBuilder,
}

impl<E: ExecutionEngine + Send + 'static> ValidatorBuilder<SequentialProcessor<E>> {
    /// A builder for the default processor, executing with `engine`.
    pub fn new(
        engine: E,
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> eyre::Result<Self> {
        let processor = SequentialProcessor::new(engine, &public_config)?;
        Ok(Self::with_processor(
            processor,
            authority,
            public_config,
            private_config,
        ))
    }
}

impl<P: CommitProcessor> ValidatorBuilder<P> {
    pub fn with_processor(
        processor: P,
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> Self {
        Self {
            processor,
            authority,
            public_config: public_config.clone(),
            // The replica defaults to WAL storage at this path when `with_storage` is not called.
            wal: Some(private_config.wal()),
            registry: Registry::new(),
            replica: ReplicaBuilder::new(authority, public_config, private_config),
        }
    }

    /// Share one Prometheus registry between the replica's and the validator's metrics.
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry.clone();
        self.replica = self.replica.with_registry(registry);
        self
    }

    pub fn with_storage(mut self, storage: StorageKind) -> Self {
        self.wal = match &storage {
            StorageKind::Wal(path) => Some(path.clone()),
            StorageKind::Ephemeral => None,
        };
        self.replica = self.replica.with_storage(storage);
        self
    }

    pub fn with_network(mut self, network: Network) -> Self {
        self.replica = self.replica.with_network(network);
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.replica = self.replica.with_metrics(metrics);
        self
    }

    pub fn with_simulated_crypto(mut self) -> Self {
        self.replica = self.replica.with_simulated_crypto();
        self
    }

    /// Assembles the validator: builds the replica with its commit consumer. No I/O happens
    /// until [`Validator::start`].
    pub fn build(self) -> Validator<P> {
        let (sender, receiver) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
        Validator {
            processor: self.processor,
            authority: self.authority,
            public_config: self.public_config,
            wal: self.wal,
            metrics: ValidatorMetrics::new(&self.registry),
            replica: self.replica.with_commit_consumer(sender).build(),
            receiver,
        }
    }
}

/// A fully configured validator, ready to [`start`](Validator::start).
pub struct Validator<P> {
    processor: P,
    authority: Authority,
    public_config: PublicReplicaConfig,
    wal: Option<PathBuf>,
    metrics: Arc<ValidatorMetrics>,
    replica: Replica,
    receiver: mpsc::Receiver<CommittedSubDag>,
}

impl<P: CommitProcessor> Validator<P> {
    /// Replays the consensus WAL (if any) through the processor, then starts the replica and
    /// the driver task feeding the processor its committed sub-dags, in commit order. The live
    /// stream never re-emits recovered commits, so replay and stream compose without overlap.
    pub async fn start<C: Ctx>(mut self) -> eyre::Result<ValidatorHandle<C, P>> {
        self.replay()?;
        let max_block_size = self.public_config.parameters.dag.max_block_size;
        let Self {
            mut processor,
            replica,
            mut receiver,
            metrics,
            ..
        } = self;

        let replica = replica.run::<C>().await?;
        let client = replica.transaction_client();
        let (executed_sender, executed) = watch::channel(processor.executed());
        let (certified_sender, certified) = watch::channel(processor.is_certified());
        let driver_metrics = metrics.clone();
        let driver = C::spawn(async move {
            let timestamp_ms = C::timestamp_utc().as_millis() as u64;
            let resubmitted = to_consensus(timestamp_ms, processor.after_replay());
            if !resubmitted.is_empty() && client.submit(resubmitted).await.is_err() {
                tracing::debug!("resubmission after replay failed; shutting down");
            }

            while let Some(subdag) = receiver.recv().await {
                // The clock starts at delivery (idle waiting is not latency).
                let start = C::now();
                let now = C::timestamp_utc();
                let payloads = processor.process(subdag, now, &driver_metrics);
                driver_metrics.observe_subdag_execution_latency(C::elapsed(&start));
                let _ = executed_sender.send(processor.executed());
                let _ = certified_sender.send(processor.is_certified());
                // Failure means the replica is shutting down.
                let transactions = to_consensus(now.as_millis() as u64, payloads);
                if !transactions.is_empty() && client.submit(transactions).await.is_err() {
                    tracing::debug!("payload submission failed; shutting down");
                }
            }
            processor
        });

        Ok(ValidatorHandle {
            replica,
            driver,
            executed,
            certified,
            metrics,
            max_block_size,
        })
    }

    /// Rebuilds the processor's state by replaying the WAL's committed sub-dags, exactly as the
    /// live stream delivered them. The storage handle only reads and is dropped before the
    /// replica re-opens the WAL (there is no file locking).
    fn replay(&mut self) -> eyre::Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let committee = self.public_config.committee();
        // Isolated metrics, discarded with these handles: replay records nothing.
        let metrics = Metrics::new_for_test(committee.len());
        let replay_metrics = ValidatorMetrics::new(&Registry::new());
        let (storage, _recovered) = Storage::open(self.authority, wal, metrics, &committee)?;

        for commit in storage.iter_commits() {
            let blocks = commit
                .sub_dag
                .iter()
                .map(|reference| {
                    storage.block_reader().get_block(*reference).ok_or_else(|| {
                        eyre::eyre!("committed block {reference} missing from the WAL")
                    })
                })
                .collect::<eyre::Result<_>>()?;
            let subdag = CommittedSubDag::new(commit.leader, blocks);
            self.processor
                .process(subdag, Duration::ZERO, &replay_metrics);
        }
        Ok(())
    }
}

/// Wraps payloads as consensus transactions stamped with the submission time.
fn to_consensus<M: Serialize>(
    timestamp_ms: u64,
    payloads: impl IntoIterator<Item = Payload<M>>,
) -> Vec<ConsensusTransaction> {
    payloads
        .into_iter()
        .map(|payload| {
            ConsensusTransaction::new(Envelope::new(timestamp_ms, payload).to_bytes().into())
        })
        .collect()
}

/// A handle to a running validator: a consensus replica whose committed sub-dags feed a
/// [`CommitProcessor`].
///
/// The driver task owns the processor and every task is spawned through [`Ctx`], so the whole
/// node runs unchanged under tokio and under the mysticeti simulator.
pub struct ValidatorHandle<C: Ctx, P: CommitProcessor> {
    replica: ReplicaHandle<C>,
    driver: C::JoinHandle<P>,
    executed: watch::Receiver<u64>,
    certified: watch::Receiver<bool>,
    metrics: Arc<ValidatorMetrics>,
    max_block_size: usize,
}

impl<C: Ctx, P: CommitProcessor> ValidatorHandle<C, P> {
    /// Submits transactions to the replica; resolves once they are queued for inclusion in a
    /// block, not once they are committed or executed. Every payload is stamped with the
    /// submission time (one clock read per batch), from which mysticeti derives its
    /// commit-latency metric.
    pub async fn submit(&self, transactions: Vec<Transaction>) -> eyre::Result<()> {
        let timestamp_ms = C::timestamp_utc().as_millis() as u64;
        let payloads = transactions.into_iter().map(Payload::<P::Message>::Execute);
        self.replica
            .submit(to_consensus(timestamp_ms, payloads))
            .await
    }

    /// The validator's metrics.
    pub fn metrics(&self) -> &Arc<ValidatorMetrics> {
        &self.metrics
    }

    /// The replica's block-size cap, which bounds every submitted batch.
    pub fn max_block_size(&self) -> usize {
        self.max_block_size
    }

    /// The number of transactions executed so far.
    pub fn executed(&self) -> u64 {
        *self.executed.borrow()
    }

    /// Starts the built-in load generator; its transactions share the replica's submission
    /// channel with [`submit`](ValidatorHandle::submit). Stop it with [`Ctx::abort`].
    #[cfg(any(test, feature = "benchmark"))]
    pub fn start_load_generator(&self, config: LoadGeneratorConfig) -> C::JoinHandle<()> {
        TransactionGenerator::start::<C, P::Message>(
            self.replica.transaction_client(),
            self.replica.authority(),
            config,
            self.max_block_size,
            self.metrics.clone(),
        )
    }

    /// Waits until at least `count` transactions have been executed. Returns early if the
    /// driver has stopped.
    pub async fn wait_for_transactions(&mut self, count: u64) {
        while *self.executed.borrow_and_update() < count {
            if self.executed.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits until everything executed is certified. This is transient while submissions are
    /// in flight, so it is meaningful after an executed-count cut
    /// ([`wait_for_transactions`](ValidatorHandle::wait_for_transactions) first). Returns early
    /// if the driver has stopped.
    pub async fn wait_for_certified(&mut self) {
        while !*self.certified.borrow_and_update() {
            if self.certified.changed().await.is_err() {
                return;
            }
        }
    }

    /// Resolves when the replica crashes or stops; long-running deployments select on this.
    /// The driver task is detached and dies with the process.
    pub async fn await_completion(self) -> eyre::Result<()> {
        self.replica.await_completion().await
    }

    /// Stops the replica, waits for the driver to drain the remaining commits, and returns the
    /// processor.
    pub async fn shutdown(self) -> P {
        // The returned syncer holds the consensus storage; the WAL outlives it on disk, which
        // is what replay on the next start relies on.
        let _syncer = self.replica.shutdown().await;
        self.driver.await.expect("driver task failed")
    }
}
