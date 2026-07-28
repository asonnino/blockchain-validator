// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator node: a consensus replica wired to an execution engine.

use std::sync::Arc;

use dag::{
    authority::Authority, block::transaction::Transaction as ConsensusTransaction,
    consensus::CommittedSubDag, context::Ctx, crypto::AsBytes, metrics::Metrics,
    sync::network::Network,
};
use execution::{
    engine::ExecutionEngine, scheduler::SequentialScheduler, transaction::Transaction,
};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::{Replica, ReplicaHandle},
};
use tokio::sync::{mpsc, watch};

/// Capacity of the commit stream; a bounded channel is mandatory — a slow consumer must
/// backpressure the replica rather than drop or reorder commits.
const COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// Builds a [`Validator`], delegating replica options to the underlying [`ReplicaBuilder`].
///
/// Under the simulator, `with_network` and `with_metrics` are mandatory: the replica defaults
/// bind real TCP and spawn onto the tokio runtime, neither of which exists there.
pub struct ValidatorBuilder<E> {
    engine: E,
    replica: ReplicaBuilder,
}

impl<E: ExecutionEngine + Send + 'static> ValidatorBuilder<E> {
    pub fn new(
        engine: E,
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
    ) -> Self {
        Self {
            engine,
            replica: ReplicaBuilder::new(authority, public_config, private_config),
        }
    }

    pub fn with_storage(mut self, storage: StorageKind) -> Self {
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

    pub fn with_crypto_disabled(mut self) -> Self {
        self.replica = self.replica.with_crypto_disabled();
        self
    }

    /// Assembles the validator: registers the commit consumer and builds the replica. No I/O
    /// happens until [`Validator::start`].
    pub fn build(self) -> Validator<E> {
        let (sender, receiver) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
        Validator {
            engine: self.engine,
            replica: self.replica.with_commit_consumer(sender).build(),
            receiver,
        }
    }
}

/// A fully configured validator, ready to [`start`](Validator::start).
pub struct Validator<E> {
    engine: E,
    replica: Replica,
    receiver: mpsc::Receiver<CommittedSubDag>,
}

impl<E: ExecutionEngine + Send + 'static> Validator<E> {
    /// Starts the replica and the driver task feeding its committed transactions, in commit
    /// order, to the scheduler. Undecodable transactions are skipped; the choice is
    /// deterministic since every validator sees the same bytes.
    pub async fn start<C: Ctx>(self) -> eyre::Result<ValidatorHandle<C, E>> {
        let replica = self.replica.run::<C>().await?;

        let engine = self.engine;
        let mut receiver = self.receiver;
        let (executed_sender, executed) = watch::channel(0);
        let driver = C::spawn(async move {
            let mut scheduler = SequentialScheduler::new(engine);
            let mut count = 0u64;
            while let Some(mut subdag) = receiver.recv().await {
                subdag.sort();
                let transactions = subdag
                    .blocks
                    .iter()
                    .flat_map(|block| block.transactions())
                    .filter_map(|transaction| {
                        Transaction::from_bytes(transaction.as_bytes())
                            .inspect_err(|error| {
                                tracing::warn!(?error, "skipping undecodable transaction")
                            })
                            .ok()
                    });
                count += scheduler.execute(transactions).len() as u64;
                let _ = executed_sender.send(count);
            }
            scheduler
        });

        Ok(ValidatorHandle {
            replica,
            driver,
            executed,
        })
    }
}

/// A handle to a running validator: a consensus replica whose committed transactions are
/// executed by an [`ExecutionEngine`] through a [`SequentialScheduler`].
///
/// The driver task owns the scheduler and every task is spawned through [`Ctx`], so the whole
/// node runs unchanged under tokio and under the mysticeti simulator.
pub struct ValidatorHandle<C: Ctx, E: ExecutionEngine + Send + 'static> {
    replica: ReplicaHandle<C>,
    driver: C::JoinHandle<SequentialScheduler<E>>,
    executed: watch::Receiver<u64>,
}

impl<C: Ctx, E: ExecutionEngine + Send + 'static> ValidatorHandle<C, E> {
    /// Submits transactions; resolves once they are queued for inclusion in a block, not once
    /// they are committed or executed. Errors only if the replica has shut down.
    pub async fn submit(&self, transactions: Vec<Transaction>) -> eyre::Result<()> {
        let transactions = transactions
            .iter()
            .map(|transaction| ConsensusTransaction::new(transaction.to_bytes().into()))
            .collect();
        self.replica.submit(transactions).await
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

    /// Stops the replica, waits for the driver to drain the remaining commits, and returns the
    /// scheduler with the executed state.
    pub async fn shutdown(self) -> SequentialScheduler<E> {
        // The returned syncer holds the consensus storage; recovery will need it, execution
        // state does not.
        let _syncer = self.replica.shutdown().await;
        self.driver.await.expect("driver task failed")
    }
}
