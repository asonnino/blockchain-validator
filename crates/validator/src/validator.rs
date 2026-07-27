// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator node: a mysticeti replica wired to an execution engine.

use dag::{
    authority::Authority, block::transaction::Transaction as DagTransaction, context::Ctx,
    crypto::AsBytes,
};
use execution::{
    engine::ExecutionEngine, scheduler::SequentialScheduler, transaction::Transaction,
};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::ReplicaHandle,
};
use tokio::sync::{mpsc, watch};

/// Capacity of the commit stream; a bounded channel is mandatory — a slow consumer must
/// backpressure the replica rather than drop or reorder commits.
const COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// A validator node: a mysticeti replica whose committed transactions are executed by an
/// [`ExecutionEngine`] through a [`SequentialScheduler`].
///
/// The driver task owns the scheduler and every task is spawned through [`Ctx`], so the whole
/// node runs unchanged under tokio and under the mysticeti simulator.
pub struct Validator<C: Ctx, E: ExecutionEngine + Send + 'static> {
    replica: ReplicaHandle<C>,
    driver: C::JoinHandle<SequentialScheduler<E>>,
    executed: watch::Receiver<u64>,
}

impl<C: Ctx, E: ExecutionEngine + Send + 'static> Validator<C, E> {
    /// Starts the replica and the driver task feeding its committed transactions, in commit
    /// order, to the scheduler. Undecodable transactions are skipped; the choice is
    /// deterministic since every validator sees the same bytes.
    pub async fn start(
        engine: E,
        authority: Authority,
        public_config: PublicReplicaConfig,
        private_config: PrivateReplicaConfig,
        storage: StorageKind,
    ) -> eyre::Result<Self> {
        let (sender, mut receiver) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
        let replica = ReplicaBuilder::new(authority, public_config, private_config)
            .with_storage(storage)
            .with_commit_consumer(sender)
            .build()
            .run::<C>()
            .await?;

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

        Ok(Self {
            replica,
            driver,
            executed,
        })
    }

    /// Submits transactions to the replica; resolves once they are queued for inclusion in a
    /// block, not once they are committed or executed.
    pub async fn submit(&self, transactions: Vec<Transaction>) -> eyre::Result<()> {
        let transactions = transactions
            .iter()
            .map(|transaction| DagTransaction::new(transaction.to_bytes().into()))
            .collect();
        self.replica.submit(transactions).await
    }

    /// Waits until at least `count` transactions have been executed. Returns early if the
    /// driver has stopped.
    pub async fn wait_for_transactions(&mut self, count: u64) {
        while *self.executed.borrow_and_update() < count && self.executed.changed().await.is_ok() {}
    }

    /// Stops the replica, waits for the driver to drain the remaining commits, and returns the
    /// scheduler with the executed state.
    pub async fn shutdown(self) -> SequentialScheduler<E> {
        drop(self.replica.shutdown().await);
        self.driver.await.expect("driver task failed")
    }
}
