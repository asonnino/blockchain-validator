// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator node: a mysticeti replica wired to an execution engine.

use std::{path::PathBuf, sync::Arc};

use checkpoint::checkpoint::CheckpointChain;
use dag::{
    authority::Authority, block::transaction::Transaction as ConsensusTransaction,
    consensus::CommittedSubDag, context::Ctx, crypto::AsBytes, metrics::Metrics, storage::Storage,
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
    authority: Authority,
    public_config: PublicReplicaConfig,
    /// The consensus WAL to replay on start; `None` for ephemeral storage.
    wal: Option<PathBuf>,
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
            authority,
            public_config: public_config.clone(),
            // The replica defaults to WAL storage at this path when `with_storage` is not called.
            wal: Some(private_config.wal()),
            replica: ReplicaBuilder::new(authority, public_config, private_config),
        }
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

    pub fn with_crypto_disabled(mut self) -> Self {
        self.replica = self.replica.with_crypto_disabled();
        self
    }

    /// Assembles the validator: builds the scheduler and the replica with its commit consumer.
    /// No I/O happens until [`Validator::start`].
    pub fn build(self) -> Validator<E> {
        let (sender, receiver) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
        Validator {
            scheduler: SequentialScheduler::new(self.engine),
            checkpoints: CheckpointChain::default(),
            authority: self.authority,
            public_config: self.public_config,
            wal: self.wal,
            replica: self.replica.with_commit_consumer(sender).build(),
            receiver,
        }
    }
}

/// A fully configured validator, ready to [`start`](Validator::start).
pub struct Validator<E> {
    scheduler: SequentialScheduler<E>,
    checkpoints: CheckpointChain,
    authority: Authority,
    public_config: PublicReplicaConfig,
    wal: Option<PathBuf>,
    replica: Replica,
    receiver: mpsc::Receiver<CommittedSubDag>,
}

impl<E: ExecutionEngine + Send + 'static> Validator<E> {
    /// Replays the consensus WAL (if any) to rebuild execution state, then starts the replica
    /// and the driver task feeding its committed transactions, in commit order, to the
    /// scheduler. The live stream never re-emits recovered commits, so replay and stream
    /// compose without overlap.
    pub async fn start<C: Ctx>(mut self) -> eyre::Result<ValidatorHandle<C, E>> {
        let replayed = self.replay()?;
        let Self {
            mut scheduler,
            mut checkpoints,
            replica,
            mut receiver,
            ..
        } = self;

        let replica = replica.run::<C>().await?;
        let (executed_sender, executed) = watch::channel(replayed);
        let driver = C::spawn(async move {
            let mut count = replayed;
            while let Some(subdag) = receiver.recv().await {
                count += Self::execute_subdag(&mut scheduler, &mut checkpoints, subdag) as u64;
                let _ = executed_sender.send(count);
            }
            (scheduler, checkpoints)
        });

        Ok(ValidatorHandle {
            replica,
            driver,
            executed,
        })
    }

    /// Rebuilds execution state by replaying the WAL's committed sub-dags through the
    /// scheduler, exactly as the live stream delivered them. The storage handle only reads and
    /// is dropped before the replica re-opens the WAL (there is no file locking).
    fn replay(&mut self) -> eyre::Result<u64> {
        let Some(wal) = &self.wal else {
            return Ok(0);
        };
        let committee = self.public_config.committee();
        // An isolated registry: replay metrics are discarded with this read-only handle.
        let metrics = Metrics::new_for_test(committee.len());
        let (storage, _recovered) = Storage::open(self.authority, wal, metrics, &committee)?;

        let mut replayed = 0;
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
            replayed +=
                Self::execute_subdag(&mut self.scheduler, &mut self.checkpoints, subdag) as u64;
        }
        Ok(replayed)
    }

    /// Executes every transaction of a committed sub-dag in canonical order and returns how
    /// many were executed. Undecodable transactions are skipped; the choice is deterministic.
    /// Sub-dags that execute nothing are not checkpointed: their commitment is unchanged, and
    /// their count varies across validators at any executed-transaction cut.
    fn execute_subdag(
        scheduler: &mut SequentialScheduler<E>,
        checkpoints: &mut CheckpointChain,
        mut subdag: CommittedSubDag,
    ) -> usize {
        let anchor = subdag.anchor;
        subdag.sort();
        let transactions = subdag
            .blocks
            .iter()
            .flat_map(|block| block.transactions())
            .filter_map(|transaction| {
                Transaction::from_bytes(transaction.as_bytes())
                    .inspect_err(|error| tracing::warn!(?error, "skipping undecodable transaction"))
                    .ok()
            });
        let executed = scheduler.execute(transactions).len();
        if executed > 0 {
            checkpoints.push(anchor, scheduler.store().commitment());
        }
        executed
    }
}

/// A handle to a running validator: a consensus replica whose committed transactions are
/// executed by an [`ExecutionEngine`] through a [`SequentialScheduler`].
///
/// The driver task owns the scheduler and every task is spawned through [`Ctx`], so the whole
/// node runs unchanged under tokio and under the mysticeti simulator.
pub struct ValidatorHandle<C: Ctx, E: ExecutionEngine + Send + 'static> {
    replica: ReplicaHandle<C>,
    driver: C::JoinHandle<(SequentialScheduler<E>, CheckpointChain)>,
    executed: watch::Receiver<u64>,
}

impl<C: Ctx, E: ExecutionEngine + Send + 'static> ValidatorHandle<C, E> {
    /// Submits transactions to the replica; resolves once they are queued for inclusion in a
    /// block, not once they are committed or executed.
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
    /// scheduler with the executed state and the checkpoint chain.
    pub async fn shutdown(self) -> (SequentialScheduler<E>, CheckpointChain) {
        // The returned syncer holds the consensus storage; the WAL outlives it on disk, which
        // is what replay on the next start relies on.
        let _syncer = self.replica.shutdown().await;
        self.driver.await.expect("driver task failed")
    }
}
