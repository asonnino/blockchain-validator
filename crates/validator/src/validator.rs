// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator node: a mysticeti replica wired to an execution engine.

use std::{path::PathBuf, sync::Arc};

use checkpoint::{certifier::CheckpointCertifier, checkpoint::Checkpoint};
use dag::{
    authority::Authority, block::transaction::Transaction as ConsensusTransaction,
    consensus::CommittedSubDag, context::Ctx, crypto::AsBytes, metrics::Metrics, storage::Storage,
    sync::network::Network,
};
use execution::{
    crypto::Digest, engine::ExecutionEngine, scheduler::SequentialScheduler,
    transaction::Transaction,
};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::{Replica, ReplicaHandle},
};
use tokio::sync::{mpsc, watch};

use crate::envelope::{Envelope, Payload};

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

    /// Assembles the validator: builds the scheduler, the certifier, and the replica with its
    /// commit consumer. No I/O happens until [`Validator::start`].
    pub fn build(self) -> eyre::Result<Validator<E>> {
        let (sender, receiver) = mpsc::channel(COMMIT_CHANNEL_CAPACITY);
        let committee = self.public_config.committee();
        let protocol = self
            .public_config
            .parameters
            .consensus
            .to_protocol(&committee)?;
        let certifier = CheckpointCertifier::new(committee, protocol.quorum_threshold);
        Ok(Validator {
            scheduler: SequentialScheduler::new(self.engine),
            certifier,
            authority: self.authority,
            public_config: self.public_config,
            wal: self.wal,
            replica: self.replica.with_commit_consumer(sender).build(),
            receiver,
        })
    }
}

/// A fully configured validator, ready to [`start`](Validator::start).
pub struct Validator<E> {
    scheduler: SequentialScheduler<E>,
    certifier: CheckpointCertifier,
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
            mut certifier,
            replica,
            mut receiver,
            ..
        } = self;

        let replica = replica.run::<C>().await?;
        let client = replica.transaction_client();
        let (executed_sender, executed) = watch::channel(replayed);
        let (certified_sender, certified) =
            watch::channel(Self::certification_status(&certifier, &scheduler));
        let driver = C::spawn(async move {
            let mut count = replayed;
            while let Some(subdag) = receiver.recv().await {
                let (executed, minted) =
                    Self::execute_subdag(&mut scheduler, &mut certifier, subdag);
                count += executed as u64;
                let _ = executed_sender.send(count);
                let _ = certified_sender.send(Self::certification_status(&certifier, &scheduler));
                // Attest to the minted checkpoint through consensus: the vote rides this
                // validator's own signed blocks. Failure is benign — the replica is shutting
                // down, and lost votes are resubmitted after replay.
                if let Some(checkpoint) = minted {
                    let timestamp_ms = C::timestamp_utc().as_millis() as u64;
                    let envelope = Envelope::new(timestamp_ms, Payload::Attest(checkpoint));
                    let attestation = ConsensusTransaction::new(envelope.to_bytes().into());
                    if client.submit(vec![attestation]).await.is_err() {
                        tracing::debug!("attestation submission failed; shutting down");
                    }
                }
            }
            (scheduler, certifier)
        });

        Ok(ValidatorHandle {
            replica,
            driver,
            executed,
            certified,
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
            // The minted checkpoint is dropped: replay never submits votes.
            let (executed, _) =
                Self::execute_subdag(&mut self.scheduler, &mut self.certifier, subdag);
            replayed += executed as u64;
        }
        Ok(replayed)
    }

    /// Executes every transaction of a committed sub-dag in canonical order, dispatches its
    /// checkpoint votes to the certifier, and returns how many transactions were executed
    /// together with the checkpoint this sub-dag minted, if any. Undecodable payloads are
    /// skipped; the choice is deterministic. Sub-dags that execute nothing are not
    /// checkpointed: their commitment is unchanged, and their count varies across validators
    /// at any executed-transaction cut.
    fn execute_subdag(
        scheduler: &mut SequentialScheduler<E>,
        certifier: &mut CheckpointCertifier,
        mut subdag: CommittedSubDag,
    ) -> (usize, Option<Checkpoint>) {
        let anchor = subdag.anchor;
        subdag.sort();
        // Votes are attributed to the block author: they ride their author's own signed
        // blocks, so no authority can forge another's vote.
        let mut votes = Vec::new();
        let transactions = subdag
            .blocks
            .iter()
            .flat_map(|block| {
                let author = block.author();
                block
                    .transactions()
                    .iter()
                    .map(move |payload| (author, payload))
            })
            .filter_map(|(author, payload)| {
                let envelope = Envelope::from_bytes(payload.as_bytes())
                    .inspect_err(|error| tracing::warn!(?error, "skipping undecodable payload"))
                    .ok()?;
                match envelope.into_payload() {
                    Payload::Execute(transaction) => Some(transaction),
                    Payload::Attest(vote) => {
                        votes.push((author, vote));
                        None
                    }
                }
            });
        let executed = scheduler.execute(transactions).len();
        let minted = (executed > 0).then(|| {
            certifier
                .push(anchor, scheduler.store().commitment())
                .clone()
        });
        // A vote can never reference its own delivering sub-dag's checkpoint (it is created
        // only after that sub-dag commits), so recording after minting loses nothing.
        for (author, vote) in votes {
            certifier.record(anchor, author, vote);
        }
        (executed, minted)
    }

    /// The pair consumed by [`ValidatorHandle::wait_for_certified`]: the highest certified
    /// commitment beside the store commitment at the same instant.
    fn certification_status(
        certifier: &CheckpointCertifier,
        scheduler: &SequentialScheduler<E>,
    ) -> (Option<Digest>, Digest) {
        let certified = certifier
            .highest_certified()
            .map(|certificate| certificate.checkpoint().commitment());
        (certified, scheduler.store().commitment())
    }
}

/// A handle to a running validator: a consensus replica whose committed transactions are
/// executed by an [`ExecutionEngine`] through a [`SequentialScheduler`].
///
/// The driver task owns the scheduler and every task is spawned through [`Ctx`], so the whole
/// node runs unchanged under tokio and under the mysticeti simulator.
pub struct ValidatorHandle<C: Ctx, E: ExecutionEngine + Send + 'static> {
    replica: ReplicaHandle<C>,
    driver: C::JoinHandle<(SequentialScheduler<E>, CheckpointCertifier)>,
    executed: watch::Receiver<u64>,
    certified: watch::Receiver<(Option<Digest>, Digest)>,
}

impl<C: Ctx, E: ExecutionEngine + Send + 'static> ValidatorHandle<C, E> {
    /// Submits transactions to the replica; resolves once they are queued for inclusion in a
    /// block, not once they are committed or executed. Every payload is stamped with the
    /// submission time (one clock read per batch), from which mysticeti derives its
    /// commit-latency metric.
    pub async fn submit(&self, transactions: Vec<Transaction>) -> eyre::Result<()> {
        let timestamp_ms = C::timestamp_utc().as_millis() as u64;
        let transactions = transactions
            .into_iter()
            .map(|transaction| {
                let envelope = Envelope::new(timestamp_ms, Payload::Execute(transaction));
                ConsensusTransaction::new(envelope.to_bytes().into())
            })
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

    /// Waits until everything executed is certified: the highest certified commitment equals
    /// the store commitment. Equality is transient while submissions are in flight, so this is
    /// meaningful after an executed-count cut ([`wait_for_transactions`]
    /// (ValidatorHandle::wait_for_transactions) first). Returns early if the driver has
    /// stopped.
    pub async fn wait_for_certified(&mut self) {
        loop {
            let (certified, store) = *self.certified.borrow_and_update();
            if certified == Some(store) {
                return;
            }
            if self.certified.changed().await.is_err() {
                return;
            }
        }
    }

    /// Stops the replica, waits for the driver to drain the remaining commits, and returns the
    /// scheduler with the executed state and the checkpoint certifier.
    pub async fn shutdown(self) -> (SequentialScheduler<E>, CheckpointCertifier) {
        // The returned syncer holds the consensus storage; the WAL outlives it on disk, which
        // is what replay on the next start relies on.
        let _syncer = self.replica.shutdown().await;
        self.driver.await.expect("driver task failed")
    }
}

#[cfg(test)]
mod tests {
    use checkpoint::certifier::CheckpointCertifier;
    use dag::{
        authority::Authority,
        block::{Block, data::Data, transaction::Transaction as ConsensusTransaction},
        consensus::CommittedSubDag,
        crypto::CryptoEngine,
    };
    use execution::{
        fake::{FakeExecutor, FakeTransaction},
        object::ObjectId,
        scheduler::SequentialScheduler,
    };

    use crate::{
        envelope::{Envelope, Payload},
        validator::Validator,
    };

    /// A sub-dag at `round` with one block per `(author, payloads)` entry, anchored at the
    /// first block.
    fn subdag_at(round: u64, blocks: Vec<(usize, Vec<Vec<u8>>)>) -> CommittedSubDag {
        let blocks: Vec<_> = blocks
            .into_iter()
            .map(|(author, payloads)| {
                let transactions = payloads
                    .into_iter()
                    .map(|bytes| ConsensusTransaction::new(bytes.into()))
                    .collect();
                let block = Block::new(
                    Authority::from(author),
                    round,
                    vec![],
                    transactions,
                    0,
                    &CryptoEngine::disabled(),
                );
                Data::new(block)
            })
            .collect();
        CommittedSubDag::new(*blocks[0].reference(), blocks)
    }

    /// A single-block sub-dag carrying the given raw payloads.
    fn subdag(payloads: Vec<Vec<u8>>) -> CommittedSubDag {
        subdag_at(1, vec![(0, payloads)])
    }

    #[test]
    fn undecodable_payloads_are_skipped() {
        let mut scheduler = SequentialScheduler::new(FakeExecutor);
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 0);
        let transaction = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let valid = Envelope::new(0, Payload::Execute(transaction.into())).to_bytes();

        let (executed, minted) = Validator::execute_subdag(
            &mut scheduler,
            &mut certifier,
            subdag(vec![vec![0xFF; 16], valid]),
        );

        assert_eq!(executed, 1);
        assert!(minted.is_some());
        assert_eq!(certifier.pending().count(), 1);
    }

    #[test]
    fn subdags_with_only_undecodable_payloads_mint_no_checkpoint() {
        let mut scheduler = SequentialScheduler::new(FakeExecutor);
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 0);

        let (executed, minted) =
            Validator::execute_subdag(&mut scheduler, &mut certifier, subdag(vec![vec![0xFF; 16]]));

        assert_eq!(executed, 0);
        assert!(minted.is_none());
        assert_eq!(certifier.pending().count(), 0);
    }

    #[test]
    fn a_quorum_of_attestations_certifies_and_mints_nothing() {
        let mut scheduler = SequentialScheduler::new(FakeExecutor);
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 0);
        let transaction = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let execute = Envelope::new(0, Payload::Execute(transaction.into())).to_bytes();
        let (_, minted) =
            Validator::execute_subdag(&mut scheduler, &mut certifier, subdag(vec![execute]));
        let checkpoint = minted.expect("executing sub-dag must mint a checkpoint");

        // A quorum of votes delivered in a later sub-dag, one block per author.
        let votes = (0..3)
            .map(|author| {
                let vote = Envelope::new(0, Payload::Attest(checkpoint.clone())).to_bytes();
                (author, vec![vote])
            })
            .collect();
        let delivering = subdag_at(2, votes);
        let anchor = delivering.anchor;
        let (executed, minted) =
            Validator::execute_subdag(&mut scheduler, &mut certifier, delivering);

        assert_eq!(executed, 0);
        assert!(minted.is_none());
        let certified = certifier.highest_certified().expect("quorum must certify");
        assert_eq!(certified.checkpoint(), &checkpoint);
        // The proof records the delivering sub-dag's anchor per counted vote.
        assert_eq!(certified.proof(), [anchor; 3]);
        assert!(certifier.pending().next().is_none());
    }

    #[test]
    fn mixed_subdags_execute_mint_and_certify_together() {
        let mut scheduler = SequentialScheduler::new(FakeExecutor);
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 0);
        let create = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let execute = Envelope::new(0, Payload::Execute(create.into())).to_bytes();
        let (_, minted) =
            Validator::execute_subdag(&mut scheduler, &mut certifier, subdag(vec![execute]));
        let checkpoint = minted.expect("executing sub-dag must mint a checkpoint");

        // Votes for the first checkpoint share the sub-dag with a fresh transaction.
        let vote = || Envelope::new(0, Payload::Attest(checkpoint.clone())).to_bytes();
        let update = FakeTransaction::success(vec![], vec![], vec![ObjectId::new(1)]);
        let update = Envelope::new(0, Payload::Execute(update.into())).to_bytes();
        let blocks = vec![
            (0, vec![vote(), update]),
            (1, vec![vote()]),
            (2, vec![vote()]),
        ];
        let (executed, minted) =
            Validator::execute_subdag(&mut scheduler, &mut certifier, subdag_at(2, blocks));

        assert_eq!(executed, 1);
        assert!(minted.is_some());
        let certified = certifier.highest_certified().expect("quorum must certify");
        assert_eq!(certified.checkpoint(), &checkpoint);
        // The prior checkpoint certified and was reclaimed; only the new mint is pending.
        assert_eq!(certifier.pending().count(), 1);
    }
}
