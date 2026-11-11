// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Commit processors: what a validator does with each committed sub-dag.

use std::time::Duration;

use checkpoint::{
    certifier::{CheckpointCertifier, CheckpointTimings},
    checkpoint::Checkpoint,
};
use dag::{consensus::CommittedSubDag, crypto::AsBytes};
use execution::{engine::ExecutionEngine, scheduler::SequentialScheduler};
use replica::config::PublicReplicaConfig;
use serde::Serialize;

use crate::{
    envelope::{Envelope, NoMessage, Payload},
    metrics::ValidatorMetrics,
};

/// Consumes committed sub-dags in commit order, starting with those replayed from the WAL.
/// Processors run under the simulator, so they must be deterministic and synchronous: no
/// threads, disk, wall clock, or ambient RNG.
pub trait CommitProcessor: Send + 'static {
    /// The processor's own consensus messages, carried as [`Payload::Message`].
    type Message: Serialize;

    /// Processes one committed sub-dag and returns the payloads this validator must submit
    /// through consensus. During replay the payloads are dropped, `now` is zero, and `metrics`
    /// is a throwaway sink; otherwise `now` is the validator's clock as time since epoch.
    fn process(
        &mut self,
        subdag: CommittedSubDag,
        now: Duration,
        metrics: &ValidatorMetrics,
    ) -> Vec<Payload<Self::Message>>;

    /// The payloads to submit once replay is done, before the first live sub-dag. Submissions
    /// still queued when the replica stopped are lost, so this is where to resubmit any that
    /// matter.
    fn after_replay(&mut self) -> Vec<Payload<Self::Message>>;

    /// The number of transactions executed so far.
    fn executed(&self) -> u64;

    /// Whether everything executed so far is certified.
    fn is_certified(&self) -> bool;
}

/// The default processor: executes each sub-dag through a [`SequentialScheduler`], mints
/// checkpoints and attests to them through consensus, and certifies checkpoints from the
/// committed votes.
pub struct SequentialProcessor<E> {
    scheduler: SequentialScheduler<E>,
    certifier: CheckpointCertifier,
    executed: u64,
}

impl<E: ExecutionEngine> SequentialProcessor<E> {
    pub fn new(engine: E, public_config: &PublicReplicaConfig) -> eyre::Result<Self> {
        let committee = public_config.committee();
        let protocol = public_config.parameters.consensus.to_protocol(&committee)?;
        Ok(Self {
            scheduler: SequentialScheduler::new(engine),
            certifier: CheckpointCertifier::new(committee, protocol.quorum_threshold),
            executed: 0,
        })
    }

    /// The scheduler with the executed state, and the checkpoint certifier.
    pub fn into_parts(self) -> (SequentialScheduler<E>, CheckpointCertifier) {
        (self.scheduler, self.certifier)
    }

    /// Executes every transaction of a committed sub-dag in canonical order, dispatches its
    /// checkpoint votes to the certifier, and returns how many transactions were executed
    /// together with the checkpoint this sub-dag minted, if any. Undecodable payloads are
    /// skipped; the choice is deterministic. Sub-dags that execute nothing are not
    /// checkpointed: their commitment is unchanged, and their count varies across validators
    /// at any executed-transaction cut. `now` stamps the mint and closes the latency metrics
    /// of checkpoints certified by this sub-dag's votes.
    fn execute_subdag(
        &mut self,
        metrics: &ValidatorMetrics,
        now: Duration,
        mut subdag: CommittedSubDag,
    ) -> (usize, Option<Checkpoint>) {
        let anchor = subdag.anchor;
        subdag.sort();
        // Votes are recorded as they decode, before this sub-dag mints: none can reference its
        // own delivering sub-dag's checkpoint, which only exists once that sub-dag committed.
        let certifier = &mut self.certifier;
        // Submission stamps reduce to their sum here, while the payloads are in hand.
        let mut stamp_sum_ms = 0u64;
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
                let envelope = Envelope::<NoMessage>::from_bytes(payload.as_bytes())
                    .inspect_err(|error| tracing::warn!(?error, "skipping undecodable payload"))
                    .ok()?;
                let stamp_ms = envelope.timestamp_ms();
                match envelope.into_payload() {
                    Payload::Execute(transaction) => {
                        stamp_sum_ms += stamp_ms;
                        Some(transaction)
                    }
                    // Votes are attributed to the block author: they ride their author's own
                    // signed blocks, so no authority can forge another's vote.
                    Payload::Attest(vote) => {
                        if let Some(timings) = certifier.record(anchor, author, vote) {
                            metrics.observe_checkpoint_certification_latency(
                                now.saturating_sub(timings.minted_at),
                            );
                            let submitted = Duration::from_millis(timings.mean_timestamp_ms);
                            metrics.observe_end_to_end_latency(now.saturating_sub(submitted));
                        }
                        None
                    }
                    Payload::Message(message) => match message {},
                }
            });
        let executed = self.scheduler.execute(transactions).len();
        let minted = (executed > 0).then(|| {
            let timings = CheckpointTimings {
                minted_at: now,
                mean_timestamp_ms: stamp_sum_ms / executed as u64,
            };
            self.certifier
                .push(anchor, self.scheduler.store().commitment(), timings)
                .clone()
        });
        (executed, minted)
    }
}

impl<E: ExecutionEngine + Send + 'static> CommitProcessor for SequentialProcessor<E> {
    type Message = NoMessage;

    fn process(
        &mut self,
        subdag: CommittedSubDag,
        now: Duration,
        metrics: &ValidatorMetrics,
    ) -> Vec<Payload<NoMessage>> {
        let (executed, minted) = self.execute_subdag(metrics, now, subdag);
        self.executed += executed as u64;
        minted.map(Payload::Attest).into_iter().collect()
    }

    /// Re-attests the checkpoints still uncertified after replay: a validator otherwise attests
    /// only once, so a lost vote could stall a checkpoint forever. The certifier's per-author
    /// dedupe makes resubmission idempotent: votes that did commit are simply ignored.
    fn after_replay(&mut self) -> Vec<Payload<NoMessage>> {
        self.certifier
            .pending()
            .cloned()
            .map(Payload::Attest)
            .collect()
    }

    fn executed(&self) -> u64 {
        self.executed
    }

    /// The highest certified commitment equals the store commitment.
    fn is_certified(&self) -> bool {
        let certified = self
            .certifier
            .highest_certified()
            .map(|certificate| certificate.checkpoint().commitment());
        certified == Some(self.scheduler.store().commitment())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

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
    use prometheus::Registry;

    use crate::{
        envelope::{NoMessage, Payload},
        metrics::ValidatorMetrics,
        processor::SequentialProcessor,
    };

    type Envelope = crate::envelope::Envelope<NoMessage>;

    fn state() -> SequentialProcessor<FakeExecutor> {
        SequentialProcessor {
            scheduler: SequentialScheduler::new(FakeExecutor),
            certifier: CheckpointCertifier::new_for_test(vec![1; 4], 3, 0),
            executed: 0,
        }
    }

    fn metrics() -> Arc<ValidatorMetrics> {
        ValidatorMetrics::new(&Registry::new())
    }

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
        let mut state = state();
        let transaction = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let valid = Envelope::new(0, Payload::Execute(transaction.into())).to_bytes();

        let (executed, minted) = state.execute_subdag(
            &metrics(),
            Duration::ZERO,
            subdag(vec![vec![0xFF; 16], valid]),
        );

        assert_eq!(executed, 1);
        assert!(minted.is_some());
        assert_eq!(state.certifier.pending().count(), 1);
    }

    #[test]
    fn subdags_with_only_undecodable_payloads_mint_no_checkpoint() {
        let mut state = state();

        let (executed, minted) =
            state.execute_subdag(&metrics(), Duration::ZERO, subdag(vec![vec![0xFF; 16]]));

        assert_eq!(executed, 0);
        assert!(minted.is_none());
        assert_eq!(state.certifier.pending().count(), 0);
    }

    #[test]
    fn a_quorum_of_attestations_certifies_and_mints_nothing() {
        let mut state = state();
        let metrics = metrics();
        // Two transactions with distinct stamps pin the mean reduction: (1000 + 3000) / 2.
        let executes = [1, 2]
            .map(ObjectId::new)
            .into_iter()
            .zip([1000, 3000])
            .map(|(id, stamp_ms)| {
                let transaction = FakeTransaction::success(vec![], vec![id], vec![]);
                Envelope::new(stamp_ms, Payload::Execute(transaction.into())).to_bytes()
            })
            .collect();
        let (_, minted) = state.execute_subdag(&metrics, Duration::from_secs(1), subdag(executes));
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
        let (executed, minted) = state.execute_subdag(&metrics, Duration::from_secs(4), delivering);

        assert_eq!(executed, 0);
        assert!(minted.is_none());
        let certified = state
            .certifier
            .highest_certified()
            .expect("quorum must certify");
        assert_eq!(certified.checkpoint(), &checkpoint);
        // The proof records the delivering sub-dag's anchor per counted vote.
        assert_eq!(certified.proof(), [anchor; 3]);
        assert!(state.certifier.pending().next().is_none());
        // Certification closed the latency metrics: minted at 1s and certified at 4s, with a
        // mean submission stamp of 2s.
        let certification = metrics.checkpoint_certification_latency_s();
        assert_eq!(certification.get_sample_count(), 1);
        assert_eq!(certification.get_sample_sum(), 3.0);
        let end_to_end = metrics.end_to_end_latency_s();
        assert_eq!(end_to_end.get_sample_count(), 1);
        assert_eq!(end_to_end.get_sample_sum(), 2.0);
    }

    #[test]
    fn mixed_subdags_execute_mint_and_certify_together() {
        let mut state = state();
        let metrics = metrics();
        let create = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let execute = Envelope::new(0, Payload::Execute(create.into())).to_bytes();
        let (_, minted) = state.execute_subdag(&metrics, Duration::ZERO, subdag(vec![execute]));
        let checkpoint = minted.expect("executing sub-dag must mint a checkpoint");

        // Votes for the first checkpoint share the sub-dag with a fresh transaction; the vote
        // stamps must stay out of the new checkpoint's mean.
        let vote = || Envelope::new(7777, Payload::Attest(checkpoint.clone())).to_bytes();
        let update = FakeTransaction::success(vec![], vec![], vec![ObjectId::new(1)]);
        let update = Envelope::new(500, Payload::Execute(update.into())).to_bytes();
        let blocks = vec![
            (0, vec![vote(), update]),
            (1, vec![vote()]),
            (2, vec![vote()]),
        ];
        let (executed, minted) =
            state.execute_subdag(&metrics, Duration::from_secs(2), subdag_at(2, blocks));

        assert_eq!(executed, 1);
        let second = minted.expect("the update must mint a second checkpoint");
        let certified = state
            .certifier
            .highest_certified()
            .expect("quorum must certify");
        assert_eq!(certified.checkpoint(), &checkpoint);
        // The prior checkpoint certified and was reclaimed; only the new mint is pending.
        assert_eq!(state.certifier.pending().count(), 1);

        // Certifying the second checkpoint closes its metrics from the update's stamp alone:
        // minted at 2s from a 0.5s submission, certified at 6s.
        let votes = (0..3)
            .map(|author| {
                let vote = Envelope::new(0, Payload::Attest(second.clone())).to_bytes();
                (author, vec![vote])
            })
            .collect();
        state.execute_subdag(&metrics, Duration::from_secs(6), subdag_at(3, votes));
        let certification = metrics.checkpoint_certification_latency_s();
        assert_eq!(certification.get_sample_count(), 2);
        assert_eq!(certification.get_sample_sum(), 2.0 + 4.0);
        let end_to_end = metrics.end_to_end_latency_s();
        assert_eq!(end_to_end.get_sample_count(), 2);
        assert_eq!(end_to_end.get_sample_sum(), 2.0 + 5.5);
    }
}
