// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The n-version commit processor, with the barrier pipeline: a validator executes each commit
//! with its own version, votes on every transaction's result, and executes the next commit only
//! once every transaction of this one is decided.

use std::{collections::VecDeque, time::Duration};

use checkpoint::certifier::{CheckpointCertifier, CheckpointTimings};
use dag::{authority::Authority, committee::Stake, consensus::CommittedSubDag, crypto::AsBytes};
use execution::{
    effects::{AbortReason, ExecutionOutput, ExecutionStatus},
    engine::{CommitInfo, ExecutionEngine},
    store::{InMemoryStore, Overlay, StateView},
    transaction::Transaction,
};
use replica::config::PublicReplicaConfig;
use validator::{
    envelope::{Envelope, Payload},
    metrics::ValidatorMetrics,
    processor::{CommitProcessor, Processed},
    time::CommitClock,
};

use crate::{
    assignment::VersionId,
    decision::{Decider, Decision},
    digest::{InputDigest, ResultDigest},
    vote::{Position, Vote},
};

/// A committed sub-dag's transactions, waiting for the barrier.
struct Queued {
    commit: u64,
    info: CommitInfo,
    transactions: Vec<Transaction>,
}

/// A transaction of the commit being decided, with this validator's own execution of it.
struct Executed {
    transaction: Transaction,
    input: InputDigest,
    result: ResultDigest,
    /// Taken once the transaction is final.
    output: Option<ExecutionOutput>,
}

/// A validator's [`CommitProcessor`] for n-version execution.
pub struct NVersionProcessor<E> {
    /// One engine per version, the own one and the others for repair.
    engines: Vec<E>,
    /// The own version.
    version: VersionId,
    /// Each authority's version.
    assignment: Vec<VersionId>,
    /// The final state: decided results only.
    store: InMemoryStore,
    decider: Decider,
    certifier: CheckpointCertifier,
    commit_clock: CommitClock,
    /// The committed sub-dags seen, which number the commits.
    commits: u64,
    queue: VecDeque<Queued>,
    /// The commit being decided.
    executing: Option<Vec<Executed>>,
    decisions: Vec<(Position, Decision)>,
}

impl<E: ExecutionEngine> NVersionProcessor<E> {
    /// `authority` runs `engines[assignment[authority]]`; [`Decider::new`] takes `threshold` and
    /// `cutoff`.
    pub fn new(
        authority: Authority,
        engines: Vec<E>,
        assignment: Vec<VersionId>,
        threshold: Stake,
        cutoff: u64,
        public_config: &PublicReplicaConfig,
    ) -> eyre::Result<Self> {
        let committee = public_config.committee();
        let protocol = public_config.parameters.consensus.to_protocol(&committee)?;
        assert_eq!(
            assignment.len(),
            committee.len(),
            "one version per authority"
        );
        assert!(
            assignment.iter().all(|version| version.0 < engines.len()),
            "one engine per version"
        );
        let version = assignment[authority.index()];
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(engines[version.0].genesis()));
        Ok(Self {
            engines,
            version,
            assignment,
            store,
            decider: Decider::new(committee.clone(), threshold, cutoff),
            certifier: CheckpointCertifier::new(committee.clone(), protocol.quorum_threshold),
            commit_clock: CommitClock::new(committee),
            commits: 0,
            queue: VecDeque::new(),
            executing: None,
            decisions: Vec::new(),
        })
    }

    /// The final state, the checkpoint certifier, and every decision in commit order.
    pub fn into_parts(
        self,
    ) -> (
        InMemoryStore,
        CheckpointCertifier,
        Vec<(Position, Decision)>,
    ) {
        (self.store, self.certifier, self.decisions)
    }

    /// Decides the executing commit's transactions while their votes allow, applying each
    /// decision to the final state. Returns whether the commit is fully decided.
    fn decide(&mut self) -> bool {
        let Some(executing) = &mut self.executing else {
            return true;
        };
        while let Some(position) = self.decider.head() {
            let executed = &mut executing[position.index as usize];
            let input = InputDigest::new(&self.store, &executed.transaction);
            let Some((_, decision)) = self.decider.decide(input) else {
                return false;
            };
            if let Decision::Final { result, voters } = &decision {
                let own = executed.input == input && executed.result == *result;
                let output = match executed.output.take() {
                    Some(output) if own => output,
                    _ => {
                        let transaction = &executed.transaction;
                        let (engines, assignment) = (&self.engines, &self.assignment);
                        repair(
                            engines,
                            assignment,
                            &self.store,
                            transaction,
                            *result,
                            voters,
                        )
                    }
                };
                self.store.apply(output);
            }
            self.decisions.push((position, decision));
        }
        true
    }

    /// Executes the next queued commit with the own version over the final state, and returns
    /// this validator's votes on its transactions. It registers the commit at `round`, so its
    /// cutoff runs from its execution rather than its commit.
    fn release(&mut self, queued: Queued, round: u64) -> Vec<Payload<Vote>> {
        let engine = &self.engines[self.version.0];
        let prologue = engine.begin_commit(&self.store, &queued.info);
        assert_eq!(
            *prologue.status(),
            ExecutionStatus::Success,
            "commits must begin"
        );
        self.store.apply(prologue);
        self.decider
            .register(queued.commit, queued.transactions.len(), round);

        let mut overlay = Overlay::new(&self.store);
        let mut votes = Vec::with_capacity(queued.transactions.len());
        let mut transactions = Vec::with_capacity(queued.transactions.len());
        for (index, transaction) in queued.transactions.into_iter().enumerate() {
            let input = InputDigest::new(&overlay, &transaction);
            let output = execute(engine, &overlay, &transaction);
            let result = ResultDigest::new(&output);
            overlay.apply(&output);
            let position = Position {
                commit: queued.commit,
                index: index
                    .try_into()
                    .expect("commits hold fewer than 2^32 transactions"),
            };
            votes.push(Payload::Message(Vote {
                position,
                input,
                result,
            }));
            transactions.push(Executed {
                transaction,
                input,
                result,
                output: Some(output),
            });
        }
        self.executing = Some(transactions);
        votes
    }
}

impl<E: ExecutionEngine + Send + 'static> CommitProcessor for NVersionProcessor<E> {
    type Message = Vote;

    fn process(
        &mut self,
        mut subdag: CommittedSubDag,
        now: Duration,
        _metrics: &ValidatorMetrics,
    ) -> Processed<Vote> {
        let anchor = subdag.anchor;
        let info = CommitInfo {
            timestamp_ms: self.commit_clock.commit(&subdag),
        };
        subdag.sort();
        let (mut transactions, mut votes) = (Vec::new(), Vec::new());
        for block in &subdag.blocks {
            let author = block.author();
            for payload in block.transactions() {
                let Ok(envelope) = Envelope::<Vote>::from_bytes(payload.as_bytes())
                    .inspect_err(|error| tracing::warn!(?error, "skipping undecodable payload"))
                else {
                    continue;
                };
                match envelope.into_payload() {
                    Payload::Execute(transaction) => transactions.push(transaction),
                    Payload::Attest(checkpoint) => {
                        self.certifier.record(anchor, author, checkpoint);
                    }
                    Payload::Message(vote) => votes.push((author, vote)),
                }
            }
        }
        self.decider.record(anchor.round, votes);
        self.queue.push_back(Queued {
            commit: self.commits,
            info,
            transactions,
        });
        self.commits += 1;

        let mut payloads = Vec::new();
        while self.decide() {
            if self.executing.take().is_some() {
                // Todo: the submission stamps behind end-to-end latency, with the metrics of
                // MystenLabs/n-version-move#17.
                let timings = CheckpointTimings {
                    minted_at: now,
                    mean_timestamp_ms: 0,
                };
                let commitment = self.store.commitment();
                let checkpoint = self.certifier.push(anchor, commitment, timings).clone();
                payloads.push(Payload::Attest(checkpoint));
            }
            let Some(queued) = self.queue.pop_front() else {
                break;
            };
            if !queued.transactions.is_empty() {
                payloads.extend(self.release(queued, anchor.round));
            }
        }
        Processed {
            payloads,
            busy: Duration::ZERO,
        }
    }

    /// Resubmits the own votes on undecided transactions and the uncertified checkpoints: both
    /// dedupe per author, so the ones that did commit are ignored.
    fn after_replay(&mut self) -> Vec<Payload<Vote>> {
        let pending = self.certifier.pending().cloned().map(Payload::Attest);
        let Some((executing, head)) = self.executing.as_ref().zip(self.decider.head()) else {
            return pending.collect();
        };
        let votes = executing[head.index as usize..]
            .iter()
            .zip(head.index..)
            .map(|(executed, index)| {
                Payload::Message(Vote {
                    position: Position { index, ..head },
                    input: executed.input,
                    result: executed.result,
                })
            });
        pending.chain(votes).collect()
    }

    fn executed(&self) -> u64 {
        self.decisions.len() as u64
    }

    /// The highest certified commitment equals the final state's.
    fn is_certified(&self) -> bool {
        let certified = self
            .certifier
            .highest_certified()
            .map(|certificate| certificate.checkpoint().commitment());
        certified == Some(self.store.commitment())
    }
}

/// `transaction`'s output under `engine`, failing static verification as the scheduler does.
fn execute<E: ExecutionEngine, V: StateView>(
    engine: &E,
    view: &V,
    transaction: &Transaction,
) -> ExecutionOutput {
    if transaction.verify() {
        engine.execute(view, transaction)
    } else {
        ExecutionOutput::aborted(AbortReason::InvalidTransaction)
    }
}

/// Re-executes `transaction` on the final state with each winning voter's version in turn, until
/// one reproduces `result`; a Byzantine voter's version may not, but an honest voter's does.
fn repair<E: ExecutionEngine>(
    engines: &[E],
    assignment: &[VersionId],
    store: &InMemoryStore,
    transaction: &Transaction,
    result: ResultDigest,
    voters: &[Authority],
) -> ExecutionOutput {
    let mut tried = Vec::new();
    for voter in voters {
        let version = assignment[voter.index()];
        if tried.contains(&version) {
            continue;
        }
        tried.push(version);
        let output = execute(&engines[version.0], store, transaction);
        if ResultDigest::new(&output) == result {
            return output;
        }
    }
    panic!("no voter's version reproduces the final result");
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use dag::{
        authority::Authority,
        block::{Block, data::Data, transaction::Transaction as ConsensusTransaction},
        consensus::CommittedSubDag,
        crypto::CryptoEngine,
    };
    use execution::{
        engine::ExecutionEngine,
        object::{Object, ObjectId, Version},
        store::{InMemoryStore, StateView},
        transaction::Transaction,
    };
    use fake_engine::{FakeExecutor, FakeTransaction};
    use prometheus::Registry;
    use replica::config::PublicReplicaConfig;
    use validator::{
        envelope::{Envelope, Payload},
        metrics::ValidatorMetrics,
        processor::CommitProcessor,
    };

    use super::{NVersionProcessor, repair};
    use crate::{
        assignment::VersionId,
        decision::Decision,
        digest::ResultDigest,
        synthetic::{SyntheticConfig, SyntheticEngine},
        vote::{Position, Vote},
    };

    /// Authority 0 of four correct versions, deciding at 3 of 4 with a cutoff of 2 rounds.
    fn processor() -> NVersionProcessor<FakeExecutor> {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let assignment = (0..4).map(VersionId).collect();
        let engines = (0..4).map(|_| FakeExecutor).collect();
        NVersionProcessor::new(
            Authority::from(0usize),
            engines,
            assignment,
            3,
            2,
            &public_config,
        )
        .unwrap()
    }

    fn execute(transaction: FakeTransaction) -> Vec<u8> {
        Envelope::new(0, Payload::<Vote>::Execute(transaction.into())).to_bytes()
    }

    /// `vote` cast by authorities 1 to 3 in a sub-dag at `round`.
    fn votes_at(round: u64, vote: &Vote) -> CommittedSubDag {
        let blocks = (1..4)
            .map(|author| {
                let copy = Vote { ..*vote };
                let payload = Envelope::new(0, Payload::Message(copy)).to_bytes();
                (author, vec![payload])
            })
            .collect();
        subdag_at(round, blocks)
    }

    /// A sub-dag at `round` with one block per `(author, payloads)` entry, anchored at the first.
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
                    round * 1_000_000_000,
                    &CryptoEngine::disabled(),
                );
                Data::new(block)
            })
            .collect();
        CommittedSubDag::new(*blocks[0].reference(), blocks)
    }

    fn process(
        processor: &mut NVersionProcessor<FakeExecutor>,
        subdag: CommittedSubDag,
    ) -> Vec<Payload<Vote>> {
        let metrics = ValidatorMetrics::new(&Registry::new());
        processor.process(subdag, Duration::ZERO, &metrics).payloads
    }

    fn own_votes(payloads: &[Payload<Vote>]) -> Vec<&Vote> {
        payloads
            .iter()
            .filter_map(|payload| match payload {
                Payload::Message(vote) => Some(vote),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn queued_commits_wait_for_the_barrier_and_their_cutoff_starts_there() {
        let mut processor = processor();
        let x = ObjectId::new(1);
        // Undecodable payloads are skipped.
        let create = execute(FakeTransaction::success(vec![], vec![x], vec![]));
        let first = process(
            &mut processor,
            subdag_at(1, vec![(0, vec![create, vec![0xff; 16]])]),
        );
        let [Payload::Message(create_vote)] = &first[..] else {
            panic!("one own vote: {first:?}");
        };
        let update = execute(FakeTransaction::success(vec![], vec![], vec![x]));
        let queued = process(&mut processor, subdag_at(2, vec![(0, vec![update])]));
        assert!(queued.is_empty(), "the update waits: {queued:?}");

        // Deciding the creation releases the update, registered at round 3.
        let released = process(&mut processor, votes_at(3, create_vote));
        let [Payload::Attest(_), Payload::Message(update_vote)] = &released[..] else {
            panic!("a checkpoint and the update's vote: {released:?}");
        };
        assert_eq!(
            update_vote.position,
            Position {
                commit: 1,
                index: 0
            }
        );
        // Round 5 is the update's deadline, counted from its release rather than its commit.
        process(&mut processor, votes_at(5, update_vote));

        let (store, _, decisions) = processor.into_parts();
        assert!(
            decisions
                .iter()
                .all(|(_, decision)| matches!(decision, Decision::Final { .. }))
        );
        assert_eq!(decisions.len(), 2);
        assert_eq!(store.latest_version(&x), Some(Version::new(2)));
    }

    #[test]
    fn replay_resubmits_votes_on_undecided_transactions_and_pending_checkpoints() {
        let mut processor = processor();
        let creations = [1, 2].map(|id| {
            execute(FakeTransaction::success(
                vec![],
                vec![ObjectId::new(id)],
                vec![],
            ))
        });
        let votes = process(&mut processor, subdag_at(1, vec![(0, creations.into())]));
        assert_eq!(processor.after_replay(), votes);

        let [first, second] = &own_votes(&votes)[..] else {
            panic!("two own votes: {votes:?}");
        };
        process(&mut processor, votes_at(2, first));
        let undecided = processor.after_replay();
        assert_eq!(own_votes(&undecided), [*second]);

        let minted = process(&mut processor, votes_at(3, second));
        let [Payload::Attest(_)] = &minted[..] else {
            panic!("a checkpoint: {minted:?}");
        };
        assert_eq!(processor.after_replay(), minted);
    }

    #[test]
    fn repair_skips_versions_that_do_not_reproduce_the_result() {
        let (store, transaction, result, engines) = repair_setup();
        let voters = [Authority::from(0usize), Authority::from(1usize)];
        let assignment = [VersionId(0), VersionId(1)];
        let output = repair(&engines, &assignment, &store, &transaction, result, &voters);
        assert_eq!(ResultDigest::new(&output), result);
    }

    #[test]
    #[should_panic(expected = "no voter's version reproduces the final result")]
    fn repair_never_applies_another_result() {
        let (store, transaction, result, engines) = repair_setup();
        let assignment = [VersionId(0), VersionId(1)];
        repair(
            &engines,
            &assignment,
            &store,
            &transaction,
            result,
            &[Authority::from(0usize)],
        );
    }

    /// Version 0 is always wrong and version 1 correct, on an update of an existing object.
    fn repair_setup() -> (
        InMemoryStore,
        Transaction,
        ResultDigest,
        Vec<SyntheticEngine>,
    ) {
        let id = ObjectId::new(1);
        let mut store = InMemoryStore::default();
        let object = Object::new(id, Version::new(1), b"x".to_vec());
        store.apply(execution::effects::ExecutionOutput::success(vec![object]));
        let transaction: Transaction = FakeTransaction::success(vec![], vec![], vec![id]).into();
        let result = ResultDigest::new(&FakeExecutor.execute(&store, &transaction));
        let config = Arc::new(SyntheticConfig {
            seed: 0,
            failure: vec![1.0, 0.0],
            clusters: Vec::new(),
        });
        let engines = (0..2)
            .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
            .collect();
        (store, transaction, result, engines)
    }
}
