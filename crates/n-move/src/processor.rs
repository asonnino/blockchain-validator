// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The n-version commit processor, with the barrier pipeline: a validator executes each commit
//! with its own version, votes on every transaction's result, and executes the next commit only
//! once every transaction of this one is decided.

use std::{collections::VecDeque, time::Duration};

use checkpoint::certifier::{CheckpointCertifier, CheckpointTimings};
use dag::{authority::Authority, committee::Stake, consensus::CommittedSubDag, crypto::AsBytes};
use execution::{
    crypto::Digest,
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
    decision::{Decider, Decision, Rejection},
    digest::{InputDigest, ResultDigest},
    vote::{Position, Vote},
};

/// The simulated time the processor's executions take, reported as [`Processed::busy`]: a
/// constant per execution plus a cost per unit of gas. Repairs count as executions; ground-truth
/// runs of the reference engine do not. Costs, like the simulated time they add up to, must fit a
/// [`Duration`]: processing panics otherwise.
#[derive(Default)]
pub struct CostModel {
    pub per_execution: Duration,
    pub per_gas: Duration,
}

impl CostModel {
    fn cost(&self, output: &ExecutionOutput) -> Duration {
        const NANOS_PER_SEC: u128 = 1_000_000_000;
        let nanos = self
            .per_gas
            .as_nanos()
            .saturating_mul(u128::from(output.gas_used()));
        let secs = u64::try_from(nanos / NANOS_PER_SEC).expect("costs fit a Duration");
        self.per_execution + Duration::new(secs, (nanos % NANOS_PER_SEC) as u32)
    }
}

/// How a transaction ended, against the reference engine's result on the decided state.
#[derive(PartialEq, Debug)]
pub enum Outcome {
    FinalCorrect,
    FinalWrong,
    Rejected(Rejection),
}

/// One transaction as this validator saw it. Times are milliseconds since the epoch on the
/// validator's clock; processing time counts from the sub-dag that triggered it.
#[derive(PartialEq, Debug)]
pub struct TransactionRecord {
    pub position: Position,
    pub digest: Digest,
    pub submitted_ms: u64,
    pub committed_ms: u64,
    pub executed_ms: u64,
    /// When the own vote committed, unless the transaction was decided before.
    pub voted_ms: Option<u64>,
    pub decided_ms: u64,
    pub outcome: Outcome,
    /// The own result lost.
    pub lost: bool,
    /// The own execution ran on inputs an earlier decision changed.
    pub lost_to_earlier: bool,
    pub repairs: u32,
    /// The own execution's gas.
    pub gas_used: u64,
}

/// The bytes of n-move votes a committed block carries.
#[derive(PartialEq, Debug)]
pub struct BlockRecord {
    pub round: u64,
    pub author: Authority,
    pub vote_bytes: usize,
}

/// What a processor measured over the commits delivered after replay.
#[derive(Default, PartialEq, Debug)]
pub struct Records {
    /// In decision order.
    pub transactions: Vec<TransactionRecord>,
    /// In commit order.
    pub blocks: Vec<BlockRecord>,
    pub busy: Duration,
}

/// A committed sub-dag's transactions, waiting for the barrier.
struct Queued {
    commit: u64,
    info: CommitInfo,
    committed_ms: u64,
    /// With their submission stamps.
    transactions: Vec<(Transaction, u64)>,
}

/// A transaction of the commit being decided, with this validator's own execution of it.
struct Executed {
    position: Position,
    transaction: Transaction,
    input: InputDigest,
    result: ResultDigest,
    /// Taken once the transaction is final.
    output: Option<ExecutionOutput>,
    gas_used: u64,
    submitted_ms: u64,
    committed_ms: u64,
    executed_ms: u64,
    voted_ms: Option<u64>,
}

/// A validator's [`CommitProcessor`] for n-version execution.
pub struct NVersionProcessor<E> {
    authority: Authority,
    /// One engine per version, the own one and the others for repair.
    engines: Vec<E>,
    /// The own version.
    version: VersionId,
    /// Each authority's version.
    assignment: Vec<VersionId>,
    /// Never wrong.
    reference: E,
    cost: CostModel,
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
    /// The first commit delivered after replay, once replay ends.
    live_from: Option<u64>,
    /// The decided transactions of replayed commits, which leave no records.
    replayed: u64,
    records: Records,
}

impl<E: ExecutionEngine> NVersionProcessor<E> {
    /// `authority` runs `engines[assignment[authority]]`; [`Decider::new`] takes `threshold` and
    /// `cutoff`.
    pub fn new(
        authority: Authority,
        engines: Vec<E>,
        reference: E,
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
            authority,
            engines,
            version,
            assignment,
            reference,
            cost: CostModel::default(),
            store,
            decider: Decider::new(committee.clone(), threshold, cutoff),
            certifier: CheckpointCertifier::new(committee.clone(), protocol.quorum_threshold),
            commit_clock: CommitClock::new(committee),
            commits: 0,
            queue: VecDeque::new(),
            executing: None,
            live_from: None,
            replayed: 0,
            records: Records::default(),
        })
    }

    pub fn with_cost(mut self, cost: CostModel) -> Self {
        self.cost = cost;
        self
    }

    /// The final state, the checkpoint certifier, and the records.
    pub fn into_parts(self) -> (InMemoryStore, CheckpointCertifier, Records) {
        (self.store, self.certifier, self.records)
    }

    /// Decides the executing commit's transactions while their votes allow, applying each
    /// decision to the final state, at `now` plus the `busy` time spent so far. Returns whether
    /// the commit is fully decided.
    fn decide(&mut self, now: Duration, busy: &mut Duration) -> bool {
        let Some(executing) = &mut self.executing else {
            return true;
        };
        while let Some(position) = self.decider.head() {
            let executed = &mut executing[position.index as usize];
            let input = InputDigest::new(&self.store, &executed.transaction);
            let Some((_, decision)) = self.decider.decide(input) else {
                return false;
            };
            let (mut lost, mut repairs) = (false, 0);
            let outcome = match decision {
                Decision::Final { result, voters } => {
                    let truth = execute(&self.reference, &self.store, &executed.transaction);
                    let correct = ResultDigest::new(&truth) == result;
                    lost = executed.result != result;
                    let own = executed.input == input && !lost;
                    let output = match executed.output.take() {
                        Some(output) if own => output,
                        _ => {
                            let transaction = &executed.transaction;
                            let (output, tries, spent) = repair(
                                &self.engines,
                                &self.assignment,
                                &self.store,
                                transaction,
                                result,
                                &voters,
                                &self.cost,
                            );
                            (repairs, *busy) = (tries, *busy + spent);
                            output
                        }
                    };
                    self.store.apply(output);
                    if correct {
                        Outcome::FinalCorrect
                    } else {
                        Outcome::FinalWrong
                    }
                }
                Decision::Rejected(rejection) => Outcome::Rejected(rejection),
            };
            if !self.live_from.is_some_and(|first| position.commit >= first) {
                self.replayed += 1;
                continue;
            }
            self.records.transactions.push(TransactionRecord {
                position,
                digest: executed.transaction.digest(),
                submitted_ms: executed.submitted_ms,
                committed_ms: executed.committed_ms,
                executed_ms: executed.executed_ms,
                voted_ms: executed.voted_ms,
                decided_ms: millis(now + *busy),
                outcome,
                lost,
                lost_to_earlier: executed.input != input,
                repairs,
                gas_used: executed.gas_used,
            });
        }
        true
    }

    /// Executes the next queued commit with the own version over the final state, and returns
    /// this validator's votes on its transactions. It registers the commit at `round`, so its
    /// cutoff runs from its execution rather than its commit.
    fn release(
        &mut self,
        queued: Queued,
        round: u64,
        now: Duration,
        busy: &mut Duration,
    ) -> Vec<Payload<Vote>> {
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
        for (index, (transaction, submitted_ms)) in queued.transactions.into_iter().enumerate() {
            let input = InputDigest::new(&overlay, &transaction);
            let output = execute(engine, &overlay, &transaction);
            *busy += self.cost.cost(&output);
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
                position,
                transaction,
                input,
                result,
                gas_used: output.gas_used(),
                output: Some(output),
                submitted_ms,
                committed_ms: queued.committed_ms,
                executed_ms: millis(now + *busy),
                voted_ms: None,
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
            let mut vote_bytes = 0;
            for payload in block.transactions() {
                let bytes = payload.as_bytes();
                let Ok(envelope) = Envelope::<Vote>::from_bytes(bytes)
                    .inspect_err(|error| tracing::warn!(?error, "skipping undecodable payload"))
                else {
                    continue;
                };
                let submitted_ms = envelope.timestamp_ms();
                match envelope.into_payload() {
                    Payload::Execute(transaction) => transactions.push((transaction, submitted_ms)),
                    Payload::Attest(checkpoint) => {
                        self.certifier.record(anchor, author, checkpoint);
                    }
                    Payload::Message(vote) => {
                        vote_bytes += bytes.len();
                        if author == self.authority {
                            self.voted(vote.position, millis(now));
                        }
                        votes.push((author, vote));
                    }
                }
            }
            if self.live_from.is_some() {
                self.records.blocks.push(BlockRecord {
                    round: block.round(),
                    author,
                    vote_bytes,
                });
            }
        }
        self.decider.record(anchor.round, votes);
        self.queue.push_back(Queued {
            commit: self.commits,
            info,
            committed_ms: millis(now),
            transactions,
        });
        self.commits += 1;

        let (mut payloads, mut busy) = (Vec::new(), Duration::ZERO);
        while self.decide(now, &mut busy) {
            if let Some(decided) = self.executing.take() {
                // Clients choose the stamps, so they sum wide.
                let stamps = decided
                    .iter()
                    .map(|executed| u128::from(executed.submitted_ms));
                let timings = CheckpointTimings {
                    minted_at: now + busy,
                    mean_timestamp_ms: (stamps.sum::<u128>() / decided.len() as u128) as u64,
                };
                let commitment = self.store.commitment();
                let checkpoint = self.certifier.push(anchor, commitment, timings).clone();
                payloads.push(Payload::Attest(checkpoint));
            }
            let Some(queued) = self.queue.pop_front() else {
                break;
            };
            if !queued.transactions.is_empty() {
                payloads.extend(self.release(queued, anchor.round, now, &mut busy));
            }
        }
        if self.live_from.is_some() {
            self.records.busy += busy;
        }
        Processed { payloads, busy }
    }

    /// Resubmits the own votes on undecided transactions and the uncertified checkpoints: both
    /// dedupe per author, so the ones that did commit are ignored.
    fn after_replay(&mut self) -> Vec<Payload<Vote>> {
        self.live_from.get_or_insert(self.commits);
        let pending = self.certifier.pending().cloned().map(Payload::Attest);
        let Some((executing, head)) = self.executing.as_ref().zip(self.decider.head()) else {
            return pending.collect();
        };
        let votes = executing[head.index as usize..].iter().map(|executed| {
            Payload::Message(Vote {
                position: executed.position,
                input: executed.input,
                result: executed.result,
            })
        });
        pending.chain(votes).collect()
    }

    fn executed(&self) -> u64 {
        self.replayed + self.records.transactions.len() as u64
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

impl<E> NVersionProcessor<E> {
    /// Notes that the own vote on `position` committed at `now_ms`.
    fn voted(&mut self, position: Position, now_ms: u64) {
        let executed = self
            .executing
            .as_mut()
            .and_then(|executing| executing.get_mut(position.index as usize))
            .filter(|executed| executed.position == position);
        if let Some(executed) = executed {
            executed.voted_ms.get_or_insert(now_ms);
        }
    }
}

fn millis(time: Duration) -> u64 {
    u64::try_from(time.as_millis()).expect("times fit u64 milliseconds")
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
/// Returns the output, the executions tried, and their cost.
fn repair<E: ExecutionEngine>(
    engines: &[E],
    assignment: &[VersionId],
    store: &InMemoryStore,
    transaction: &Transaction,
    result: ResultDigest,
    voters: &[Authority],
    cost: &CostModel,
) -> (ExecutionOutput, u32, Duration) {
    let (mut tried, mut spent) = (Vec::new(), Duration::ZERO);
    for voter in voters {
        let version = assignment[voter.index()];
        if tried.contains(&version) {
            continue;
        }
        tried.push(version);
        let output = execute(&engines[version.0], store, transaction);
        spent += cost.cost(&output);
        if ResultDigest::new(&output) == result {
            return (output, tried.len() as u32, spent);
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
        effects::ExecutionOutput,
        engine::{CommitInfo, ExecutionEngine},
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

    use super::{CostModel, NVersionProcessor, Outcome, repair};
    use crate::{
        assignment::VersionId,
        digest::ResultDigest,
        synthetic::{SyntheticConfig, SyntheticEngine},
        vote::{Position, Vote},
    };

    /// Authority 0 of four correct versions, deciding at 3 of 4 with a cutoff of 2 rounds.
    fn processor() -> NVersionProcessor<FakeExecutor> {
        processor_with(|| FakeExecutor)
    }

    fn processor_with<E: ExecutionEngine + Send + 'static>(
        engine: impl Fn() -> E,
    ) -> NVersionProcessor<E> {
        let mut processor = replaying_with(engine);
        processor.after_replay();
        processor
    }

    /// [`processor_with`], still replaying.
    fn replaying_with<E: ExecutionEngine>(engine: impl Fn() -> E) -> NVersionProcessor<E> {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let assignment = (0..4).map(VersionId).collect();
        let engines = (0..4).map(|_| engine()).collect();
        let authority = Authority::from(0usize);
        NVersionProcessor::new(
            authority,
            engines,
            engine(),
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
        votes_after(round, None, vote)
    }

    /// [`votes_at`], with the anchor's parent stamped `parent_ms` if any.
    fn votes_after(round: u64, parent_ms: Option<u64>, vote: &Vote) -> CommittedSubDag {
        let blocks = (1..4)
            .map(|author| {
                let copy = Vote { ..*vote };
                let payload = Envelope::new(0, Payload::Message(copy)).to_bytes();
                (author, vec![payload])
            })
            .collect();
        subdag_after(round, parent_ms, blocks)
    }

    /// A sub-dag at `round` with one block per `(author, payloads)` entry, anchored at the first.
    fn subdag_at(round: u64, blocks: Vec<(usize, Vec<Vec<u8>>)>) -> CommittedSubDag {
        subdag_after(round, None, blocks)
    }

    /// [`subdag_at`], whose anchor includes a parent from the round below stamped `parent_ms`, if
    /// any: the commit clock times a commit by its anchor's parents.
    fn subdag_after(
        round: u64,
        parent_ms: Option<u64>,
        blocks: Vec<(usize, Vec<Vec<u8>>)>,
    ) -> CommittedSubDag {
        let block = |author: usize, round, includes, payloads: Vec<Vec<u8>>, ms: u64| {
            let transactions = payloads
                .into_iter()
                .map(|bytes| ConsensusTransaction::new(bytes.into()))
                .collect();
            let (author, crypto) = (Authority::from(author), CryptoEngine::disabled());
            Data::new(Block::new(
                author,
                round,
                includes,
                transactions,
                ms * 1_000_000,
                &crypto,
            ))
        };
        let parent = parent_ms.map(|ms| block(3, round - 1, vec![], vec![], ms));
        let includes = parent.iter().map(|parent| *parent.reference()).collect();
        let mut entries = blocks.into_iter();
        let (author, payloads) = entries.next().expect("an anchor");
        let anchor = block(author, round, includes, payloads, round * 1000);
        let reference = *anchor.reference();
        let others =
            entries.map(|(author, payloads)| block(author, round, vec![], payloads, round * 1000));
        let blocks = parent.into_iter().chain([anchor]).chain(others).collect();
        CommittedSubDag::new(reference, blocks)
    }

    fn process<E: ExecutionEngine + Send + 'static>(
        processor: &mut NVersionProcessor<E>,
        subdag: CommittedSubDag,
    ) -> Vec<Payload<Vote>> {
        let metrics = ValidatorMetrics::new(&Registry::new());
        let now = Duration::from_secs(subdag.anchor.round);
        processor.process(subdag, now, &metrics).payloads
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
        // A late own vote on the decided creation leaves the update's record alone.
        let late = Envelope::new(0, Payload::Message(Vote { ..*create_vote })).to_bytes();
        process(&mut processor, subdag_at(4, vec![(0, vec![late])]));
        // Round 5 is the update's deadline, counted from its release rather than its commit.
        process(&mut processor, votes_at(5, update_vote));

        let (store, _, records) = processor.into_parts();
        let outcomes: Vec<_> = records
            .transactions
            .iter()
            .map(|record| &record.outcome)
            .collect();
        assert_eq!(outcomes, [&Outcome::FinalCorrect, &Outcome::FinalCorrect]);
        assert_eq!(store.latest_version(&x), Some(Version::new(2)));
        // The update waited from its commit at 2 s to its release at 3 s; no own vote committed.
        let times: Vec<_> = records
            .transactions
            .iter()
            .map(|record| {
                let stages = (record.committed_ms, record.executed_ms);
                (stages, record.voted_ms, record.decided_ms)
            })
            .collect();
        assert_eq!(
            times,
            [((1000, 1000), None, 3000), ((2000, 3000), None, 5000)]
        );
    }

    /// Starts each commit by stamping [`clock`] with the commit's timestamp, as Move's prologue
    /// sets its clock.
    struct Clocked;

    fn clock() -> ObjectId {
        ObjectId::new(0xc1)
    }

    impl ExecutionEngine for Clocked {
        fn begin_commit<V: StateView>(&self, view: &V, commit: &CommitInfo) -> ExecutionOutput {
            let version = view
                .latest_version(&clock())
                .map_or(Version::new(1), |v| v.next());
            let stamp = commit.timestamp_ms.to_be_bytes().to_vec();
            ExecutionOutput::success(vec![Object::new(clock(), version, stamp)])
        }

        fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
            FakeExecutor.execute(view, transaction)
        }
    }

    #[test]
    fn released_commits_begin_with_their_own_prologue() {
        let mut processor = processor_with(|| Clocked);
        let x = ObjectId::new(1);
        let create = execute(FakeTransaction::success(vec![], vec![x], vec![]));
        let first = process(
            &mut processor,
            subdag_after(2, Some(1000), vec![(0, vec![create])]),
        );
        let [Payload::Message(create_vote)] = &first[..] else {
            panic!("one own vote: {first:?}");
        };
        // Committed at 3 s and queued; it reads the clock as it updates x.
        let update = execute(FakeTransaction::success(vec![clock()], vec![], vec![x]));
        process(
            &mut processor,
            subdag_after(3, Some(3000), vec![(0, vec![update])]),
        );
        // Released at 5 s.
        let released = process(&mut processor, votes_after(4, Some(5000), create_vote));
        let [Payload::Attest(_), Payload::Message(update_vote)] = &released[..] else {
            panic!("a checkpoint and the update's vote: {released:?}");
        };
        let minted = process(&mut processor, votes_at(6, update_vote));
        let [Payload::Attest(checkpoint)] = &minted[..] else {
            panic!("a checkpoint: {minted:?}");
        };

        let (store, _, _) = processor.into_parts();
        let clock = store.latest(&clock()).unwrap();
        // One prologue per released commit, stamped with the queued commit's own time.
        assert_eq!(clock.version(), Version::new(2));
        assert_eq!(clock.contents(), 3000u64.to_be_bytes());
        // The update read the clock its prologue wrote: one past its version 2.
        assert_eq!(store.latest_version(&x), Some(Version::new(3)));
        assert_eq!(checkpoint.commitment(), store.commitment());
    }

    #[test]
    fn executions_cost_busy_time_and_records_follow_it() {
        let cost = CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::ZERO,
        };
        let mut processor = processor().with_cost(cost);
        let metrics = ValidatorMetrics::new(&Registry::new());
        let creations = [1, 2].map(|id| {
            let creation = FakeTransaction::success(vec![], vec![ObjectId::new(id)], vec![]);
            Envelope::new(7, Payload::<Vote>::Execute(creation.into())).to_bytes()
        });
        let subdag = subdag_at(1, vec![(0, creations.into())]);
        let executed = processor.process(subdag, Duration::from_secs(10), &metrics);
        assert_eq!(executed.busy, Duration::from_millis(2));

        // Every authority's votes on both, the own ones included, commit at 20 s.
        let votes: Vec<_> = own_votes(&executed.payloads)
            .into_iter()
            .map(|vote| Envelope::new(0, Payload::Message(Vote { ..*vote })).to_bytes())
            .collect();
        let blocks = (0..4).map(|author| (author, votes.clone())).collect();
        processor.process(subdag_at(2, blocks), Duration::from_secs(20), &metrics);

        let (_, _, records) = processor.into_parts();
        let times: Vec<_> = records
            .transactions
            .iter()
            .map(|record| {
                let stages = (record.submitted_ms, record.committed_ms, record.executed_ms);
                (stages, record.voted_ms, record.decided_ms)
            })
            .collect();
        let decided = (Some(20_000), 20_000);
        let expected = [
            ((7, 10_000, 10_001), decided),
            ((7, 10_000, 10_002), decided),
        ];
        let expected: Vec<_> = expected
            .iter()
            .map(|(stages, (voted, decided))| (*stages, *voted, *decided))
            .collect();
        assert_eq!(times, expected);
        assert_eq!(records.busy, Duration::from_millis(2));
        let blocks: Vec<_> = records
            .blocks
            .iter()
            .map(|block| (block.round, block.author.index(), block.vote_bytes))
            .collect();
        let bytes = votes.iter().map(Vec::len).sum();
        let voting = (0..4).map(|author| (2, author, bytes));
        assert_eq!(
            blocks,
            [(1, 0, 0)].into_iter().chain(voting).collect::<Vec<_>>()
        );

        let gas = ExecutionOutput::success(Vec::new()).with_gas_used(500);
        let cost = CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::from_micros(1),
        };
        assert_eq!(cost.cost(&gas), Duration::from_micros(1500));
        // Costs beyond 2^64 ns stay exact.
        let heavy = ExecutionOutput::success(Vec::new()).with_gas_used(20_000_000_000);
        let per_second = CostModel {
            per_execution: Duration::ZERO,
            per_gas: Duration::from_secs(1),
        };
        assert_eq!(per_second.cost(&heavy), Duration::from_secs(20_000_000_000));
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
    fn replayed_commits_count_but_leave_no_records() {
        let cost = CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::ZERO,
        };
        let mut processor = replaying_with(|| FakeExecutor).with_cost(cost);
        let metrics = ValidatorMetrics::new(&Registry::new());
        let replay = |processor: &mut NVersionProcessor<_>, subdag| {
            processor.process(subdag, Duration::ZERO, &metrics).payloads
        };
        // Replay decides the creation; the update, the last replayed commit (2), executes.
        let (x, y) = (ObjectId::new(1), ObjectId::new(2));
        let create = execute(FakeTransaction::success(vec![], vec![x], vec![]));
        let first = replay(&mut processor, subdag_at(1, vec![(0, vec![create])]));
        let [Payload::Message(create_vote)] = &first[..] else {
            panic!("one own vote: {first:?}");
        };
        replay(&mut processor, votes_at(2, create_vote));
        let update = execute(FakeTransaction::success(vec![], vec![], vec![x]));
        let released = replay(&mut processor, subdag_at(3, vec![(0, vec![update])]));
        let [Payload::Message(update_vote)] = &released[..] else {
            panic!("the update's vote: {released:?}");
        };

        // The first live commit (3) waits for the update, which decides live: only the live
        // commit leaves records.
        processor.after_replay();
        let create = execute(FakeTransaction::success(vec![], vec![y], vec![]));
        process(&mut processor, subdag_at(4, vec![(0, vec![create])]));
        let live = process(&mut processor, votes_at(5, update_vote));
        process(&mut processor, votes_at(6, own_votes(&live)[0]));

        assert_eq!(processor.executed(), 3);
        let (_, _, records) = processor.into_parts();
        let positions: Vec<_> = records
            .transactions
            .iter()
            .map(|record| record.position)
            .collect();
        assert_eq!(
            positions,
            [Position {
                commit: 3,
                index: 0
            }]
        );
        assert_eq!(records.busy, Duration::from_millis(1));
        let rounds: Vec<_> = records.blocks.iter().map(|block| block.round).collect();
        assert_eq!(rounds, [4, 5, 5, 5, 6, 6, 6]);
    }

    #[test]
    fn checkpoints_take_the_mean_of_any_stamps() {
        let mut processor = processor();
        let creations = [(1, u64::MAX), (2, 1)].map(|(id, stamp_ms)| {
            let creation = FakeTransaction::success(vec![], vec![ObjectId::new(id)], vec![]);
            Envelope::new(stamp_ms, Payload::<Vote>::Execute(creation.into())).to_bytes()
        });
        let votes = process(&mut processor, subdag_at(1, vec![(0, creations.into())]));
        let [first, second] = &own_votes(&votes)[..] else {
            panic!("two own votes: {votes:?}");
        };
        process(&mut processor, votes_at(2, first));
        let minted = process(&mut processor, votes_at(3, second));
        let [Payload::Attest(checkpoint)] = &minted[..] else {
            panic!("a checkpoint: {minted:?}");
        };

        let (_, mut certifier, _) = processor.into_parts();
        let anchor = votes_at(4, first).anchor;
        let timings = (0..3usize).find_map(|author| {
            certifier.record(anchor, Authority::from(author), checkpoint.clone())
        });
        assert_eq!(timings.unwrap().mean_timestamp_ms, 1 << 63);
    }

    #[test]
    fn repair_skips_versions_that_do_not_reproduce_the_result() {
        let (store, transaction, result, engines) = repair_setup();
        let voters = [Authority::from(0usize), Authority::from(1usize)];
        let assignment = [VersionId(0), VersionId(1)];
        let cost = CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::ZERO,
        };
        let (output, tries, spent) = repair(
            &engines,
            &assignment,
            &store,
            &transaction,
            result,
            &voters,
            &cost,
        );
        assert_eq!(ResultDigest::new(&output), result);
        assert_eq!((tries, spent), (2, Duration::from_millis(2)));
    }

    #[test]
    #[should_panic(expected = "no voter's version reproduces the final result")]
    fn repair_never_applies_another_result() {
        let (store, transaction, result, engines) = repair_setup();
        let assignment = [VersionId(0), VersionId(1)];
        let voters = [Authority::from(0usize)];
        let cost = CostModel::default();
        repair(
            &engines,
            &assignment,
            &store,
            &transaction,
            result,
            &voters,
            &cost,
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
        store.apply(ExecutionOutput::success(vec![object]));
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
