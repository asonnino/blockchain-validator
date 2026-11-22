// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The n-version commit processor, with the barrier pipeline: a validator executes each commit
//! with its own version, votes on every transaction's result, and executes the next commit only
//! once every transaction of this one is decided.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use checkpoint::certifier::{CheckpointCertifier, CheckpointTimings};
use dag::{
    authority::Authority,
    committee::{Committee, Stake},
    consensus::CommittedSubDag,
    crypto::AsBytes,
};
use execution::{
    effects::{ExecutionOutput, ExecutionStatus},
    engine::{CommitInfo, ExecutionEngine},
    store::{InMemoryStore, Overlay},
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
    byzantine::{Ballot, Byzantine},
    decision::{Decider, Decision},
    digest::{InputDigest, ResultDigest},
    report::{BlockRecord, Outcome, Records, TransactionRecord},
    versions::{VersionId, Versions},
    vote::{Position, Vote},
};

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
    /// The vote this validator cast on it, if it did yet.
    vote: Option<Vote>,
}

/// A validator's [`CommitProcessor`] for n-version execution.
pub struct NVersionProcessor<E> {
    authority: Authority,
    versions: Versions<E>,
    /// The own version.
    version: VersionId,
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
    /// Every decided transaction, recorded or not.
    decided: u64,
    /// Whether to keep [`Records`], which grow with every transaction.
    recording: bool,
    records: Records,
    committee: Arc<Committee>,
    byzantine: Option<Byzantine>,
    /// Late votes, in the order they fall due, with the anchor round they wait for.
    delayed: VecDeque<(u64, Vote)>,
}

impl<E: ExecutionEngine> NVersionProcessor<E> {
    /// `authority` runs its version of `versions`; [`Decider::new`] takes `threshold` and `cutoff`.
    pub fn new(
        authority: Authority,
        versions: Versions<E>,
        threshold: Stake,
        cutoff: u64,
        public_config: &PublicReplicaConfig,
    ) -> eyre::Result<Self> {
        let committee = public_config.committee();
        let protocol = public_config.parameters.consensus.to_protocol(&committee)?;
        assert_eq!(
            versions.assignment().len(),
            committee.len(),
            "one version per authority"
        );
        let version = versions.assignment()[authority.index()];
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(versions.engine(version).genesis()));
        Ok(Self {
            authority,
            versions,
            version,
            store,
            decider: Decider::new(committee.clone(), threshold, cutoff),
            certifier: CheckpointCertifier::new(committee.clone(), protocol.quorum_threshold),
            commit_clock: CommitClock::new(committee.clone()),
            commits: 0,
            queue: VecDeque::new(),
            executing: None,
            live_from: None,
            decided: 0,
            recording: false,
            records: Records::default(),
            committee,
            byzantine: None,
            delayed: VecDeque::new(),
        })
    }

    pub fn with_records(mut self) -> Self {
        self.recording = true;
        self
    }

    /// Makes this validator vote as `byzantine` says. Its authorities must hold less stake than
    /// the threshold, or they could finalize a result no version reproduces, and repair panics.
    pub fn with_byzantine(mut self, byzantine: Byzantine) -> Self {
        self.byzantine = Some(byzantine);
        self
    }

    pub fn version(&self) -> VersionId {
        self.version
    }

    /// Whether to record what happens now: records cover only the commits delivered after replay.
    fn records_live(&self) -> bool {
        self.recording && self.live_from.is_some()
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
                    let truth = self.versions.truth(&self.store, &executed.transaction);
                    let correct = ResultDigest::new(&truth) == result;
                    lost = executed.result != result;
                    let own = executed.input == input && !lost;
                    let output = match executed.output.take() {
                        Some(output) if own => output,
                        _ => {
                            let transaction = &executed.transaction;
                            let (output, tries, spent) =
                                self.versions
                                    .repair(&self.store, transaction, result, &voters);
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
            self.decided += 1;
            let live = self.live_from.is_some_and(|first| position.commit >= first);
            if !self.recording || !live {
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
        let prologue = self
            .versions
            .engine(self.version)
            .begin_commit(&self.store, &queued.info);
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
        let mut delayed = Vec::new();
        let mut transactions = Vec::with_capacity(queued.transactions.len());
        for (index, (transaction, submitted_ms)) in queued.transactions.into_iter().enumerate() {
            let input = InputDigest::new(&overlay, &transaction);
            let (output, cost) = self.versions.execute(self.version, &overlay, &transaction);
            *busy += cost;
            let result = ResultDigest::new(&output);
            let position = Position {
                commit: queued.commit,
                index: index
                    .try_into()
                    .expect("commits hold fewer than 2^32 transactions"),
            };
            let honest = Vote {
                position,
                input,
                result,
            };
            // Byzantine votes see the transaction's input state, before the own output.
            let ballot = match &self.byzantine {
                None => Ballot::Now(honest),
                Some(byzantine) => byzantine.ballot(honest, &transaction, round, || {
                    let committee = &self.committee;
                    self.versions
                        .results(committee, byzantine, &overlay, &transaction)
                }),
            };
            overlay.apply(&output);
            let vote = match ballot {
                Ballot::Now(vote) => {
                    votes.push(Payload::Message(vote));
                    Some(vote)
                }
                Ballot::Due(due, vote) => {
                    delayed.push((due, vote));
                    None
                }
                Ballot::Withheld => None,
            };
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
                vote,
            });
        }
        self.executing = Some(transactions);
        self.delayed.extend(delayed);
        votes
    }
}

impl<E: ExecutionEngine + Send + 'static> CommitProcessor for NVersionProcessor<E> {
    type Message = Vote;

    fn process(
        &mut self,
        mut subdag: CommittedSubDag,
        now: Duration,
        metrics: &ValidatorMetrics,
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
                        if let Some(timings) = self.certifier.record(anchor, author, checkpoint) {
                            let minted = now.saturating_sub(timings.minted_at);
                            metrics.observe_checkpoint_certification_latency(minted);
                            let submitted = Duration::from_millis(timings.mean_timestamp_ms);
                            metrics.observe_end_to_end_latency(now.saturating_sub(submitted));
                        }
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
            if self.records_live() {
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
        payloads.extend(self.due_votes(anchor.round));
        if self.records_live() {
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
        let votes = executing[head.index as usize..]
            .iter()
            .filter_map(|executed| executed.vote.map(Payload::Message));
        pending.chain(votes).collect()
    }

    fn executed(&self) -> u64 {
        self.decided
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
    /// The late votes due at anchor round `round`, noted as cast.
    fn due_votes(&mut self, round: u64) -> Vec<Payload<Vote>> {
        let mut due = Vec::new();
        while let Some(&(at, vote)) = self.delayed.front()
            && at <= round
        {
            self.delayed.pop_front();
            let executed = self
                .executing
                .as_mut()
                .and_then(|executing| executing.get_mut(vote.position.index as usize))
                .filter(|executed| executed.position == vote.position);
            if let Some(executed) = executed {
                executed.vote = Some(vote);
            }
            due.push(Payload::Message(vote));
        }
        due
    }

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
        crypto::Digest,
        effects::ExecutionOutput,
        engine::{CommitInfo, ExecutionEngine},
        object::{Object, ObjectId, Version},
        store::StateView,
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

    use super::NVersionProcessor;
    use crate::{
        byzantine::{Byzantine, Selective, Strategy},
        digest::ResultDigest,
        report::{Outcome, Records},
        versions::{Cluster, CostModel, SyntheticConfig, SyntheticEngine, VersionId, Versions},
        vote::{Position, Vote},
    };

    /// Authority 0 of four correct versions, deciding at 3 of 4 with a cutoff of 2 rounds.
    fn processor() -> NVersionProcessor<FakeExecutor> {
        processor_with(versions(|| FakeExecutor))
    }

    /// Four versions, and the reference, all of `engine`, one per authority.
    fn versions<E: ExecutionEngine>(engine: impl Fn() -> E) -> Versions<E> {
        let engines = (0..4).map(|_| engine()).collect();
        Versions::new(engines, engine(), (0..4).map(VersionId).collect())
    }

    /// [`processor`], over `versions`.
    fn processor_with<E: ExecutionEngine + Send + 'static>(
        versions: Versions<E>,
    ) -> NVersionProcessor<E> {
        let mut processor = replaying_with(versions);
        processor.after_replay();
        processor
    }

    /// [`processor_with`], still replaying.
    fn replaying_with<E: ExecutionEngine>(versions: Versions<E>) -> NVersionProcessor<E> {
        unrecorded_with(versions).with_records()
    }

    /// [`replaying_with`], keeping no records.
    fn unrecorded_with<E: ExecutionEngine>(versions: Versions<E>) -> NVersionProcessor<E> {
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let authority = Authority::from(0usize);
        NVersionProcessor::new(authority, versions, 3, 2, &public_config).unwrap()
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
        let mut processor = processor_with(versions(|| Clocked));
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
        let mut processor = processor_with(versions(|| FakeExecutor).with_cost(cost));
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
        let mut processor = replaying_with(versions(|| FakeExecutor).with_cost(cost));
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
    fn without_records_decisions_still_count() {
        let mut processor = unrecorded_with(versions(|| FakeExecutor));
        processor.after_replay();
        let create = execute(FakeTransaction::success(
            vec![],
            vec![ObjectId::new(1)],
            vec![],
        ));
        let first = process(&mut processor, subdag_at(1, vec![(0, vec![create])]));
        let [Payload::Message(vote)] = &first[..] else {
            panic!("one own vote: {first:?}");
        };
        process(&mut processor, votes_at(2, vote));
        assert_eq!(processor.executed(), 1);
        assert_eq!(processor.into_parts().2, Records::default());
    }

    #[test]
    fn certified_checkpoints_feed_the_latency_metrics() {
        let mut processor = processor();
        let creation = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]);
        let create = Envelope::new(500, Payload::<Vote>::Execute(creation.into())).to_bytes();
        let first = process(&mut processor, subdag_at(1, vec![(0, vec![create])]));
        let [Payload::Message(vote)] = &first[..] else {
            panic!("one own vote: {first:?}");
        };
        let minted = process(&mut processor, votes_at(2, vote));
        let [Payload::Attest(checkpoint)] = &minted[..] else {
            panic!("a checkpoint: {minted:?}");
        };

        // A quorum of attestations certifies it.
        let metrics = ValidatorMetrics::new(&Registry::new());
        let attestations = (1..4)
            .map(|author| {
                let attestation = Payload::<Vote>::Attest(checkpoint.clone());
                (author, vec![Envelope::new(0, attestation).to_bytes()])
            })
            .collect();
        processor.process(subdag_at(3, attestations), Duration::from_secs(3), &metrics);
        // Minted at 2 s and certified at 3 s, for a transaction submitted at 0.5 s.
        let certification = metrics.checkpoint_certification_latency_s();
        assert_eq!(certification.get_sample_count(), 1);
        assert_eq!(certification.get_sample_sum(), 1.0);
        let end_to_end = metrics.end_to_end_latency_s();
        assert_eq!(end_to_end.get_sample_count(), 1);
        assert_eq!(end_to_end.get_sample_sum(), 2.5);
    }

    /// Authority 0, voting as `strategy` on the transactions `selective` picks.
    fn byzantine(
        strategy: Strategy,
        selective: Option<Selective>,
    ) -> NVersionProcessor<FakeExecutor> {
        processor().with_byzantine(Byzantine {
            strategy,
            selective,
            authorities: vec![Authority::from(0usize)],
        })
    }

    /// `count` creations, of distinct objects.
    fn creations(count: u64) -> Vec<Vec<u8>> {
        let creation = |id| FakeTransaction::success(vec![], vec![ObjectId::new(id)], vec![]);
        (1..=count).map(|id| execute(creation(id))).collect()
    }

    #[test]
    fn withheld_votes_are_never_cast() {
        let mut processor = byzantine(Strategy::Withhold, None);
        let released = process(&mut processor, subdag_at(1, vec![(0, creations(2))]));
        assert!(own_votes(&released).is_empty());
        assert!(own_votes(&processor.after_replay()).is_empty());
    }

    #[test]
    fn fixed_votes_carry_the_fixed_digest() {
        let fixed = ResultDigest::fixed(7);
        let mut processor = byzantine(Strategy::Fixed(fixed), None);
        let released = process(&mut processor, subdag_at(1, vec![(0, creations(2))]));
        let votes = own_votes(&released);
        assert_eq!(votes.len(), 2);
        assert!(votes.iter().all(|vote| vote.result == fixed));
        assert_eq!(own_votes(&processor.after_replay()), votes);
    }

    #[test]
    fn late_votes_wait_their_rounds() {
        let mut processor = byzantine(Strategy::Late { rounds: 2 }, None);
        let released = process(&mut processor, subdag_at(1, vec![(0, creations(1))]));
        assert!(own_votes(&released).is_empty());
        let idle = |round| subdag_at(round, vec![(1, Vec::new())]);
        assert!(own_votes(&process(&mut processor, idle(2))).is_empty());
        // Replay resubmits only what was cast.
        assert!(own_votes(&processor.after_replay()).is_empty());
        assert_eq!(own_votes(&process(&mut processor, idle(3))).len(), 1);
        assert_eq!(own_votes(&processor.after_replay()).len(), 1);
    }

    #[test]
    fn votes_late_past_the_last_round_are_never_cast() {
        let mut processor = byzantine(Strategy::Late { rounds: u64::MAX }, None);
        let released = process(&mut processor, subdag_at(1, vec![(0, creations(1))]));
        assert!(own_votes(&released).is_empty());
        assert!(processor.delayed.is_empty());
    }

    #[test]
    fn late_votes_falling_due_after_their_decision_stay_off_later_transactions() {
        let mut late = byzantine(Strategy::Late { rounds: 2 }, None);
        let released = process(&mut late, subdag_at(1, vec![(0, creations(1))]));
        assert!(own_votes(&released).is_empty());
        let mut honest = processor();
        let honest_votes = process(&mut honest, subdag_at(1, vec![(0, creations(1))]));
        let [Payload::Message(vote)] = &honest_votes[..] else {
            panic!("one honest vote: {honest_votes:?}");
        };

        // Authorities 1 to 3 decide the first commit as a second one, also of one transaction, is
        // released; its late vote falls due at round 4.
        let ballot = || Envelope::new(0, Payload::Message(*vote)).to_bytes();
        let second = execute(FakeTransaction::success(
            vec![],
            vec![ObjectId::new(2)],
            vec![],
        ));
        let blocks = vec![
            (0, vec![second]),
            (1, vec![ballot()]),
            (2, vec![ballot()]),
            (3, vec![ballot()]),
        ];
        process(&mut late, subdag_at(2, blocks));

        // The first commit's vote falls due, and isn't taken for the second commit's.
        let due = process(&mut late, subdag_at(3, vec![(1, Vec::new())]));
        let due = own_votes(&due);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].position.commit, 0);
        assert!(own_votes(&late.after_replay()).is_empty());
    }

    /// `authority`'s processor over four synthetic versions, one per authority, of which version 1
    /// is wrong on `transaction` alone.
    fn wrong_on(authority: usize, transaction: Digest) -> NVersionProcessor<SyntheticEngine> {
        let synthetic = |clusters| {
            Arc::new(SyntheticConfig {
                seed: 0,
                failure: vec![0.0; 4],
                clusters,
            })
        };
        let cluster = Cluster {
            versions: vec![VersionId(1)],
            probability: 1.0,
            transactions: Some(vec![transaction]),
        };
        let config = synthetic(vec![cluster]);
        let engines = (0..4)
            .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
            .collect();
        let reference = SyntheticEngine::new(VersionId(0), synthetic(Vec::new()));
        let versions = Versions::new(engines, reference, (0..4).map(VersionId).collect());
        let public_config = PublicReplicaConfig::new_for_tests(4);
        let authority = Authority::from(authority);
        let mut processor =
            NVersionProcessor::new(authority, versions, 3, 2, &public_config).unwrap();
        processor.after_replay();
        processor
    }

    #[test]
    fn colluders_run_every_version_on_the_commits_earlier_outputs() {
        // The update reads the object the commit's creation writes.
        let x = ObjectId::new(1);
        let create = || FakeTransaction::success(vec![], vec![x], vec![]);
        let update = || FakeTransaction::success(vec![], vec![], vec![x]);
        let results = |mut processor: NVersionProcessor<SyntheticEngine>| {
            let commit = vec![(0, vec![execute(create()), execute(update())])];
            let released = process(&mut processor, subdag_at(1, commit));
            let votes = own_votes(&released);
            votes.iter().map(|vote| vote.result).collect::<Vec<_>>()
        };
        let digest = Transaction::from(update()).digest();
        let colluder = wrong_on(0, digest).with_byzantine(Byzantine {
            strategy: Strategy::Collude,
            selective: None,
            authorities: vec![Authority::from(0usize)],
        });
        assert_eq!(results(colluder), results(wrong_on(1, digest)));
    }

    #[test]
    fn selective_byzantines_pick_a_seeded_share() {
        let fixed = ResultDigest::fixed(7);
        let picked = |share, seed| {
            let selective = Selective { share, seed };
            let mut processor = byzantine(Strategy::Fixed(fixed), Some(selective));
            let released = process(&mut processor, subdag_at(1, vec![(0, creations(16))]));
            let votes = own_votes(&released);
            votes
                .iter()
                .map(|vote| vote.result == fixed)
                .collect::<Vec<_>>()
        };
        assert!(picked(0.0, 1).iter().all(|byzantine| !byzantine));
        assert!(picked(1.0, 1).iter().all(|byzantine| *byzantine));
        let half = picked(0.5, 1);
        assert!(half.contains(&true) && half.contains(&false), "{half:?}");
        assert_eq!(half, picked(0.5, 1));
        assert_ne!(half, picked(0.5, 2));
    }
}
