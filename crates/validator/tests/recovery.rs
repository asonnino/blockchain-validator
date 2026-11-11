// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Full-committee restart: execution state is rebuilt by replaying the consensus WAL.
//!
//! Port offsets across test binaries: mysticeti uses 0-500, smoke.rs uses 3000, and this file
//! uses 3100-3700 (a fresh offset per start avoids rebinding ports still in TIME_WAIT). It skips
//! 3500, which lands on port 5000, held by the macOS AirPlay receiver.

mod common;

use std::{iter, time::Duration};

use common::Testbed;
use dag::consensus::CommittedSubDag;
use execution::{
    fake::{FakeExecutor, FakeTransaction},
    object::{ObjectId, Version},
    store::StateView,
};
use validator::{
    envelope::{NoMessage, Payload},
    metrics::ValidatorMetrics,
    processor::{CommitProcessor, SequentialProcessor},
};

const STATE_UPDATES: usize = 3;

/// The default processor with every vote withheld, so no checkpoint ever certifies.
struct Mute(SequentialProcessor<FakeExecutor>);

impl CommitProcessor for Mute {
    type Message = NoMessage;

    fn process(
        &mut self,
        subdag: CommittedSubDag,
        now: Duration,
        metrics: &ValidatorMetrics,
    ) -> Vec<Payload<NoMessage>> {
        self.0.process(subdag, now, metrics);
        Vec::new()
    }

    fn after_replay(&mut self) -> Vec<Payload<NoMessage>> {
        Vec::new()
    }

    fn executed(&self) -> u64 {
        self.0.executed()
    }

    fn is_certified(&self) -> bool {
        self.0.is_certified()
    }
}

#[tokio::test]
async fn restarted_committee_recovers_execution_state() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let id = ObjectId::new(1);
    let total = (STATE_UPDATES + 1) as u64;

    // First run: create an object and update it a few times.
    let mut testbed = Testbed::start_with_wal(dir.path(), 3100).await;
    let mut transactions = vec![FakeTransaction::success(vec![], vec![id], vec![]).into()];
    transactions.extend(
        (0..STATE_UPDATES).map(|_| FakeTransaction::success(vec![], vec![], vec![id]).into()),
    );
    testbed.submit(0, transactions).await;
    testbed.wait_for_transactions(total).await;
    // Halt only once everything is certified: the run then loses no in-flight votes, so
    // replay alone rebuilds the certified watermark (re-attestation is a separate concern).
    testbed.wait_for_certified().await;
    let references = testbed.shutdown().await;

    // Restart on the same WALs: replay must rebuild the executed count and the certified
    // watermark without new commits — the WAL's post-cut sub-dags carry only attestations,
    // which mint nothing.
    let mut testbed = Testbed::start_with_wal(dir.path(), 3200).await;
    testbed.wait_for_transactions(total).await;
    testbed.wait_for_certified().await;
    // Replay records nothing: the pre-restart certifications must not re-observe.
    for validator in testbed.validators() {
        let certification = validator.metrics().checkpoint_certification_latency_s();
        assert_eq!(certification.get_sample_count(), 0);
    }

    // The restarted committee stays live: one more update commits and executes everywhere.
    let update = FakeTransaction::success(vec![], vec![], vec![id]).into();
    testbed.submit(0, vec![update]).await;
    testbed.wait_for_transactions(total + 1).await;
    testbed.wait_for_certified().await;
    // Exactly the post-restart checkpoint observes.
    for validator in testbed.validators() {
        let certification = validator.metrics().checkpoint_certification_latency_s();
        assert_eq!(certification.get_sample_count(), 1);
    }
    let rebuilt = testbed.shutdown().await;

    assert_eq!(references.len(), rebuilt.len());
    for ((reference, reference_certifier), (rebuilt, rebuilt_certifier)) in
        references.iter().zip(&rebuilt)
    {
        // Replay reproduced the pre-restart history exactly...
        for version in 1..=total {
            assert_eq!(
                rebuilt.store().get(&id, Version::new(version)),
                reference.store().get(&id, Version::new(version)),
            );
        }
        // ...and the post-restart update landed on top of it.
        let latest = rebuilt.store().latest(&id).expect("object must exist");
        assert_eq!(latest.version(), Version::new(total + 1));
        // Replay rebuilt the certified watermark from the WAL's ordered votes, and the
        // post-restart checkpoint certified on top of it.
        let reference_certified = reference_certifier
            .highest_certified()
            .expect("first run halts certified");
        let rebuilt_certified = rebuilt_certifier
            .highest_certified()
            .expect("restarted run halts certified");
        assert_eq!(
            reference_certified.checkpoint().commitment(),
            reference.store().commitment()
        );
        assert_eq!(
            rebuilt_certified.checkpoint().commitment(),
            rebuilt.store().commitment()
        );
        assert!(rebuilt_certified.round() > reference_certified.round());
    }
    let (first, first_certifier) = &rebuilt[0];
    let store = first.store();
    for (scheduler, certifier) in &rebuilt {
        assert_eq!(scheduler.store(), store);
        assert_eq!(
            certifier.highest_certified(),
            first_certifier.highest_certified()
        );
        assert!(certifier.pending().next().is_none());
    }
}

#[tokio::test]
async fn restarted_committee_certifies_the_pending_window() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let id = ObjectId::new(1);
    let total = (STATE_UPDATES + 1) as u64;

    // First run: halt as soon as everything executed, with votes still in flight. Those the
    // replica took sit in its WAL, possibly unproposed (asonnino/mysticeti#221).
    let mut testbed = Testbed::start_with_wal(dir.path(), 3300).await;
    let mut transactions = vec![FakeTransaction::success(vec![], vec![id], vec![]).into()];
    transactions.extend(
        (0..STATE_UPDATES).map(|_| FakeTransaction::success(vec![], vec![], vec![id]).into()),
    );
    testbed.submit(0, transactions).await;
    testbed.wait_for_transactions(total).await;
    let references = testbed.shutdown().await;
    // Without an uncertified window at the halt, the restart below would prove nothing.
    assert!(
        references
            .iter()
            .any(|(_, certifier)| certifier.pending().next().is_some())
    );

    // Restart: replay rebuilds the pending window, and the recovered votes certify it, with
    // re-attestation covering any vote that was lost.
    let mut testbed = Testbed::start_with_wal(dir.path(), 3400).await;
    testbed.wait_for_transactions(total).await;
    testbed.wait_for_certified().await;
    let rebuilt = testbed.shutdown().await;

    let (first, first_certifier) = &rebuilt[0];
    let store = first.store();
    let certified = first_certifier
        .highest_certified()
        .expect("everything executed is certified");
    assert_eq!(certified.checkpoint().commitment(), store.commitment());
    for (scheduler, certifier) in &rebuilt {
        assert_eq!(scheduler.store(), store);
        assert_eq!(
            certifier.highest_certified(),
            first_certifier.highest_certified()
        );
        assert!(certifier.pending().next().is_none());
    }
}

#[tokio::test]
async fn restarted_committee_attests_replayed_checkpoints() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let id = ObjectId::new(1);
    let total = (STATE_UPDATES + 1) as u64;

    // First run: votes withheld. One transaction per sub-dag mints one checkpoint each, so a
    // partial resubmission below leaves the contiguous watermark short.
    let mute = |config: &_| Mute(common::default_processor(config));
    let mut testbed = Testbed::boot(Some(dir.path()), 3600, mute).await;
    let create = FakeTransaction::success(vec![], vec![id], vec![]);
    let updates = (0..STATE_UPDATES).map(|_| FakeTransaction::success(vec![], vec![], vec![id]));
    for (executed, transaction) in iter::once(create).chain(updates).enumerate() {
        testbed.submit(0, vec![transaction.into()]).await;
        testbed.wait_for_transactions(executed as u64 + 1).await;
    }
    for Mute(processor) in testbed.stop().await {
        let (_, certifier) = processor.into_parts();
        assert!(certifier.highest_certified().is_none());
        assert_eq!(certifier.pending().count(), total as usize);
    }

    // Restart without new transactions: only re-attestation can certify the replayed window.
    let mut testbed = Testbed::start_with_wal(dir.path(), 3700).await;
    testbed.wait_for_transactions(total).await;
    testbed.wait_for_certified().await;
    let rebuilt = testbed.shutdown().await;

    let (first, first_certifier) = &rebuilt[0];
    let store = first.store();
    let certified = first_certifier
        .highest_certified()
        .expect("everything executed is certified");
    assert_eq!(certified.checkpoint().commitment(), store.commitment());
    for (scheduler, certifier) in &rebuilt {
        assert_eq!(scheduler.store(), store);
        assert_eq!(certifier.highest_certified(), Some(certified));
        assert!(certifier.pending().next().is_none());
    }
}
