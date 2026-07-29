// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Full-committee restart: execution state is rebuilt by replaying the consensus WAL.
//!
//! Port offsets across test binaries: mysticeti uses 0-500, smoke.rs uses 3000, and this file
//! uses 3100/3200 (a fresh offset per start avoids rebinding ports still in TIME_WAIT).

mod common;

use common::Testbed;
use execution::{
    fake::FakeTransaction,
    object::{ObjectId, Version},
    store::StateView,
};

const STATE_UPDATES: usize = 3;

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
    let references = testbed.shutdown().await;

    // Restart on the same WALs: replay must rebuild the executed count without new commits.
    let mut testbed = Testbed::start_with_wal(dir.path(), 3200).await;
    testbed.wait_for_transactions(total).await;

    // The restarted committee stays live: one more update commits and executes everywhere.
    let update = FakeTransaction::success(vec![], vec![], vec![id]).into();
    testbed.submit(0, vec![update]).await;
    testbed.wait_for_transactions(total + 1).await;
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
        // Without vote submission every minted checkpoint stays pending. The reference
        // checkpoints are a prefix of the rebuilt ones: replay reproduced them, then the
        // post-restart update appended. Prefix rather than equality guards the edge where the
        // WAL holds a final commit the pre-shutdown driver never received.
        let reference_count = reference_certifier.pending().count();
        assert!(rebuilt_certifier.pending().count() > reference_count);
        assert!(
            reference_certifier
                .pending()
                .eq(rebuilt_certifier.pending().take(reference_count))
        );
    }
    let (first, first_certifier) = &rebuilt[0];
    let store = first.store();
    for (scheduler, certifier) in &rebuilt {
        assert_eq!(scheduler.store(), store);
        assert!(certifier.pending().eq(first_certifier.pending()));
    }
}
