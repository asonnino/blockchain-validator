// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end validator test on the local tokio network.
//!
//! Port offsets across test binaries: mysticeti uses 0-500, this file uses 3000, and
//! recovery.rs uses 3100/3200.

mod common;

use common::Testbed;
use execution::{
    fake::{FakeExecutor, FakeTransaction},
    object::{ObjectId, Version},
    store::StateView,
};

#[tokio::test]
async fn submitted_transactions_execute_identically_on_all_validators() {
    let mut testbed = Testbed::start(3000).await;

    // One batch, so block order (and thus commit order) executes the creation before the
    // read-modify-write.
    let id = ObjectId::new(1);
    testbed
        .submit(
            0,
            vec![
                FakeTransaction::success(vec![], vec![id], vec![]).into(),
                FakeTransaction::success(vec![], vec![], vec![id]).into(),
            ],
        )
        .await;
    testbed.wait_for_transactions(2).await;
    let schedulers = testbed.shutdown().await;

    let store = schedulers[0].store();
    for scheduler in &schedulers {
        assert_eq!(scheduler.store(), store);
    }
    let latest = store.latest(&id).expect("object must exist");
    assert_eq!(latest.version(), Version::new(2));
    assert_eq!(latest.contents(), FakeExecutor::NEW_OBJECT_CONTENT);
}
