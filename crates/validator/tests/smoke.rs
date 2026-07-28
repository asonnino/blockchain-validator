// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end validator test on the local tokio network.
//!
//! Port offsets across test binaries: mysticeti uses 0-500; this crate uses 3000.

use std::{path::Path, time::Duration};

use dag::{authority::Authority, context::TokioCtx};
use execution::{
    fake::{FakeExecutor, FakeTransaction},
    object::{ObjectId, Version},
    store::StateView,
};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use tokio::time;
use validator::validator::ValidatorBuilder;

const COMMITTEE_SIZE: usize = 4;
const PORT_OFFSET: u16 = 3000;

#[tokio::test]
async fn submitted_transactions_execute_identically_on_all_validators() {
    let public_config =
        PublicReplicaConfig::new_for_tests(COMMITTEE_SIZE).with_port_offset(PORT_OFFSET);
    // The storage path is unused with ephemeral storage; the keys match the public config
    // because both helpers derive them from the same seed.
    let private_configs =
        PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), COMMITTEE_SIZE);

    let mut validators = Vec::with_capacity(COMMITTEE_SIZE);
    for (index, private_config) in private_configs.into_iter().enumerate() {
        let validator = ValidatorBuilder::new(
            FakeExecutor,
            Authority::from(index),
            public_config.clone(),
            private_config,
        )
        .with_storage(StorageKind::Ephemeral)
        .build()
        .start::<TokioCtx>()
        .await
        .expect("validator must start");
        validators.push(validator);
    }

    // One batch, so block order (and thus commit order) executes the creation before the
    // read-modify-write.
    let id = ObjectId::new(1);
    validators[0]
        .submit(vec![
            FakeTransaction::success(vec![], vec![id], vec![]).into(),
            FakeTransaction::success(vec![], vec![], vec![id]).into(),
        ])
        .await
        .expect("submission must succeed");

    for validator in &mut validators {
        time::timeout(Duration::from_secs(30), validator.wait_for_transactions(2))
            .await
            .expect("timed out waiting for transactions to execute");
    }

    let mut schedulers = Vec::with_capacity(COMMITTEE_SIZE);
    for validator in validators {
        schedulers.push(validator.shutdown().await);
    }

    let store = schedulers[0].store();
    for scheduler in &schedulers {
        assert_eq!(scheduler.store(), store);
    }
    let latest = store.latest(&id).expect("object must exist");
    assert_eq!(latest.version(), Version::new(2));
    assert_eq!(latest.contents(), FakeExecutor::NEW_OBJECT_CONTENT);
}
