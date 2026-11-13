// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The n-version processor on a committee under the mysticeti simulator, each validator running a
//! synthetic version of the fake engine. A correct result keeps every object's contents
//! [`FakeExecutor::NEW_OBJECT_CONTENT`]; a wrong one appends a tag.

use std::{path::Path, pin::pin, sync::Arc, time::Duration};

use checkpoint::certifier::CheckpointCertifier;
use dag::{authority::Authority, context::Ctx, metrics::Metrics};
use execution::{
    object::ObjectId,
    store::{InMemoryStore, StateView},
    transaction::Transaction,
};
use fake_engine::{FakeExecutor, FakeTransaction};
use futures::future::{Either, select};
use n_move::{
    Cluster, Decision, NVersionProcessor, Position, Rejection, SyntheticConfig, SyntheticEngine,
    VersionId, assign,
};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use simulator::{
    LatencyModel, SimulatedNetwork, SimulatorContext, SimulatorExecutor, UniformLatency,
};
use validator::validator::ValidatorBuilder;

const COMMITTEE_SIZE: usize = 4;
const THRESHOLD: u64 = 3;
/// Generous, so only disagreement rejects.
const CUTOFF: u64 = 50;
/// The objects the workload creates, then updates.
const OBJECTS: u64 = 10;
/// Simulated time any wait may take: validators that disagree would otherwise wait forever.
const LIMIT: Duration = Duration::from_secs(60);

/// Every validator's final state, certifier, and decisions.
type Results = Vec<(
    InMemoryStore,
    CheckpointCertifier,
    Vec<(Position, Decision)>,
)>;

/// Runs one synthetic version per authority under `config`, submitting each batch once every
/// validator decided the previous ones, from a different validator each time.
fn run(seed: u64, config: SyntheticConfig, batches: Vec<Vec<Transaction>>) -> Results {
    SimulatorExecutor::run(StdRng::seed_from_u64(seed), async move {
        let public_config = PublicReplicaConfig::new_for_tests(COMMITTEE_SIZE);
        let committee = public_config.committee();
        let latency = LatencyModel::Uniform(UniformLatency { range_ms: 50..100 });
        let (network, node_networks) = SimulatedNetwork::new(&committee, latency);
        let private_configs =
            PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), COMMITTEE_SIZE);
        let config = Arc::new(config);
        let assignment = assign(&committee, COMMITTEE_SIZE, seed);

        let mut validators = Vec::with_capacity(COMMITTEE_SIZE);
        for (index, (node_network, private_config)) in
            node_networks.into_iter().zip(private_configs).enumerate()
        {
            let authority = Authority::from(index);
            let engines = (0..COMMITTEE_SIZE)
                .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
                .collect();
            let processor = NVersionProcessor::new(
                authority,
                engines,
                assignment.clone(),
                THRESHOLD,
                CUTOFF,
                &public_config,
            )
            .expect("processor must build");
            let validator = ValidatorBuilder::with_processor(
                processor,
                authority,
                public_config.clone(),
                private_config,
            )
            .with_storage(StorageKind::Ephemeral)
            .with_simulated_crypto()
            // The default metrics spawn their reporter on tokio, absent here.
            .with_metrics(Metrics::new_for_test(COMMITTEE_SIZE))
            .with_network(node_network)
            .build()
            .start::<SimulatorContext>()
            .await
            .expect("validator must start");
            validators.push(validator);
        }
        network.connect_all().await;

        let mut decided = 0;
        for (index, batch) in batches.into_iter().enumerate() {
            decided += batch.len() as u64;
            let submitter = &validators[index % COMMITTEE_SIZE];
            submitter
                .submit(batch)
                .await
                .expect("submission must succeed");
            for validator in &mut validators {
                within(validator.wait_for_transactions(decided)).await;
            }
        }
        for validator in &mut validators {
            within(validator.wait_for_certified()).await;
        }

        let mut results = Vec::with_capacity(COMMITTEE_SIZE);
        for validator in validators {
            results.push(validator.shutdown().await.into_parts());
        }
        results
    })
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    match select(pin!(future), pin!(SimulatorContext::sleep(LIMIT))).await {
        Either::Left((output, _)) => output,
        Either::Right(_) => panic!("still waiting after {LIMIT:?} of simulated time"),
    }
}

fn config(clusters: Vec<Cluster>) -> SyntheticConfig {
    SyntheticConfig {
        seed: 0,
        failure: vec![0.0; COMMITTEE_SIZE],
        clusters,
    }
}

fn cluster(versions: &[usize], transactions: Option<Vec<&Transaction>>) -> Cluster {
    Cluster {
        versions: versions.iter().map(|version| VersionId(*version)).collect(),
        probability: 1.0,
        transactions: transactions
            .map(|transactions| transactions.iter().map(|tx| tx.digest()).collect()),
    }
}

fn workload() -> Vec<Vec<Transaction>> {
    let ids = (1..=OBJECTS).map(ObjectId::new);
    let creations = ids
        .clone()
        .map(|id| FakeTransaction::success(vec![], vec![id], vec![]).into())
        .collect();
    let updates = ids
        .map(|id| FakeTransaction::success(vec![], vec![], vec![id]).into())
        .collect();
    vec![creations, updates]
}

/// The contents of every workload object in `store`.
fn contents(store: &InMemoryStore) -> Vec<Vec<u8>> {
    (1..=OBJECTS)
        .map(|id| {
            store
                .latest(&ObjectId::new(id))
                .unwrap()
                .contents()
                .to_vec()
        })
        .collect()
}

/// Asserts every validator ended with the same store and certificate, every decision final.
fn assert_agreement(results: &Results) {
    let (store, certifier, decisions) = &results[0];
    assert_eq!(decisions.len() as u64, 2 * OBJECTS);
    assert!(
        decisions
            .iter()
            .all(|(_, decision)| matches!(decision, Decision::Final { .. }))
    );
    let certified = certifier.highest_certified().expect("certified");
    assert_eq!(certified.checkpoint().commitment(), store.commitment());
    for (other_store, other_certifier, other_decisions) in results {
        assert_eq!(other_store, store);
        assert_eq!(other_certifier.highest_certified(), Some(certified));
        assert_eq!(other_decisions, decisions);
    }
}

#[test]
fn correct_versions_finalize_everything_deterministically() {
    let results = run(1, config(Vec::new()), workload());
    assert_agreement(&results);
    let correct = FakeExecutor::NEW_OBJECT_CONTENT;
    assert!(contents(&results[0].0).iter().all(|c| c == correct));

    let again = run(1, config(Vec::new()), workload());
    assert_eq!(again[0].0, results[0].0);
    assert_eq!(again[0].2, results[0].2);
}

#[test]
fn a_wrong_version_loses_and_repairs() {
    let results = run(2, config(vec![cluster(&[0], None)]), workload());
    assert_agreement(&results);
    let correct = FakeExecutor::NEW_OBJECT_CONTENT;
    assert!(contents(&results[0].0).iter().all(|c| c == correct));
}

#[test]
fn a_threshold_of_versions_agreeing_on_a_wrong_result_finalizes_it() {
    let results = run(3, config(vec![cluster(&[0, 1, 2], None)]), workload());
    assert_agreement(&results);
    let correct = FakeExecutor::NEW_OBJECT_CONTENT;
    assert!(contents(&results[0].0).iter().all(|c| c != correct));
}

#[test]
fn dependents_of_a_rejected_transaction_are_rejected_for_it() {
    let (x, y) = (ObjectId::new(1), ObjectId::new(2));
    let create: Transaction = FakeTransaction::success(vec![], vec![x], vec![]).into();
    let update: Transaction = FakeTransaction::success(vec![], vec![], vec![x]).into();
    let dependent: Transaction = FakeTransaction::success(vec![x], vec![y], vec![]).into();
    // Two clusters of two split the update's votes; every version agrees on the dependent, each
    // on its own result for the update.
    let split = vec![
        cluster(&[0, 1], Some(vec![&update])),
        cluster(&[2, 3], Some(vec![&update])),
    ];
    let results = run(
        4,
        config(split),
        vec![vec![create], vec![update, dependent]],
    );

    let (store, _, decisions) = &results[0];
    let [
        (_, created),
        (update, rejected),
        (dependent, dependent_rejected),
    ] = &decisions[..]
    else {
        panic!("three decisions: {decisions:?}");
    };
    assert!(matches!(created, Decision::Final { .. }));
    assert_eq!(update.commit, dependent.commit, "one commit");
    assert_eq!(*rejected, Decision::Rejected(Rejection::Impossible));
    assert_eq!(
        *dependent_rejected,
        Decision::Rejected(Rejection::EarlierTransaction)
    );
    assert_eq!(
        store.latest(&x).unwrap().contents(),
        FakeExecutor::NEW_OBJECT_CONTENT
    );
    assert_eq!(store.latest_version(&y), None);
    for (other_store, _, other_decisions) in &results {
        assert_eq!((other_store, other_decisions), (store, decisions));
    }
}
