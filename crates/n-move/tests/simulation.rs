// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The n-version processor on a committee under the mysticeti simulator, each validator running a
//! synthetic version of the fake engine and classifying outcomes against a version never wrong.

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
    Cluster, CostModel, NVersionProcessor, Outcome, Position, Records, Rejection, SyntheticConfig,
    SyntheticEngine, VersionId, assign,
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
const EXECUTION: Duration = Duration::from_millis(1);

/// Every validator's final state, certifier, and records.
type Results = Vec<(InMemoryStore, CheckpointCertifier, Records)>;

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
        let correct = Arc::new(crate::config(Vec::new()));
        let assignment = assign(&committee, COMMITTEE_SIZE, seed);

        let mut validators = Vec::with_capacity(COMMITTEE_SIZE);
        for (index, (node_network, private_config)) in
            node_networks.into_iter().zip(private_configs).enumerate()
        {
            let authority = Authority::from(index);
            let engines = (0..COMMITTEE_SIZE)
                .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
                .collect();
            let reference = SyntheticEngine::new(VersionId(0), correct.clone());
            let processor = NVersionProcessor::new(
                authority,
                engines,
                reference,
                assignment.clone(),
                THRESHOLD,
                CUTOFF,
                &public_config,
            )
            .expect("processor must build")
            .with_cost(CostModel {
                per_execution: EXECUTION,
                per_gas: Duration::ZERO,
            });
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

/// Each decided transaction's position and outcome, in decision order.
fn outcomes(records: &Records) -> Vec<(Position, &Outcome)> {
    let transactions = records.transactions.iter();
    transactions
        .map(|record| (record.position, &record.outcome))
        .collect()
}

/// Asserts every validator ended with the same store, certificate, and outcomes, every one
/// `expected`, and saw each transaction's stages in order.
fn assert_agreement(results: &Results, expected: Outcome) {
    let (store, certifier, records) = &results[0];
    assert_eq!(records.transactions.len() as u64, 2 * OBJECTS);
    assert!(
        outcomes(records)
            .iter()
            .all(|(_, outcome)| **outcome == expected)
    );
    let certified = certifier.highest_certified().expect("certified");
    assert_eq!(certified.checkpoint().commitment(), store.commitment());
    for (other_store, other_certifier, other_records) in results {
        assert_eq!(other_store, store);
        assert_eq!(other_certifier.highest_certified(), Some(certified));
        assert_eq!(outcomes(other_records), outcomes(records));
        let mut executions = other_records.transactions.len() as u32;
        for record in &other_records.transactions {
            let stages = [record.submitted_ms, record.committed_ms, record.executed_ms];
            let voted = record.voted_ms.unwrap_or(record.executed_ms);
            assert!(
                stages.is_sorted() && (record.executed_ms..=record.decided_ms).contains(&voted)
            );
            // Every repair succeeds with the first winning voter's version.
            assert_eq!(record.repairs, u32::from(record.lost));
            assert!(!record.lost_to_earlier);
            executions += record.repairs;
        }
        assert_eq!(other_records.busy, EXECUTION * executions);
    }
}

/// How many validators lost every result, repairing each.
fn losers(results: &Results) -> usize {
    let lost_all = |records: &Records| {
        let mut transactions = records.transactions.iter();
        transactions.all(|record| record.lost && record.repairs > 0)
    };
    results
        .iter()
        .filter(|(_, _, records)| lost_all(records))
        .count()
}

#[test]
fn correct_versions_finalize_everything_deterministically() {
    let results = run(1, config(Vec::new()), workload());
    assert_agreement(&results, Outcome::FinalCorrect);
    assert_eq!(losers(&results), 0);

    let again = run(1, config(Vec::new()), workload());
    for ((store, _, records), (other_store, _, other_records)) in results.iter().zip(&again) {
        assert_eq!((store, records), (other_store, other_records));
    }
}

#[test]
fn a_wrong_version_loses_and_repairs() {
    let results = run(2, config(vec![cluster(&[0], None)]), workload());
    assert_agreement(&results, Outcome::FinalCorrect);
    assert_eq!(losers(&results), 1);
}

#[test]
fn a_threshold_of_versions_agreeing_on_a_wrong_result_finalizes_it() {
    let results = run(3, config(vec![cluster(&[0, 1, 2], None)]), workload());
    assert_agreement(&results, Outcome::FinalWrong);
    // The correct version loses to the wrong majority.
    assert_eq!(losers(&results), 1);
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

    let (store, _, records) = &results[0];
    let [
        (_, created),
        (update, rejected),
        (dependent, dependent_rejected),
    ] = &outcomes(records)[..]
    else {
        panic!("three decisions: {:?}", records.transactions);
    };
    assert_eq!(**created, Outcome::FinalCorrect);
    assert_eq!(update.commit, dependent.commit, "one commit");
    assert_eq!(**rejected, Outcome::Rejected(Rejection::Impossible));
    assert_eq!(
        **dependent_rejected,
        Outcome::Rejected(Rejection::EarlierTransaction)
    );
    assert_eq!(
        store.latest(&x).unwrap().contents(),
        FakeExecutor::NEW_OBJECT_CONTENT
    );
    assert_eq!(store.latest_version(&y), None);
    for (other_store, _, other_records) in &results {
        assert_eq!(other_store, store);
        assert_eq!(outcomes(other_records), outcomes(records));
        // Every validator executed the dependent on its own, stale, result for the update.
        let stale: Vec<_> = other_records
            .transactions
            .iter()
            .map(|record| record.lost_to_earlier)
            .collect();
        assert_eq!(stale, [false, false, true]);
    }
}
