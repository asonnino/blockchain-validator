// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Multi-replica convergence and determinism under the mysticeti discrete-event simulator.
//! Unlike the smoke test, every validator submits through its own replica, so the total order
//! of transactions is genuinely decided by consensus across concurrent proposers.

use std::{path::Path, sync::Arc, time::Duration};

use checkpoint::certifier::CheckpointCertifier;
use dag::{authority::Authority, context::Ctx, metrics::Metrics};
use execution::{
    fake::{FakeExecutor, FakeTransaction},
    object::{ObjectId, Version},
    scheduler::SequentialScheduler,
    store::StateView,
};
use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use simulator::{SimulatedNetwork, SimulatorContext, SimulatorExecutor};
use validator::validator::{ValidatorBuilder, ValidatorHandle};

const COMMITTEE_SIZE: usize = 4;
const STATE_UPDATES: usize = 50;
/// Spreads submissions across many consensus rounds (simulated time is free).
const SUBMISSION_INTERVAL: Duration = Duration::from_millis(100);

/// A committee of validators running under the simulator.
struct SimulatedTestbed {
    /// Dropping the network silently severs all connections.
    _network: SimulatedNetwork,
    validators: Vec<ValidatorHandle<SimulatorContext, FakeExecutor>>,
    metrics: Vec<Arc<Metrics>>,
}

impl SimulatedTestbed {
    async fn start() -> Self {
        let public_config = PublicReplicaConfig::new_for_tests(COMMITTEE_SIZE);
        let latency = Duration::from_millis(50)..Duration::from_millis(100);
        let (network, node_networks) = SimulatedNetwork::new(&public_config.committee(), latency);
        let private_configs =
            PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), COMMITTEE_SIZE);

        let mut validators = Vec::with_capacity(COMMITTEE_SIZE);
        let mut metrics = Vec::with_capacity(COMMITTEE_SIZE);
        for (index, (node_network, private_config)) in
            node_networks.into_iter().zip(private_configs).enumerate()
        {
            let node_metrics = Metrics::new_for_test(COMMITTEE_SIZE);
            metrics.push(node_metrics.clone());
            let validator = ValidatorBuilder::new(
                FakeExecutor,
                Authority::from(index),
                public_config.clone(),
                private_config,
            )
            .with_storage(StorageKind::Ephemeral)
            .with_crypto_disabled()
            .with_metrics(node_metrics)
            .with_network(node_network)
            .build()
            .expect("validator must build")
            .start::<SimulatorContext>()
            .await
            .expect("validator must start");
            validators.push(validator);
        }
        network.connect_all().await;

        Self {
            _network: network,
            validators,
            metrics,
        }
    }

    fn validators(&self) -> &[ValidatorHandle<SimulatorContext, FakeExecutor>] {
        &self.validators
    }

    async fn wait_for_transactions(&mut self, count: u64) {
        for validator in &mut self.validators {
            validator.wait_for_transactions(count).await;
        }
    }

    async fn shutdown(self) -> Vec<(SequentialScheduler<FakeExecutor>, CheckpointCertifier)> {
        let mut results = Vec::with_capacity(self.validators.len());
        for validator in self.validators {
            results.push(validator.shutdown().await);
        }
        results
    }
}

/// Every validator creates its own object, then modifies it while reading its neighbor's: the
/// read couples the Lamport versions across objects, so the final state encodes the commit
/// interleaving.
async fn submission_loop(
    index: usize,
    validator: &ValidatorHandle<SimulatorContext, FakeExecutor>,
) {
    let id = ObjectId::new(index as u64);
    let neighbor = ObjectId::new(((index + 1) % COMMITTEE_SIZE) as u64);
    let create = FakeTransaction::success(vec![], vec![id], vec![]).into();
    validator
        .submit(vec![create])
        .await
        .expect("submission must succeed");
    for _ in 0..STATE_UPDATES {
        SimulatorContext::sleep(SUBMISSION_INTERVAL).await;
        // Aborts deterministically with a missing read until the neighbor's object is created.
        let modify = FakeTransaction::success(vec![neighbor], vec![], vec![id]).into();
        validator
            .submit(vec![modify])
            .await
            .expect("submission must succeed");
    }
}

type RunOutcome = (
    Vec<(SequentialScheduler<FakeExecutor>, CheckpointCertifier)>,
    Vec<Arc<Metrics>>,
);

fn run_once(seed: u64) -> RunOutcome {
    SimulatorExecutor::run(StdRng::seed_from_u64(seed), async move {
        let mut committee = SimulatedTestbed::start().await;
        let submissions = committee
            .validators()
            .iter()
            .enumerate()
            .map(|(index, validator)| submission_loop(index, validator));
        join_all(submissions).await;

        // Every submitted transaction executes (success or abort), so the cut is exact.
        let total = (COMMITTEE_SIZE * (STATE_UPDATES + 1)) as u64;
        committee.wait_for_transactions(total).await;
        let metrics = committee.metrics.clone();
        (committee.shutdown().await, metrics)
    })
}

#[test]
fn validators_converge_under_simulation() {
    let (results, metrics) = run_once(7);

    // Commit latency derives from the envelope timestamps: positive, and bounded well below
    // the several simulated seconds the submissions span — unstamped payloads would instead
    // record "commit time − epoch", which grows with the run.
    for metrics in &metrics {
        let p50 = metrics
            .collect()
            .latency_percentile_ms(0.5)
            .expect("commit-latency histogram must be populated");
        assert!(
            p50 > 0.0 && p50 < 2_000.0,
            "implausible commit latency: {p50} ms"
        );
    }

    let (reference, certifier) = &results[0];
    let store = reference.store();
    // Without vote submission (arriving with the payload envelope), every minted checkpoint
    // stays pending; equal pending chains are the divergence check.
    for (scheduler, other) in &results {
        assert_eq!(scheduler.store(), store);
        assert!(other.pending().eq(certifier.pending()));
    }
    assert!(certifier.pending().next().is_some());
    // Every submitter's object was created and modified at least once.
    for index in 0..COMMITTEE_SIZE {
        let latest = store
            .latest(&ObjectId::new(index as u64))
            .expect("object must exist");
        assert!(latest.version() >= Version::new(2));
    }
}

#[test]
fn simulation_is_deterministic_per_seed() {
    for seed in [7, 42] {
        let (first, _) = run_once(seed);
        let (second, _) = run_once(seed);
        for ((a, a_certifier), (b, b_certifier)) in first.iter().zip(&second) {
            assert_eq!(
                a.store(),
                b.store(),
                "seed {seed} produced diverging stores"
            );
            assert!(
                a_certifier.pending().eq(b_certifier.pending()),
                "seed {seed} produced diverging checkpoints"
            );
        }
    }
}
