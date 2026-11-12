// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The counter workload on a committee under the mysticeti simulator, every validator
//! submitting through its own replica.

#![cfg(feature = "sui")]

mod common;

use std::{ops::RangeInclusive, path::Path, time::Duration};

use checkpoint::certifier::CheckpointCertifier;
use common::{clock, shared_counters, value};
use dag::{authority::Authority, context::Ctx, metrics::Metrics};
use execution::scheduler::SequentialScheduler;
use move_engine::{COUNTER, CounterWorkload, MoveEngine};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use simulator::{
    LatencyModel, SimulatedNetwork, SimulatorContext, SimulatorExecutor, UniformLatency,
};
use validator::{generator::LoadGeneratorConfig, validator::ValidatorBuilder};

const COMMITTEE_SIZE: usize = 4;
/// Executed-transaction cut at which the load generators are stopped.
const TARGET_TRANSACTIONS: u64 = 400;
/// One quiescence window: long enough for any in-flight block to commit everywhere.
const DRAIN_INTERVAL: Duration = Duration::from_secs(5);

fn engine() -> MoveEngine {
    MoveEngine::new(vec![vec![COUNTER.to_vec()]])
}

/// Every validator's final state.
type Results = Vec<(SequentialScheduler<MoveEngine>, CheckpointCertifier)>;

/// Runs the workload to a common fixed point and returns the final states, with the run's span
/// of simulated time in milliseconds since the Unix epoch.
fn run_once(seed: u64) -> (Results, RangeInclusive<u64>) {
    SimulatorExecutor::run(StdRng::seed_from_u64(seed), async move {
        let now_ms = || SimulatorContext::timestamp_utc().as_millis() as u64;
        let start_ms = now_ms();
        let public_config = PublicReplicaConfig::new_for_tests(COMMITTEE_SIZE);
        let latency = LatencyModel::Uniform(UniformLatency { range_ms: 50..100 });
        let (network, node_networks) = SimulatedNetwork::new(&public_config.committee(), latency);
        let private_configs =
            PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), COMMITTEE_SIZE);

        let mut validators = Vec::with_capacity(COMMITTEE_SIZE);
        let mut workloads = Vec::with_capacity(COMMITTEE_SIZE);
        for (index, (node_network, private_config)) in
            node_networks.into_iter().zip(private_configs).enumerate()
        {
            let engine = engine();
            workloads.push(CounterWorkload::new(&engine));
            let authority = Authority::from(index);
            let validator =
                ValidatorBuilder::new(engine, authority, public_config.clone(), private_config)
                    .expect("validator must build")
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

        let generators: Vec<_> = validators
            .iter()
            .zip(workloads)
            .map(|(validator, workload)| {
                validator.start_load_generator(LoadGeneratorConfig::new_for_test(), workload)
            })
            .collect();
        for validator in &mut validators {
            validator.wait_for_transactions(TARGET_TRANSACTIONS).await;
        }
        for generator in &generators {
            SimulatorContext::abort(generator);
        }
        // Queued transactions keep committing after the generators stop, so wait until every
        // validator holds the same executed count for a full quiescence window.
        loop {
            let target = validators.iter().map(|v| v.executed()).max().unwrap();
            for validator in &mut validators {
                validator.wait_for_transactions(target).await;
            }
            SimulatorContext::sleep(DRAIN_INTERVAL).await;
            if validators.iter().all(|v| v.executed() == target) {
                break;
            }
        }
        for validator in &mut validators {
            validator.wait_for_certified().await;
        }
        let end_ms = now_ms();

        let mut results = Vec::with_capacity(COMMITTEE_SIZE);
        for validator in validators {
            results.push(validator.shutdown().await.into_parts());
        }
        (results, start_ms..=end_ms)
    })
}

#[test]
fn validators_converge_on_the_counter_workload() {
    let (results, span_ms) = run_once(7);
    let (reference, certifier) = &results[0];
    let store = reference.store();
    let certified = certifier
        .highest_certified()
        .expect("everything executed is certified");
    assert_eq!(certified.checkpoint().commitment(), store.commitment());
    for (scheduler, other) in &results {
        assert_eq!(scheduler.store(), store);
        assert_eq!(other.highest_certified(), Some(certified));
    }

    // Every validator's stream contends on the shared counters.
    let shared = shared_counters(&engine());
    assert!(!shared.is_empty());
    for id in &shared {
        assert!(value(store, id) > 0);
    }
    // The clock advanced with the commits, to a time within the run.
    let time_ms = value(store, &clock());
    assert!(
        span_ms.contains(&time_ms),
        "{time_ms} ms outside {span_ms:?}"
    );
}

#[test]
fn simulation_is_deterministic_per_seed() {
    let ((first, _), (second, _)) = (run_once(42), run_once(42));
    for ((a, a_certifier), (b, b_certifier)) in first.iter().zip(&second) {
        assert_eq!(a.store(), b.store());
        assert_eq!(
            a_certifier.highest_certified(),
            b_certifier.highest_certified()
        );
    }
}
