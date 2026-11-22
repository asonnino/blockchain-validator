// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A committee of n-version processors under the mysticeti simulator, deciding batches of
//! transactions submitted one after the other.

// Each test binary compiles its own copy of this module and uses a subset of it.
#![allow(dead_code)]

use std::{path::Path, pin::pin, time::Duration};

use checkpoint::certifier::CheckpointCertifier;
use dag::{authority::Authority, context::Ctx, metrics::Metrics};
use execution::{engine::ExecutionEngine, store::InMemoryStore, transaction::Transaction};
use futures::future::{Either, select};
use n_move::{NVersionProcessor, Outcome, Position, Records};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::StorageKind,
    config::{PrivateReplicaConfig, PublicReplicaConfig},
};
use simulator::{
    LatencyModel, SimulatedNetwork, SimulatorContext, SimulatorExecutor, UniformLatency,
};
use validator::validator::ValidatorBuilder;

/// Simulated time any wait may take: validators that disagree would otherwise wait forever.
const LIMIT: Duration = Duration::from_secs(60);

/// Every validator's final state, certifier, and records.
pub type Results = Vec<(InMemoryStore, CheckpointCertifier, Records)>;

/// Runs a committee of `size` validators, each with the processor `processor` builds for it,
/// submitting each batch once every validator decided the previous ones, from a different
/// validator each time.
pub fn run<E: ExecutionEngine + Send + 'static>(
    size: usize,
    seed: u64,
    processor: impl Fn(Authority, &PublicReplicaConfig) -> NVersionProcessor<E> + Send + 'static,
    batches: Vec<Vec<Transaction>>,
) -> Results {
    SimulatorExecutor::run(StdRng::seed_from_u64(seed), async move {
        let public_config = PublicReplicaConfig::new_for_tests(size);
        let latency = LatencyModel::Uniform(UniformLatency { range_ms: 50..100 });
        let (network, node_networks) = SimulatedNetwork::new(&public_config.committee(), latency);
        let private_configs = PrivateReplicaConfig::new_for_benchmarks(Path::new("unused"), size);

        let mut validators = Vec::with_capacity(size);
        for (index, (node_network, private_config)) in
            node_networks.into_iter().zip(private_configs).enumerate()
        {
            let authority = Authority::from(index);
            let validator = ValidatorBuilder::with_processor(
                processor(authority, &public_config),
                authority,
                public_config.clone(),
                private_config,
            )
            .with_storage(StorageKind::Ephemeral)
            .with_simulated_crypto()
            // The default metrics spawn their reporter on tokio, absent here.
            .with_metrics(Metrics::new_for_test(size))
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
            let submitter = &validators[index % size];
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

        let mut results = Vec::with_capacity(size);
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

/// Each decided transaction's position and outcome, in decision order.
pub fn outcomes(records: &Records) -> Vec<(Position, &Outcome)> {
    let transactions = records.transactions.iter();
    transactions
        .map(|record| (record.position, &record.outcome))
        .collect()
}
