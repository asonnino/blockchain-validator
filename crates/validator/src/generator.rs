// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A deterministic load generator, mirroring mysticeti's `replica::generator` — same pacing,
//! seeding, clock caching, and block chunking — but emitting an engine's [`Workload`] in
//! timestamped [`Envelope`]s, so everything downstream of commit (execution, checkpoints,
//! certification) fires.

use std::{mem, sync::Arc, time::Duration};

use dag::{
    authority::Authority, block::transaction::Transaction as ConsensusTransaction,
    config::ImportExport, context::Ctx, crypto::AsBytes,
};
use execution::{
    transaction::{FunctionId, Transaction},
    workload::Workload,
};
use rand::{SeedableRng, rngs::StdRng};
use replica::client::TransactionClient;
use serde::{Deserialize, Serialize};

use crate::{
    envelope::{Envelope, MAX_PAYLOAD_SIZE, Payload},
    metrics::ValidatorMetrics,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct LoadGeneratorConfig {
    /// Transactions to submit per second.
    #[serde(default = "load_generator_defaults::default_load")]
    pub load: usize,
    /// Target serialized envelope size per transaction, in bytes.
    #[serde(default = "load_generator_defaults::default_transaction_size")]
    pub transaction_size: usize,
    /// Delay before the first submission.
    /// Accepts humantime strings in YAML (e.g. `"0s"`, `"500ms"`, `"30s"`).
    #[serde(default = "load_generator_defaults::default_initial_delay")]
    #[serde(with = "humantime_serde")]
    pub initial_delay: Duration,
}

impl LoadGeneratorConfig {
    pub const DEFAULT_FILENAME: &'static str = "load-generator-config.yaml";

    pub fn new_for_test() -> Self {
        Self {
            load: 100,
            transaction_size: 128,
            initial_delay: Duration::ZERO,
        }
    }
}

impl Default for LoadGeneratorConfig {
    fn default() -> Self {
        Self {
            load: load_generator_defaults::default_load(),
            transaction_size: load_generator_defaults::default_transaction_size(),
            initial_delay: load_generator_defaults::default_initial_delay(),
        }
    }
}

impl ImportExport for LoadGeneratorConfig {}

mod load_generator_defaults {
    use std::time::Duration;

    pub fn default_load() -> usize {
        10
    }

    pub fn default_transaction_size() -> usize {
        512
    }

    pub fn default_initial_delay() -> Duration {
        Duration::from_secs(30)
    }
}

/// One authority's transaction stream: its workload, with the RNG and counter the generator
/// owns.
struct Stream<W> {
    workload: W,
    rng: StdRng,
    namespace: u64,
    counter: u64,
}

impl<W: Workload> Stream<W> {
    fn next(&mut self, size: usize) -> Transaction {
        let transaction =
            self.workload
                .transaction(self.namespace, self.counter, size, &mut self.rng);
        self.counter += 1;
        transaction
    }
}

pub struct TransactionGenerator {
    client: TransactionClient,
    max_block_size: usize,
    metrics: Arc<ValidatorMetrics>,
}

impl TransactionGenerator {
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);

    pub fn start<C: Ctx, M: Serialize + 'static, W: Workload>(
        client: TransactionClient,
        seed: Authority,
        config: LoadGeneratorConfig,
        max_block_size: usize,
        metrics: Arc<ValidatorMetrics>,
        workload: W,
    ) -> C::JoinHandle<()> {
        assert!(config.load > 0, "load must be positive");
        let size = Self::budget::<M>(config.transaction_size);
        tracing::info!(
            "Starting generator with {} transactions per second, initial delay {:?}",
            config.load,
            config.initial_delay
        );
        let stream = Stream {
            workload,
            rng: StdRng::seed_from_u64(seed.as_u64()),
            namespace: seed.as_u64(),
            counter: 0,
        };
        C::spawn(
            Self {
                client,
                max_block_size,
                metrics,
            }
            .run::<C, M, W>(config, stream, size),
        )
    }

    /// The serialized size each transaction may take for its envelope to be exactly
    /// `transaction_size` bytes — the analog of upstream's fixed raw payload size. Envelope
    /// overhead is invariant across transactions and timestamps, so one probe suffices.
    fn budget<M: Serialize>(transaction_size: usize) -> usize {
        let probe = Transaction::new(FunctionId::new(0), Vec::new(), Vec::new());
        let probe_size = bincode::serialized_size(&probe).expect("transactions serialize");
        let overhead = Envelope::<M>::new(0, Payload::Execute(probe))
            .to_bytes()
            .len()
            - probe_size as usize;
        assert!(
            transaction_size > overhead,
            "transaction_size must be greater than {overhead} bytes"
        );
        // Oversized envelopes fail to decode after commit and are silently skipped, so the
        // generator would exercise nothing downstream.
        assert!(
            transaction_size as u64 <= MAX_PAYLOAD_SIZE,
            "transaction_size must not exceed the {MAX_PAYLOAD_SIZE}-byte payload cap"
        );
        transaction_size - overhead
    }

    fn fill_batch<M: Serialize, W: Workload>(
        stream: &mut Stream<W>,
        timestamp_ms: u64,
        transactions_per_interval: usize,
        size: usize,
    ) -> Vec<ConsensusTransaction> {
        let mut batch = Vec::with_capacity(transactions_per_interval);
        for _ in 0..transactions_per_interval {
            let envelope = Envelope::<M>::new(timestamp_ms, Payload::Execute(stream.next(size)));
            batch.push(ConsensusTransaction::new(envelope.to_bytes().into()));
        }
        batch
    }

    async fn ship_blocks(&self, batch: Vec<ConsensusTransaction>, block_capacity: usize) -> bool {
        let mut block = Vec::with_capacity(block_capacity);
        let mut block_size = 0;
        for transaction in batch {
            block_size += transaction.as_bytes().len();
            block.push(transaction);

            if block_size >= self.max_block_size {
                let full_block = mem::replace(&mut block, Vec::with_capacity(block_capacity));
                if self.client.submit(full_block).await.is_err() {
                    return false;
                }
                block_size = 0;
            }
        }
        block.is_empty() || self.client.submit(block).await.is_ok()
    }

    async fn run<C: Ctx, M: Serialize, W: Workload>(
        self,
        config: LoadGeneratorConfig,
        mut stream: Stream<W>,
        size: usize,
    ) {
        let intervals_per_second = 1000 / Self::TARGET_BLOCK_INTERVAL.as_millis() as usize;
        let transactions_per_interval = config.load.div_ceil(intervals_per_second);
        let block_capacity =
            (self.max_block_size / config.transaction_size).min(transactions_per_interval);

        // Cache the context clock at startup and derive subsequent timestamps from a
        // monotonic instant, avoiding repeated clock reads in the hot loop. Works uniformly
        // under the real and the simulated clock.
        let base_system_time = C::timestamp_utc();
        let base_instant = C::now();

        // Under tokio missed ticks are skipped (no catch-up burst after the initial delay);
        // under the simulator the interval degrades to a fixed sleep between batches. Neither
        // backend bursts, though `div_ceil` rounds the effective rate up to the next multiple
        // of `intervals_per_second`, as upstream does.
        let mut interval = C::interval(Self::TARGET_BLOCK_INTERVAL);
        C::sleep(config.initial_delay).await;
        loop {
            C::interval_tick(&mut interval).await;
            let timestamp_ms = (base_system_time + C::elapsed(&base_instant)).as_millis() as u64;

            // Structured envelopes must be serialized per transaction, so the per-tick
            // allocations of upstream's reused flat buffer cannot be avoided here.
            let batch = Self::fill_batch::<M, W>(
                &mut stream,
                timestamp_ms,
                transactions_per_interval,
                size,
            );

            if !self.ship_blocks(batch, block_capacity).await {
                tracing::info!("Transaction channel closed, stopping generator");
                return;
            }

            // Reported per tick, diverging from upstream's amortized flush: a counter must
            // not under-report, and ten atomic adds per second cost nothing.
            self.metrics
                .inc_submitted_transactions(transactions_per_interval as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use execution::transaction::AccessMode;
    use fake_engine::FakeWorkload;

    use super::*;
    use crate::envelope::NoMessage;

    const TRANSACTION_SIZE: usize = 128;

    #[test]
    fn payloads_decode_to_distinct_creations_of_exact_size() {
        let mut stream = Stream {
            workload: FakeWorkload::default(),
            rng: StdRng::seed_from_u64(1),
            namespace: 1,
            counter: 0,
        };
        let size = TransactionGenerator::budget::<NoMessage>(TRANSACTION_SIZE);
        let batch = TransactionGenerator::fill_batch::<NoMessage, _>(&mut stream, 1234, 10, size);
        let mut ids = Vec::with_capacity(batch.len());
        for transaction in batch {
            let bytes = transaction.as_bytes();
            assert_eq!(bytes.len(), TRANSACTION_SIZE);
            let envelope = Envelope::<NoMessage>::from_bytes(bytes).unwrap();
            assert_eq!(envelope.timestamp_ms(), 1234);
            let Payload::Execute(transaction) = envelope.into_payload() else {
                panic!("expected an execution payload");
            };
            let [(id, AccessMode::WriteOnly)] = transaction.inputs() else {
                panic!("expected a single creation");
            };
            assert!(
                !ids.contains(id),
                "the stream must advance between transactions"
            );
            ids.push(*id);
        }
    }

    #[test]
    fn config_roundtrips_through_yaml_files() {
        let config = LoadGeneratorConfig {
            load: 250,
            transaction_size: 256,
            initial_delay: Duration::from_secs(5),
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        config.print(file.path()).unwrap();
        // Hand-edited and orchestrator-uploaded configs rely on the humantime disk format.
        let written = std::fs::read_to_string(file.path()).unwrap();
        assert!(written.contains("initial_delay: 5s"), "{written}");
        let loaded = LoadGeneratorConfig::load(file.path()).unwrap();
        assert_eq!(loaded.load, config.load);
        assert_eq!(loaded.transaction_size, config.transaction_size);
        assert_eq!(loaded.initial_delay, config.initial_delay);
    }

    #[test]
    fn config_fills_defaults_and_parses_humantime_delays() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "initial_delay: 30s\n").unwrap();
        let loaded = LoadGeneratorConfig::load(file.path()).unwrap();
        assert_eq!(loaded.load, LoadGeneratorConfig::default().load);
        assert_eq!(
            loaded.transaction_size,
            LoadGeneratorConfig::default().transaction_size
        );
        assert_eq!(loaded.initial_delay, Duration::from_secs(30));
    }
}
