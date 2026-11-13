// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Synthetic versions: the fake engine, wrong on configured transactions. A version that is never
//! wrong produces [`FakeExecutor`]'s output, the ground truth.

use std::sync::Arc;

use execution::{
    crypto::Digest,
    effects::{AbortReason, Event, ExecutionOutput, FailureDetail},
    engine::ExecutionEngine,
    object::Object,
    store::StateView,
    transaction::Transaction,
};
use fake_engine::FakeExecutor;

use crate::{assignment::VersionId, digest::seeded};

/// Versions wrong together, all with the same wrong result.
pub struct Cluster {
    pub versions: Vec<VersionId>,
    pub probability: f64,
    /// The digests of the only transactions the cluster can be wrong on, or `None` for every
    /// transaction.
    pub transactions: Option<Vec<Digest>>,
}

/// Which versions are wrong on which transactions. Wrongness hashes the seed with the transaction's
/// digest, so runs with the same seed agree.
pub struct SyntheticConfig {
    pub seed: u64,
    /// Each version's probability of a wrong result of its own, indexed by [`VersionId`].
    pub failure: Vec<f64>,
    /// Checked before each version's own failures.
    pub clusters: Vec<Cluster>,
}

/// What a wrong result is tagged with, so equal tags give equal wrong results.
struct Tag(Vec<u8>);

impl Tag {
    fn cluster(index: usize) -> Self {
        Self::new(0, index)
    }

    fn version(VersionId(index): VersionId) -> Self {
        Self::new(1, index)
    }

    fn new(kind: u8, index: usize) -> Self {
        let mut bytes = vec![kind];
        bytes.extend((index as u64).to_be_bytes());
        Self(bytes)
    }

    /// The wrong result in place of `output`: the tag appended to every written object's
    /// contents, or a failure carrying it when nothing is written.
    fn corrupt(&self, output: ExecutionOutput) -> ExecutionOutput {
        if output.writes().is_empty() {
            let detail = FailureDetail::new(self.0.clone());
            return ExecutionOutput::aborted(AbortReason::ExecutionFailure(detail));
        }
        let writes = output
            .writes()
            .iter()
            .map(|object| {
                let contents = [object.contents(), &self.0].concat();
                Object::new(object.id(), object.version(), contents)
            })
            .collect();
        let events = output
            .events()
            .iter()
            .map(|event| Event::new(event.contents().to_vec()))
            .collect();
        ExecutionOutput::success(writes)
            .with_deletes(output.deletes().to_vec())
            .with_events(events)
    }
}

/// One version of the contract.
pub struct SyntheticEngine {
    version: VersionId,
    config: Arc<SyntheticConfig>,
}

impl SyntheticEngine {
    pub fn new(version: VersionId, config: Arc<SyntheticConfig>) -> Self {
        assert!(
            version.0 < config.failure.len(),
            "the version needs a failure probability"
        );
        Self { version, config }
    }

    fn wrongness(&self, transaction: &Transaction) -> Option<Tag> {
        let digest = transaction.digest();
        let fires = |tag: &Tag, probability: f64| {
            let draw = seeded(self.config.seed, &[&tag.0, digest.as_bytes()]);
            // The top 53 bits, uniform in [0, 1).
            ((draw >> 11) as f64 / (1u64 << 53) as f64) < probability
        };
        let clusters = self.config.clusters.iter().enumerate();
        clusters
            .filter(|(_, cluster)| cluster.versions.contains(&self.version))
            .filter(|(_, cluster)| {
                let targets = cluster.transactions.as_ref();
                targets.is_none_or(|targets| targets.contains(&digest))
            })
            .map(|(index, cluster)| (Tag::cluster(index), cluster.probability))
            .chain([(
                Tag::version(self.version),
                self.config.failure[self.version.0],
            )])
            .find(|(tag, probability)| fires(tag, *probability))
            .map(|(tag, _)| tag)
    }
}

impl ExecutionEngine for SyntheticEngine {
    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
        let output = FakeExecutor.execute(view, transaction);
        match self.wrongness(transaction) {
            Some(tag) => tag.corrupt(output),
            None => output,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use execution::{
        effects::{AbortReason, ExecutionOutput, ExecutionStatus},
        engine::ExecutionEngine,
        object::{Object, ObjectId, Version},
        store::InMemoryStore,
        transaction::Transaction,
    };
    use fake_engine::{FakeExecutor, FakeTransaction};

    use super::{Cluster, SyntheticConfig, SyntheticEngine};
    use crate::{assignment::VersionId, digest::ResultDigest};

    /// A store holding object 1, and transactions that each write it and create a distinct object.
    fn setup() -> (InMemoryStore, Vec<Transaction>) {
        let mut store = InMemoryStore::default();
        let object = Object::new(ObjectId::new(1), Version::new(1), b"x".to_vec());
        store.apply(ExecutionOutput::success(vec![object]));
        let transactions = (2..50)
            .map(|id| {
                let created = vec![ObjectId::new(id)];
                FakeTransaction::success(Vec::new(), created, vec![ObjectId::new(1)]).into()
            })
            .collect();
        (store, transactions)
    }

    fn engines(config: SyntheticConfig) -> Vec<SyntheticEngine> {
        let config = Arc::new(config);
        (0..config.failure.len())
            .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
            .collect()
    }

    /// Whether each engine's result matches the ground truth, per transaction.
    fn correct(engines: &[SyntheticEngine]) -> Vec<Vec<bool>> {
        let (store, transactions) = setup();
        transactions
            .iter()
            .map(|transaction| {
                let truth = ResultDigest::new(&FakeExecutor.execute(&store, transaction));
                let digest = |engine: &SyntheticEngine| {
                    ResultDigest::new(&engine.execute(&store, transaction))
                };
                engines
                    .iter()
                    .map(|engine| digest(engine) == truth)
                    .collect()
            })
            .collect()
    }

    fn config(seed: u64, failure: Vec<f64>) -> SyntheticConfig {
        SyntheticConfig {
            seed,
            failure,
            clusters: Vec::new(),
        }
    }

    #[test]
    fn wrongness_follows_the_seed() {
        let half = || vec![0.5; 3];
        let first = correct(&engines(config(1, half())));
        assert_eq!(first, correct(&engines(config(1, half()))));
        assert_ne!(first, correct(&engines(config(2, half()))));
        // Draws differ per version and per transaction.
        assert!(
            first
                .iter()
                .any(|row| row.contains(&true) && row.contains(&false))
        );
        for version in 0..3 {
            let column: Vec<_> = first.iter().map(|row| row[version]).collect();
            assert!(column.contains(&true) && column.contains(&false));
        }
    }

    #[test]
    fn probabilities_zero_and_one_are_never_and_always() {
        let outcomes = correct(&engines(config(0, vec![0.0, 1.0])));
        assert!(outcomes.iter().all(|row| row == &[true, false]));
    }

    #[test]
    fn cluster_members_agree_on_their_wrong_result() {
        // Clusters come first, so version 1's own failures don't apply; version 0's own wrong
        // result must not collide with cluster 0's.
        let mut config = config(0, vec![1.0, 1.0, 0.0]);
        config.clusters.push(Cluster {
            versions: vec![VersionId(1), VersionId(2)],
            probability: 1.0,
            transactions: None,
        });
        let engines = engines(config);
        let (store, transactions) = setup();
        let truth = ResultDigest::new(&FakeExecutor.execute(&store, &transactions[0]));
        let digests: Vec<_> = engines
            .iter()
            .map(|engine| ResultDigest::new(&engine.execute(&store, &transactions[0])))
            .collect();
        assert_eq!(digests[1], digests[2]);
        assert!(digests[0] != truth && digests[1] != truth && digests[0] != digests[1]);
    }

    #[test]
    fn targeted_clusters_are_wrong_on_their_transactions_only() {
        let (_, transactions) = setup();
        let mut config = config(0, vec![0.0]);
        config.clusters.push(Cluster {
            versions: vec![VersionId(0)],
            probability: 1.0,
            transactions: Some(vec![transactions[1].digest()]),
        });
        let outcomes = correct(&engines(config));
        let wrong: Vec<_> = (0..outcomes.len()).filter(|&i| !outcomes[i][0]).collect();
        assert_eq!(wrong, [1]);
    }

    #[test]
    fn wrong_results_with_nothing_written_fail() {
        let (store, _) = setup();
        let read: Transaction =
            FakeTransaction::success(vec![ObjectId::new(1)], vec![], vec![]).into();
        let engines = engines(config(0, vec![1.0, 1.0]));
        let outputs: Vec<_> = engines
            .iter()
            .map(|engine| engine.execute(&store, &read))
            .collect();
        for output in &outputs {
            let failed = matches!(
                output.status(),
                ExecutionStatus::Aborted(AbortReason::ExecutionFailure(_))
            );
            assert!(failed, "{:?}", output.status());
        }
        assert_ne!(
            ResultDigest::new(&outputs[0]),
            ResultDigest::new(&outputs[1])
        );
    }
}
