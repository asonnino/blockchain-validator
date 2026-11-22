// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The contract's versions: the engines running them, which validator runs which, the reference
//! never wrong, and what executing them costs.

mod assignment;
mod synthetic;

use std::time::Duration;

pub use assignment::{VersionId, assign};
use dag::{authority::Authority, committee::Committee};
use execution::{
    effects::{AbortReason, ExecutionOutput},
    engine::ExecutionEngine,
    store::{InMemoryStore, StateView},
    transaction::Transaction,
};
pub use synthetic::{Cluster, SyntheticConfig, SyntheticEngine};

use crate::{
    byzantine::{Byzantine, Results},
    digest::ResultDigest,
};

/// The simulated time the processor's executions take, reported as [`Processed::busy`]: a
/// constant per execution plus a cost per unit of gas. Repairs count as executions; ground-truth
/// runs of the reference engine and collusion's runs of every version do not, as colluders are
/// omniscient. Costs, like the simulated time they add up to, must fit a [`Duration`]: processing
/// panics otherwise.
///
/// [`Processed::busy`]: validator::processor::Processed::busy
#[derive(Default)]
pub struct CostModel {
    pub per_execution: Duration,
    pub per_gas: Duration,
}

impl CostModel {
    fn cost(&self, output: &ExecutionOutput) -> Duration {
        const NANOS_PER_SEC: u128 = 1_000_000_000;
        let nanos = self
            .per_gas
            .as_nanos()
            .saturating_mul(u128::from(output.gas_used()));
        let secs = u64::try_from(nanos / NANOS_PER_SEC).expect("costs fit a Duration");
        self.per_execution + Duration::new(secs, (nanos % NANOS_PER_SEC) as u32)
    }
}

/// A validator's engines: one per version, its own and the others for repair, and the reference.
pub struct Versions<E> {
    engines: Vec<E>,
    /// Each authority's version.
    assignment: Vec<VersionId>,
    /// Never wrong.
    reference: E,
    cost: CostModel,
}

impl<E: ExecutionEngine> Versions<E> {
    /// `assignment[authority]` indexes `engines`.
    pub fn new(engines: Vec<E>, reference: E, assignment: Vec<VersionId>) -> Self {
        assert!(
            assignment.iter().all(|version| version.0 < engines.len()),
            "one engine per version"
        );
        Self {
            engines,
            assignment,
            reference,
            cost: CostModel::default(),
        }
    }

    pub fn with_cost(mut self, cost: CostModel) -> Self {
        self.cost = cost;
        self
    }

    pub(crate) fn assignment(&self) -> &[VersionId] {
        &self.assignment
    }

    pub(crate) fn engine(&self, version: VersionId) -> &E {
        &self.engines[version.0]
    }

    /// `transaction`'s output under `version`, and its cost.
    pub(crate) fn execute<V: StateView>(
        &self,
        version: VersionId,
        view: &V,
        transaction: &Transaction,
    ) -> (ExecutionOutput, Duration) {
        let output = execute(self.engine(version), view, transaction);
        let cost = self.cost.cost(&output);
        (output, cost)
    }

    /// `transaction`'s output under the reference.
    pub(crate) fn truth<V: StateView>(
        &self,
        view: &V,
        transaction: &Transaction,
    ) -> ExecutionOutput {
        execute(&self.reference, view, transaction)
    }

    /// Re-executes `transaction` on the final state with each winning voter's version in turn,
    /// until one reproduces `result`; a Byzantine voter's version may not, but an honest voter's
    /// does. Returns the output, the executions tried, and their cost.
    pub(crate) fn repair(
        &self,
        store: &InMemoryStore,
        transaction: &Transaction,
        result: ResultDigest,
        voters: &[Authority],
    ) -> (ExecutionOutput, u32, Duration) {
        let (mut tried, mut spent) = (Vec::new(), Duration::ZERO);
        for voter in voters {
            let version = self.assignment[voter.index()];
            if tried.contains(&version) {
                continue;
            }
            tried.push(version);
            let (output, cost) = self.execute(version, store, transaction);
            spent += cost;
            if ResultDigest::new(&output) == result {
                return (output, tried.len() as u32, spent);
            }
        }
        panic!("no voter's version reproduces the final result");
    }

    /// What collusion compares for `transaction`, executed on `view` by the reference and by
    /// every honest authority's version.
    pub(crate) fn results<V: StateView>(
        &self,
        committee: &Committee,
        byzantine: &Byzantine,
        view: &V,
        transaction: &Transaction,
    ) -> Results {
        let result = |engine: &E| ResultDigest::new(&execute(engine, view, transaction));
        let by_version: Vec<_> = self.engines.iter().map(result).collect();
        let honest = committee
            .authorities()
            .filter(|authority| !byzantine.authorities.contains(authority))
            .map(|authority| {
                let stake = committee.get_stake(authority).expect("committee members");
                (stake, by_version[self.assignment[authority.index()].0])
            });
        Results {
            truth: result(&self.reference),
            honest: honest.collect(),
        }
    }
}

/// `transaction`'s output under `engine`, failing static verification as the scheduler does.
fn execute<E: ExecutionEngine, V: StateView>(
    engine: &E,
    view: &V,
    transaction: &Transaction,
) -> ExecutionOutput {
    if transaction.verify() {
        engine.execute(view, transaction)
    } else {
        ExecutionOutput::aborted(AbortReason::InvalidTransaction)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use dag::authority::Authority;
    use execution::{
        effects::{AbortReason, ExecutionOutput, ExecutionStatus},
        engine::ExecutionEngine,
        object::{Object, ObjectId, Version},
        store::InMemoryStore,
        transaction::Transaction,
    };
    use fake_engine::{FakeExecutor, FakeTransaction};
    use replica::config::PublicReplicaConfig;

    use super::{CostModel, SyntheticConfig, SyntheticEngine, VersionId, Versions};
    use crate::{
        byzantine::{Byzantine, Strategy},
        digest::ResultDigest,
    };

    /// Synthetic versions wrong with the `failure` probabilities, run as `assignment` says (by
    /// authority).
    fn synthetic(failure: Vec<f64>, assignment: &[usize]) -> Versions<SyntheticEngine> {
        let synthetic = |failure| {
            Arc::new(SyntheticConfig {
                seed: 0,
                failure,
                clusters: Vec::new(),
            })
        };
        let versions = failure.len();
        let config = synthetic(failure);
        let engines = (0..versions)
            .map(|version| SyntheticEngine::new(VersionId(version), config.clone()))
            .collect();
        let reference = SyntheticEngine::new(VersionId(0), synthetic(vec![0.0]));
        Versions::new(
            engines,
            reference,
            assignment.iter().copied().map(VersionId).collect(),
        )
    }

    #[test]
    fn executions_cost_a_constant_plus_their_gas() {
        let gas = ExecutionOutput::success(Vec::new()).with_gas_used(500);
        let cost = CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::from_micros(1),
        };
        assert_eq!(cost.cost(&gas), Duration::from_micros(1500));
        // Costs beyond 2^64 ns stay exact.
        let heavy = ExecutionOutput::success(Vec::new()).with_gas_used(20_000_000_000);
        let per_second = CostModel {
            per_execution: Duration::ZERO,
            per_gas: Duration::from_secs(1),
        };
        assert_eq!(per_second.cost(&heavy), Duration::from_secs(20_000_000_000));
    }

    #[test]
    fn collusion_compares_the_versions_of_honest_authorities() {
        // Version 0 is wrong, and authority 1 runs it; authority 2 holds 2 of 5.
        let versions = synthetic(vec![1.0, 0.0, 0.0, 0.0], &[1, 0, 2, 3]);
        let mut public_config = PublicReplicaConfig::new_for_tests(4);
        public_config.identifiers[2].stake = 2;
        let committee = public_config.committee();
        let creation = FakeTransaction::success(vec![], vec![ObjectId::new(1)], vec![]).into();
        let wrong = |authorities: &[usize]| {
            let byzantine = Byzantine {
                strategy: Strategy::Collude,
                selective: None,
                authorities: authorities.iter().copied().map(Authority::from).collect(),
            };
            let view = InMemoryStore::default();
            let results = versions.results(&committee, &byzantine, &view, &creation);
            let honest = results.honest.into_iter();
            honest
                .map(|(stake, result)| (stake, result != results.truth))
                .collect::<Vec<_>>()
        };
        assert_eq!(wrong(&[0]), [(1, true), (2, false), (1, false)]);
        // Every Byzantine authority's result is left out, not only the own.
        assert_eq!(wrong(&[0, 1]), [(2, false), (1, false)]);
    }

    #[test]
    fn invalid_transactions_abort_before_any_version_runs() {
        let versions = synthetic(vec![1.0], &[0]);
        let x = ObjectId::new(1);
        let duplicate = FakeTransaction::success(vec![], vec![x, x], vec![]).into();
        let (output, _) = versions.execute(VersionId(0), &InMemoryStore::default(), &duplicate);
        let invalid = ExecutionStatus::Aborted(AbortReason::InvalidTransaction);
        assert_eq!(*output.status(), invalid);
    }

    #[test]
    fn repair_skips_versions_that_do_not_reproduce_the_result() {
        let (versions, store, transaction, result) = repair_setup();
        let versions = versions.with_cost(CostModel {
            per_execution: Duration::from_millis(1),
            per_gas: Duration::ZERO,
        });
        let voters = [Authority::from(0usize), Authority::from(1usize)];
        let (output, tries, spent) = versions.repair(&store, &transaction, result, &voters);
        assert_eq!(ResultDigest::new(&output), result);
        assert_eq!((tries, spent), (2, Duration::from_millis(2)));
    }

    #[test]
    #[should_panic(expected = "no voter's version reproduces the final result")]
    fn repair_never_applies_another_result() {
        let (versions, store, transaction, result) = repair_setup();
        versions.repair(&store, &transaction, result, &[Authority::from(0usize)]);
    }

    /// Authority 0 runs version 0, always wrong, and authority 1 version 1, correct; the
    /// transaction updates an existing object, and the result is its correct one.
    fn repair_setup() -> (
        Versions<SyntheticEngine>,
        InMemoryStore,
        Transaction,
        ResultDigest,
    ) {
        let id = ObjectId::new(1);
        let mut store = InMemoryStore::default();
        let object = Object::new(id, Version::new(1), b"x".to_vec());
        store.apply(ExecutionOutput::success(vec![object]));
        let transaction: Transaction = FakeTransaction::success(vec![], vec![], vec![id]).into();
        let result = ResultDigest::new(&FakeExecutor.execute(&store, &transaction));
        (
            synthetic(vec![1.0, 0.0], &[0, 1]),
            store,
            transaction,
            result,
        )
    }
}
