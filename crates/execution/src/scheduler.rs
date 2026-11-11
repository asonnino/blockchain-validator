// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The sequential transaction scheduler.

use std::vec::Drain;

use crate::{
    effects::{AbortReason, ExecutionOutput, ExecutionStatus, TransactionOutcome},
    engine::ExecutionEngine,
    store::InMemoryStore,
    transaction::Transaction,
};

/// Executes transactions strictly in commit order and owns the resulting state.
pub struct SequentialScheduler<E> {
    store: InMemoryStore,
    engine: E,
    /// The latest batch's outcomes; kept across batches to reuse the allocation.
    outcomes: Vec<TransactionOutcome>,
}

impl<E: ExecutionEngine> SequentialScheduler<E> {
    /// Starts from the engine's genesis objects.
    pub fn new(engine: E) -> Self {
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(engine.genesis()));
        Self {
            store,
            engine,
            outcomes: Vec::new(),
        }
    }

    /// Executes `transactions` in order, applying each transaction's output before executing the
    /// next, and returns their outcomes in the same order. Execution is eager: dropping the
    /// returned iterator early loses outcomes, never transactions.
    pub fn execute(
        &mut self,
        transactions: impl IntoIterator<Item = Transaction>,
    ) -> Drain<'_, TransactionOutcome> {
        for transaction in transactions {
            let outcome = if transaction.verify() {
                let mut output = self.engine.execute(&self.store, &transaction);
                let outcome = TransactionOutcome {
                    status: output.status(),
                    events: output.take_events(),
                };
                self.store.apply(output);
                outcome
            } else {
                TransactionOutcome {
                    status: ExecutionStatus::Aborted(AbortReason::InvalidTransaction),
                    events: Vec::new(),
                }
            };
            self.outcomes.push(outcome);
        }
        self.outcomes.drain(..)
    }

    pub fn store(&self) -> &InMemoryStore {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crypto::Digest,
        effects::{AbortReason, Event, ExecutionOutput, ExecutionStatus, TransactionOutcome},
        engine::ExecutionEngine,
        object::{Object, ObjectId, Version},
        scheduler::SequentialScheduler,
        store::StateView,
        transaction::{AccessMode, FunctionId, Transaction},
    };

    const ABORT: FunctionId = FunctionId::new(1);
    const DELETE: FunctionId = FunctionId::new(2);

    /// Aborts [`ABORT`] calls. Every other call touches its non-read-only inputs at the next
    /// version: [`DELETE`] calls delete them and emit one event, the rest write them.
    struct Stub;

    impl ExecutionEngine for Stub {
        fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
            if transaction.function() == ABORT {
                return ExecutionOutput::aborted(AbortReason::ExplicitAbort);
            }
            let Some(version) = transaction.next_version(view) else {
                return ExecutionOutput::aborted(AbortReason::MissingRead);
            };
            let touched = transaction
                .inputs()
                .iter()
                .filter(|(_, mode)| *mode != AccessMode::ReadOnly)
                .map(|(id, _)| *id);
            if transaction.function() == DELETE {
                return ExecutionOutput::success(Vec::new())
                    .with_deletes(touched.map(|id| (id, version)).collect())
                    .with_events(vec![Event::new(vec![2])]);
            }
            let writes = touched
                .map(|id| Object::new(id, version, Vec::new()))
                .collect();
            ExecutionOutput::success(writes)
        }
    }

    fn write(inputs: Vec<(ObjectId, AccessMode)>) -> Transaction {
        Transaction::new(FunctionId::new(0), inputs, Vec::new())
    }

    fn batch() -> Vec<Transaction> {
        let id = ObjectId::new(1);
        vec![
            write(vec![(id, AccessMode::WriteOnly)]),
            Transaction::new(ABORT, Vec::new(), Vec::new()),
            write(vec![(id, AccessMode::ReadWrite)]),
        ]
    }

    #[test]
    fn transactions_execute_in_commit_order() {
        let mut scheduler = SequentialScheduler::new(Stub);
        let statuses: Vec<_> = scheduler
            .execute(batch())
            .map(|outcome| outcome.status)
            .collect();

        assert_eq!(statuses.len(), 3);
        assert_eq!(statuses[0], ExecutionStatus::Success);
        assert_eq!(
            statuses[1],
            ExecutionStatus::Aborted(AbortReason::ExplicitAbort)
        );
        // The abort did not halt the batch: the read-modify-write landed on top of the creation.
        assert_eq!(statuses[2], ExecutionStatus::Success);
        let latest = scheduler.store().latest(&ObjectId::new(1)).unwrap();
        assert_eq!(latest.version(), Version::new(2));
    }

    #[test]
    fn unconsumed_outcomes_stay_out_of_the_next_batch() {
        let mut scheduler = SequentialScheduler::new(Stub);
        drop(scheduler.execute(batch()));
        // The batch executed even though its outcomes were dropped unread.
        let latest = scheduler.store().latest(&ObjectId::new(1)).unwrap();
        assert_eq!(latest.version(), Version::new(2));
        let outcomes = scheduler.execute([Transaction::new(ABORT, Vec::new(), Vec::new())]);
        assert_eq!(outcomes.len(), 1);
    }

    #[test]
    fn invalid_transactions_abort_without_executing() {
        let id = ObjectId::new(1);
        let transaction = write(vec![
            (id, AccessMode::WriteOnly),
            (id, AccessMode::WriteOnly),
        ]);
        let mut scheduler = SequentialScheduler::new(Stub);
        let outcomes: Vec<_> = scheduler.execute([transaction]).collect();

        let status = ExecutionStatus::Aborted(AbortReason::InvalidTransaction);
        assert_eq!(
            outcomes,
            [TransactionOutcome {
                status,
                events: Vec::new()
            }]
        );
        assert!(scheduler.store().latest(&id).is_none());
    }

    #[test]
    fn stores_start_from_genesis() {
        /// Has one genesis object and executes nothing.
        struct Genesis;

        impl ExecutionEngine for Genesis {
            fn genesis(&self) -> Vec<Object> {
                vec![Object::new(ObjectId::new(7), Version::new(1), vec![7])]
            }

            fn execute<V: StateView>(&self, _: &V, _: &Transaction) -> ExecutionOutput {
                unreachable!()
            }
        }

        let scheduler = SequentialScheduler::new(Genesis);
        let object = scheduler.store().latest(&ObjectId::new(7));
        assert_eq!(object.unwrap().contents(), [7]);
        assert_ne!(scheduler.store().commitment(), Digest::default());
    }

    #[test]
    fn undeclared_creations_land_in_the_store() {
        /// Creates a fresh object without declaring it.
        struct Creator;

        impl ExecutionEngine for Creator {
            fn execute<V: StateView>(
                &self,
                view: &V,
                transaction: &Transaction,
            ) -> ExecutionOutput {
                let version = transaction.next_version(view).expect("no inputs to read");
                ExecutionOutput::success(vec![Object::new(ObjectId::new(9), version, vec![])])
            }
        }

        let mut scheduler = SequentialScheduler::new(Creator);
        let outcomes: Vec<_> = scheduler.execute([write(Vec::new())]).collect();
        assert_eq!(outcomes[0].status, ExecutionStatus::Success);
        assert!(scheduler.store().latest(&ObjectId::new(9)).is_some());
    }

    #[test]
    fn deletions_apply_and_events_surface() {
        let id = ObjectId::new(1);
        let mut scheduler = SequentialScheduler::new(Stub);
        let outcomes: Vec<_> = scheduler
            .execute([
                write(vec![(id, AccessMode::WriteOnly)]),
                Transaction::new(DELETE, vec![(id, AccessMode::ReadWrite)], Vec::new()),
                write(vec![(id, AccessMode::ReadWrite)]),
            ])
            .collect();

        let deleted = TransactionOutcome {
            status: ExecutionStatus::Success,
            events: vec![Event::new(vec![2])],
        };
        assert_eq!(outcomes[1], deleted);
        // The deleted object no longer resolves, so reading it aborts.
        assert_eq!(
            outcomes[2].status,
            ExecutionStatus::Aborted(AbortReason::MissingRead)
        );
        assert!(scheduler.store().latest(&id).is_none());
    }
}
