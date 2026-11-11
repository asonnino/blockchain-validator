// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The sequential transaction scheduler.

use crate::{
    effects::{AbortReason, ExecutionStatus},
    engine::ExecutionEngine,
    store::InMemoryStore,
    transaction::Transaction,
};

/// Executes transactions strictly in commit order and owns the resulting state.
pub struct SequentialScheduler<E> {
    store: InMemoryStore,
    engine: E,
}

impl<E: ExecutionEngine> SequentialScheduler<E> {
    pub fn new(engine: E) -> Self {
        Self {
            store: InMemoryStore::default(),
            engine,
        }
    }

    /// Executes `transactions` in order, applying each transaction's output before executing the
    /// next, and returns their statuses in the same order.
    pub fn execute(
        &mut self,
        transactions: impl IntoIterator<Item = Transaction>,
    ) -> Vec<ExecutionStatus> {
        transactions
            .into_iter()
            .map(|transaction| {
                if !transaction.verify() {
                    return ExecutionStatus::Aborted(AbortReason::InvalidTransaction);
                }
                let output = self.engine.execute(&self.store, &transaction);
                let status = output.status();
                self.store.apply(output);
                status
            })
            .collect()
    }

    pub fn store(&self) -> &InMemoryStore {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        effects::{AbortReason, ExecutionOutput, ExecutionStatus},
        engine::ExecutionEngine,
        object::{Object, ObjectId, Version},
        scheduler::SequentialScheduler,
        store::StateView,
        transaction::{AccessMode, FunctionId, Transaction},
    };

    const ABORT: FunctionId = FunctionId::new(1);

    /// Aborts [`ABORT`] calls and writes every other call's non-read-only inputs at the next
    /// version.
    struct Stub;

    impl ExecutionEngine for Stub {
        fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
            if transaction.function() == ABORT {
                return ExecutionOutput::aborted(AbortReason::ExplicitAbort);
            }
            let Some(version) = transaction.next_version(view) else {
                return ExecutionOutput::aborted(AbortReason::MissingRead);
            };
            let writes = transaction
                .inputs()
                .iter()
                .filter(|(_, mode)| *mode != AccessMode::ReadOnly)
                .map(|(id, _)| Object::new(*id, version, Vec::new()))
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
        let statuses = scheduler.execute(batch());

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
    fn invalid_transactions_abort_without_executing() {
        let id = ObjectId::new(1);
        let transaction = write(vec![
            (id, AccessMode::WriteOnly),
            (id, AccessMode::WriteOnly),
        ]);
        let mut scheduler = SequentialScheduler::new(Stub);
        let statuses = scheduler.execute([transaction]);

        assert_eq!(
            statuses,
            [ExecutionStatus::Aborted(AbortReason::InvalidTransaction)]
        );
        assert!(scheduler.store().latest(&id).is_none());
    }
}
