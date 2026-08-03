// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A fake execution engine for tests and demos.

use crate::{
    effects::{AbortReason, ExecutionOutput},
    engine::ExecutionEngine,
    object::{Object, ObjectId},
    store::StateView,
    transaction::{AccessMode, FunctionId, Transaction},
};

/// The functions the fake engine understands.
pub enum FakeFunctionId {
    /// Successfully executes.
    Success,
    /// Aborts unconditionally.
    Abort,
}

impl FakeFunctionId {
    const SUCCESS: FunctionId = FunctionId::new(0);
    const ABORT: FunctionId = FunctionId::new(1);

    /// The wire id of the function.
    fn id(self) -> FunctionId {
        match self {
            Self::Success => Self::SUCCESS,
            Self::Abort => Self::ABORT,
        }
    }

    /// Resolves a transaction's function, if the engine knows it.
    fn resolve(transaction: &Transaction) -> Option<Self> {
        match transaction.function() {
            id if id == Self::SUCCESS => Some(Self::Success),
            id if id == Self::ABORT => Some(Self::Abort),
            _ => None,
        }
    }
}

/// A [`Transaction`] built for the fake engine.
pub struct FakeTransaction(Transaction);

impl FakeTransaction {
    /// A [`FakeFunctionId::Success`] call.
    pub fn success(
        read_only: Vec<ObjectId>,
        write_only: Vec<ObjectId>,
        read_write: Vec<ObjectId>,
    ) -> Self {
        Self::success_with_args(read_only, write_only, read_write, Vec::new())
    }

    /// A [`FakeFunctionId::Success`] call carrying pure arguments; the engine ignores them, so
    /// they act as payload padding.
    pub fn success_with_args(
        read_only: Vec<ObjectId>,
        write_only: Vec<ObjectId>,
        read_write: Vec<ObjectId>,
        args: Vec<u8>,
    ) -> Self {
        let mut inputs = Vec::with_capacity(read_only.len() + write_only.len() + read_write.len());
        inputs.extend(read_only.into_iter().map(|id| (id, AccessMode::ReadOnly)));
        inputs.extend(write_only.into_iter().map(|id| (id, AccessMode::WriteOnly)));
        inputs.extend(read_write.into_iter().map(|id| (id, AccessMode::ReadWrite)));
        Self(Transaction::new(FakeFunctionId::Success.id(), inputs, args))
    }

    /// A [`FakeFunctionId::Abort`] call.
    pub fn abort() -> Self {
        Self(Transaction::new(
            FakeFunctionId::Abort.id(),
            Vec::new(),
            Vec::new(),
        ))
    }
}

impl From<FakeTransaction> for Transaction {
    fn from(transaction: FakeTransaction) -> Self {
        transaction.0
    }
}

/// A stateless [`ExecutionEngine`] interpreting the [`FakeFunctionId`] functions.
///
/// Execution is a no-op: written objects get a bumped version — fresh
/// [`FakeExecutor::NEW_OBJECT_CONTENT`] for `WriteOnly` inputs, carrying their contents forward
/// for `ReadWrite` inputs.
pub struct FakeExecutor;

impl FakeExecutor {
    /// The contents of every created object.
    pub const NEW_OBJECT_CONTENT: &'static [u8] = b"fake";
}

impl ExecutionEngine for FakeExecutor {
    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
        match FakeFunctionId::resolve(transaction) {
            None => return ExecutionOutput::aborted(AbortReason::UnknownFunction),
            Some(FakeFunctionId::Abort) => {
                return ExecutionOutput::aborted(AbortReason::ExplicitAbort);
            }
            Some(FakeFunctionId::Success) => (),
        }
        let Some(version) = transaction.next_version(view) else {
            return ExecutionOutput::aborted(AbortReason::MissingRead);
        };

        let objects = transaction
            .inputs()
            .iter()
            .filter_map(|(id, mode)| match mode {
                AccessMode::WriteOnly => {
                    Some(Object::new(*id, version, Self::NEW_OBJECT_CONTENT.to_vec()))
                }
                AccessMode::ReadWrite => {
                    let object = view.latest(id).expect("checked by next_version");
                    Some(Object::new(*id, version, object.contents().to_vec()))
                }
                AccessMode::ReadOnly => None,
            })
            .collect();
        ExecutionOutput::success(objects)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        effects::{AbortReason, ExecutionOutput, ExecutionStatus},
        engine::ExecutionEngine,
        fake::{FakeExecutor, FakeTransaction},
        object::{Object, ObjectId, Version},
        store::{InMemoryStore, StateView},
        transaction::{FunctionId, Transaction},
    };

    #[test]
    fn creations_land_in_the_output_not_in_the_store() {
        let store = InMemoryStore::default();
        let id = ObjectId::new(1);
        let transaction = FakeTransaction::success(vec![], vec![id], vec![]).into();
        let output = FakeExecutor.execute(&store, &transaction);

        assert_eq!(output.status(), ExecutionStatus::Success);
        let contents = FakeExecutor::NEW_OBJECT_CONTENT.to_vec();
        assert_eq!(
            output.writes(),
            [Object::new(id, Version::new(1), contents)]
        );
        assert!(store.latest(&id).is_none());
    }

    #[test]
    fn output_version_is_one_past_the_highest_existing_input() {
        let mut store = InMemoryStore::default();
        let (read, created) = (ObjectId::new(1), ObjectId::new(2));
        store.apply(ExecutionOutput::success(vec![Object::new(
            read,
            Version::new(5),
            vec![],
        )]));

        let transaction = FakeTransaction::success(vec![read], vec![created], vec![]).into();
        let output = FakeExecutor.execute(&store, &transaction);
        let contents = FakeExecutor::NEW_OBJECT_CONTENT.to_vec();
        assert_eq!(
            output.writes(),
            [Object::new(created, Version::new(6), contents)]
        );
    }

    #[test]
    fn read_writes_bump_the_version_and_keep_the_contents() {
        let mut store = InMemoryStore::default();
        let id = ObjectId::new(1);
        store.apply(ExecutionOutput::success(vec![Object::new(
            id,
            Version::new(3),
            vec![1],
        )]));

        let transaction = FakeTransaction::success(vec![], vec![], vec![id]).into();
        let output = FakeExecutor.execute(&store, &transaction);
        assert_eq!(output.writes(), [Object::new(id, Version::new(4), vec![1])]);
    }

    #[test]
    fn multi_write_transactions_land_every_write_at_one_version() {
        let mut store = InMemoryStore::default();
        let (created_a, created_b) = (ObjectId::new(1), ObjectId::new(2));
        let modified = ObjectId::new(3);
        store.apply(ExecutionOutput::success(vec![Object::new(
            modified,
            Version::new(2),
            vec![7],
        )]));

        let transaction =
            FakeTransaction::success(vec![], vec![created_a, created_b], vec![modified]).into();
        let output = FakeExecutor.execute(&store, &transaction);
        assert_eq!(output.status(), ExecutionStatus::Success);
        assert_eq!(output.writes().len(), 3);

        store.apply(output);
        for id in [created_a, created_b, modified] {
            assert_eq!(store.latest(&id).unwrap().version(), Version::new(3));
        }
    }

    #[test]
    fn explicit_aborts_write_nothing() {
        let transaction = FakeTransaction::abort().into();
        let output = FakeExecutor.execute(&InMemoryStore::default(), &transaction);
        assert_eq!(
            output.status(),
            ExecutionStatus::Aborted(AbortReason::ExplicitAbort)
        );
        assert!(output.writes().is_empty());
    }

    #[test]
    fn missing_reads_abort() {
        let id = ObjectId::new(1);
        for transaction in [
            FakeTransaction::success(vec![id], vec![], vec![]),
            FakeTransaction::success(vec![], vec![], vec![id]),
        ] {
            let output = FakeExecutor.execute(&InMemoryStore::default(), &transaction.into());
            assert_eq!(
                output.status(),
                ExecutionStatus::Aborted(AbortReason::MissingRead)
            );
            assert!(output.writes().is_empty());
        }
    }

    #[test]
    fn unknown_functions_abort() {
        let transaction = Transaction::new(FunctionId::new(9), vec![], vec![]);
        let output = FakeExecutor.execute(&InMemoryStore::default(), &transaction);
        assert_eq!(
            output.status(),
            ExecutionStatus::Aborted(AbortReason::UnknownFunction)
        );
    }
}
