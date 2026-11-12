// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The execution engine abstraction.

use crate::{effects::ExecutionOutput, object::Object, store::StateView, transaction::Transaction};

/// What consensus tells engines about a committed sub-dag before its transactions run;
/// identical on every validator.
pub struct CommitInfo {
    /// The round of the sub-dag's anchor.
    pub round: u64,
    /// The anchor's timestamp, in milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
}

/// Executes a transaction against a read-only view of the store.
///
/// Engines are stateless: `execute` is a pure, synchronous function of the view and the
/// transaction, returning an [`ExecutionOutput`]. It is also infallible at the Rust level:
/// malformed or misbehaving transactions yield aborted outputs, not errors.
pub trait ExecutionEngine {
    /// The objects every store starts from, before the first commit. They must be identical on
    /// every validator; the state commitment covers them.
    fn genesis(&self) -> Vec<Object> {
        Vec::new()
    }

    /// Runs before the transactions of each committed sub-dag that has any, and its output is
    /// applied first. It must succeed.
    fn begin_commit<V: StateView>(&self, _view: &V, _commit: &CommitInfo) -> ExecutionOutput {
        ExecutionOutput::success(Vec::new())
    }

    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput;
}
