// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The execution engine abstraction.

use crate::{effects::ExecutionOutput, store::StateView, transaction::Transaction};

/// Executes a transaction against a read-only view of the store.
///
/// Engines are stateless: `execute` is a pure, synchronous function of the view and the
/// transaction, returning an [`ExecutionOutput`] — the outcome plus the freshly written
/// objects — for the store to consume. It is also infallible at the Rust level: malformed or
/// misbehaving transactions yield aborted outputs, not errors.
pub trait ExecutionEngine {
    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput;
}
