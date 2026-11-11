// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The workload abstraction.

use rand::rngs::StdRng;

use crate::transaction::Transaction;

/// Generates the transactions a load generator submits. Workloads run under the simulator, so
/// each transaction must be a deterministic function of the arguments and the workload's state.
pub trait Workload: Send + 'static {
    /// The `counter`-th transaction of authority `namespace`'s generator, drawing randomness from
    /// `rng`, which the generator seeds per authority. `size` is the serialized size the generator
    /// targets: workloads that can pad meet it exactly, others ignore it.
    fn transaction(
        &mut self,
        namespace: u64,
        counter: u64,
        size: usize,
        rng: &mut StdRng,
    ) -> Transaction;
}
