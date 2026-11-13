// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The results of executing a transaction.

use std::mem;

use crate::object::{Object, ObjectId, Version};

/// Why a transaction aborted.
#[derive(Clone, PartialEq, Debug)]
pub enum AbortReason {
    /// The payload requested an abort.
    ExplicitAbort,
    /// The call read an object that does not exist.
    MissingRead,
    /// The call created an object whose id exists or once did.
    IdInUse,
    /// The engine does not know the called function.
    UnknownFunction,
    /// The transaction failed static verification.
    InvalidTransaction,
    /// The engine failed the call, for example on a Move abort.
    ExecutionFailure(FailureDetail),
}

/// How an engine failed a call, in the engine's own encoding; empty when the engine adds
/// nothing.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct FailureDetail(Vec<u8>);

impl FailureDetail {
    pub fn new(contents: Vec<u8>) -> Self {
        Self(contents)
    }

    pub fn contents(&self) -> &[u8] {
        &self.0
    }
}

/// The outcome of executing a transaction.
#[derive(Clone, PartialEq, Debug)]
pub enum ExecutionStatus {
    Success,
    Aborted(AbortReason),
}

/// An engine-defined event, opaque to everything but the engine and its consumers. Events are
/// not state: they stay out of the store and the state commitment.
#[derive(PartialEq, Debug)]
pub struct Event(Vec<u8>);

impl Event {
    pub fn new(contents: Vec<u8>) -> Self {
        Self(contents)
    }

    pub fn contents(&self) -> &[u8] {
        &self.0
    }
}

/// The complete output of executing a transaction: the outcome, the freshly created object
/// values (each self-describing: id, version, contents), the objects deleted or wrapped, and
/// the emitted events.
///
/// **Invariant:** aborted transactions write, delete, and emit nothing. Unlike Sui, which bumps
/// the versions of mutable inputs on abort, versions here derive from the view at execution time
/// rather than being assigned at consensus, so an abort needs no writes.
pub struct ExecutionOutput {
    status: ExecutionStatus,
    writes: Vec<Object>,
    deletes: Vec<(ObjectId, Version)>,
    events: Vec<Event>,
    /// The gas the engine metered for the call, in its own units; like events, outside the store
    /// and the state commitment.
    gas_used: u64,
}

impl ExecutionOutput {
    /// The output of a successful execution.
    pub fn success(writes: Vec<Object>) -> Self {
        Self {
            status: ExecutionStatus::Success,
            writes,
            deletes: Vec::new(),
            events: Vec::new(),
            gas_used: 0,
        }
    }

    /// The output of an aborted execution.
    pub fn aborted(reason: AbortReason) -> Self {
        Self {
            status: ExecutionStatus::Aborted(reason),
            writes: Vec::new(),
            deletes: Vec::new(),
            events: Vec::new(),
            gas_used: 0,
        }
    }

    /// Deletes or wraps objects, each at the version this transaction writes at.
    pub fn with_deletes(mut self, deletes: Vec<(ObjectId, Version)>) -> Self {
        assert_eq!(
            self.status,
            ExecutionStatus::Success,
            "aborts delete nothing"
        );
        self.deletes = deletes;
        self
    }

    pub fn with_events(mut self, events: Vec<Event>) -> Self {
        assert_eq!(self.status, ExecutionStatus::Success, "aborts emit nothing");
        self.events = events;
        self
    }

    pub fn with_gas_used(mut self, gas_used: u64) -> Self {
        self.gas_used = gas_used;
        self
    }

    pub fn status(&self) -> &ExecutionStatus {
        &self.status
    }

    pub fn gas_used(&self) -> u64 {
        self.gas_used
    }

    pub(crate) fn take_events(&mut self) -> Vec<Event> {
        mem::take(&mut self.events)
    }

    pub fn writes(&self) -> &[Object] {
        &self.writes
    }

    pub fn deletes(&self) -> &[(ObjectId, Version)] {
        &self.deletes
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub(crate) fn into_changes(self) -> (Vec<Object>, Vec<(ObjectId, Version)>) {
        (self.writes, self.deletes)
    }
}

/// A transaction's status, events, and gas used, once its state changes are applied.
#[derive(PartialEq, Debug)]
pub struct TransactionOutcome {
    pub status: ExecutionStatus,
    pub events: Vec<Event>,
    pub gas_used: u64,
}
