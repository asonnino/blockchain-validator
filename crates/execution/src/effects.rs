// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The results of executing a transaction.

use crate::object::Object;

/// Why a transaction aborted.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum AbortReason {
    /// The payload requested an abort.
    ExplicitAbort,
    /// The call read an object that does not exist.
    MissingRead,
    /// The engine does not know the called function.
    UnknownFunction,
    /// The transaction failed static verification.
    InvalidTransaction,
}

/// The outcome of executing a transaction.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ExecutionStatus {
    Success,
    Aborted(AbortReason),
}

/// The complete output of executing a transaction: the outcome plus the freshly created object
/// values, each self-describing (id, version, contents).
///
/// **Invariant:** aborted transactions write nothing.
pub struct ExecutionOutput {
    status: ExecutionStatus,
    writes: Vec<Object>,
}

impl ExecutionOutput {
    /// The output of a successful execution.
    pub fn success(writes: Vec<Object>) -> Self {
        Self {
            status: ExecutionStatus::Success,
            writes,
        }
    }

    /// The output of an aborted execution.
    pub fn aborted(reason: AbortReason) -> Self {
        Self {
            status: ExecutionStatus::Aborted(reason),
            writes: Vec::new(),
        }
    }

    pub fn status(&self) -> ExecutionStatus {
        self.status
    }

    pub fn writes(&self) -> &[Object] {
        &self.writes
    }

    pub(crate) fn into_writes(self) -> Vec<Object> {
        self.writes
    }
}
