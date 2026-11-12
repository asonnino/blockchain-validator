// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The detail of the calls Sui fails.

use execution::effects::FailureDetail;
use move_core_types::vm_status::StatusCode;
use serde::{Deserialize, Serialize};
use sui_types::execution_status::ExecutionFailureStatus;

/// Why Sui failed a call, encoded in the call's
/// [`ExecutionFailure`](execution::effects::AbortReason::ExecutionFailure) detail.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct MoveFailure {
    /// Sui's report, a Move abort's code and location included.
    pub status: ExecutionFailureStatus,
    /// The Move VM's status when the VM raised the failure. It tells apart the failures Sui
    /// reports alike as primitive runtime errors, such as arithmetic errors and out-of-bounds
    /// vector indices.
    pub vm_status: Option<StatusCode>,
}

impl MoveFailure {
    /// `None` for the engine's own rejections, made before Sui runs the call.
    pub fn decode(detail: &FailureDetail) -> Option<Self> {
        let contents = detail.contents();
        (!contents.is_empty())
            .then(|| bcs::from_bytes(contents).expect("the engine encodes its failures"))
    }

    pub(crate) fn encode(&self) -> FailureDetail {
        FailureDetail::new(bcs::to_bytes(self).expect("failures serialize"))
    }
}
