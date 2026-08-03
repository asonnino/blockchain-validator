// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator node.
//!
//! Glue in the spirit of sui-node: builds the consensus replica and wires its committed
//! sub-dags into the execution scheduler. Checkpointing comes later.

pub mod envelope;
#[cfg(any(test, feature = "test-utils"))]
pub mod generator;
pub mod metrics;
pub mod validator;
