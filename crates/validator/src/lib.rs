// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator node.
//!
//! Glue in the spirit of sui-node: builds the consensus replica and feeds its committed
//! sub-dags to a commit processor.

pub mod envelope;
#[cfg(any(test, feature = "benchmark"))]
pub mod generator;
pub mod metrics;
pub mod processor;
pub mod time;
pub mod validator;
