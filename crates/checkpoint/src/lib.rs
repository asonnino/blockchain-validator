// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint engine.
//!
//! Mints commitments to the execution state, one per committed sub-dag that executed
//! transactions, and certifies each once a quorum of stake attests the same value through
//! consensus.

pub mod certifier;
pub mod checkpoint;
