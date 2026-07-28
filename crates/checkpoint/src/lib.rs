// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint engine.
//!
//! Records commitments to the execution state, one per committed sub-dag that executed
//! transactions, forming a chain every honest validator must agree on. Certification arrives
//! when checkpoints travel between nodes.

pub mod checkpoint;
