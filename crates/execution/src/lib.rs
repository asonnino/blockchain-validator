// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transaction execution.
//!
//! Defines the `ExecutionEngine` trait consumed by the checkpoint and
//! validator crates. Implementations must be generic over `dag::context::Ctx`
//! (no threads, disk, or wall-clock) so the whole validator stays simulatable,
//! and take explicit seeds/config for any randomness (`Ctx` deliberately exposes no RNG).
