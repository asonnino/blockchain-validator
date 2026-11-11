// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Executes Sui Move packages through Sui's own execution layer. Everything sits behind the
//! `sui` feature, so default builds compile an empty crate.

#![cfg(feature = "sui")]

mod engine;
mod store;

pub use engine::{MoveArgument, MoveCall, MoveEngine};
