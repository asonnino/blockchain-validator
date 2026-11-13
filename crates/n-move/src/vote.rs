// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The votes validators exchange through consensus.

use serde::{Deserialize, Serialize};

use crate::digest::{InputDigest, ResultDigest};

/// A transaction's place in the commit sequence: its commit, counted from the first, and its index
/// among the commit's transactions. Every validator sees the same sequence.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct Position {
    pub commit: u64,
    pub index: u32,
}

/// A validator's vote on a transaction: the digest of the inputs it found and of the result its
/// version produced.
#[derive(Serialize, Deserialize)]
pub struct Vote {
    pub position: Position,
    pub input: InputDigest,
    pub result: ResultDigest,
}
