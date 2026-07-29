// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoints: commitments to the execution state, sampled per committed sub-dag.

use dag::block::BlockReference;
use execution::crypto::Digest;
use serde::{Deserialize, Serialize};

/// A commitment to the execution state after executing a committed sub-dag's transactions.
///
/// Chaining is inherent: the commitment is the store's rolling hash, which already depends on
/// the full ordered write history. The anchor alone identifies the checkpoint — ordering is
/// local knowledge, so no sequence number travels. A checkpoint doubles as this validator's
/// vote when submitted through consensus.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct Checkpoint {
    anchor: BlockReference,
    commitment: Digest,
}

impl Checkpoint {
    pub(crate) fn new(anchor: BlockReference, commitment: Digest) -> Self {
        Self { anchor, commitment }
    }

    pub fn anchor(&self) -> BlockReference {
        self.anchor
    }

    pub fn commitment(&self) -> Digest {
        self.commitment
    }

    pub fn round(&self) -> u64 {
        self.anchor.round
    }

    /// The checkpoint of the `n`-th executing sub-dag: anchored at round `n` (authority 0),
    /// with the default commitment.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(n: u64) -> Self {
        Self::new(BlockReference::new_test(0, n), Digest::default())
    }
}

/// A checkpoint a quorum of stake attested to: the committee's agreement on the execution
/// state at that point.
#[derive(PartialEq, Debug)]
pub struct CertifiedCheckpoint {
    checkpoint: Checkpoint,
    proof: Vec<BlockReference>,
}

impl CertifiedCheckpoint {
    pub(crate) fn new(checkpoint: Checkpoint, proof: Vec<BlockReference>) -> Self {
        Self { checkpoint, proof }
    }

    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    pub fn proof(&self) -> &[BlockReference] {
        &self.proof
    }

    pub fn round(&self) -> u64 {
        self.checkpoint.round()
    }
}
