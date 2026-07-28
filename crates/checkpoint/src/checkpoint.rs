// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoints: commitments to the execution state, sampled per committed sub-dag.

use dag::block::BlockReference;
use execution::crypto::Digest;

/// A commitment to the execution state after executing a committed sub-dag's transactions.
///
/// Chaining is inherent: the commitment is the store's rolling hash, which already depends on
/// the full ordered write history.
#[derive(PartialEq, Debug)]
pub struct Checkpoint {
    sequence: u64,
    anchor: BlockReference,
    commitment: Digest,
}

impl Checkpoint {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn anchor(&self) -> BlockReference {
        self.anchor
    }

    pub fn commitment(&self) -> Digest {
        self.commitment
    }
}

/// The growing chain of checkpoints; every honest validator builds the same one.
#[derive(Default, PartialEq, Debug)]
pub struct CheckpointChain {
    checkpoints: Vec<Checkpoint>,
}

impl CheckpointChain {
    /// Records the commitment sampled after executing the sub-dag anchored at `anchor`.
    pub fn push(&mut self, anchor: BlockReference, commitment: Digest) {
        self.checkpoints.push(Checkpoint {
            sequence: self.checkpoints.len() as u64,
            anchor,
            commitment,
        });
    }

    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    /// The latest checkpoint — the highest-executed watermark.
    pub fn highest(&self) -> Option<&Checkpoint> {
        self.checkpoints.last()
    }
}

#[cfg(test)]
mod tests {
    use dag::block::BlockReference;
    use execution::{
        crypto::Digest,
        effects::ExecutionOutput,
        object::{Object, ObjectId, Version},
        store::InMemoryStore,
    };

    use crate::checkpoint::CheckpointChain;

    fn anchor(round: u64) -> BlockReference {
        BlockReference::new_test(0, round)
    }

    #[test]
    fn pushes_assign_consecutive_sequences() {
        let mut chain = CheckpointChain::default();
        assert!(chain.highest().is_none());

        chain.push(anchor(1), Digest::default());
        chain.push(anchor(2), Digest::default());

        let sequences: Vec<_> = chain.checkpoints().iter().map(|c| c.sequence()).collect();
        assert_eq!(sequences, [0, 1]);
        assert_eq!(chain.highest().unwrap().sequence(), 1);
    }

    #[test]
    fn identical_pushes_yield_equal_chains() {
        let mut first = CheckpointChain::default();
        let mut second = CheckpointChain::default();
        first.push(anchor(1), Digest::default());
        second.push(anchor(1), Digest::default());
        assert_eq!(first, second);

        second.push(anchor(2), Digest::default());
        assert_ne!(first, second);
    }

    #[test]
    fn differing_anchors_yield_unequal_chains() {
        let mut first = CheckpointChain::default();
        let mut second = CheckpointChain::default();
        first.push(anchor(1), Digest::default());
        second.push(anchor(2), Digest::default());
        assert_ne!(first, second);
    }

    #[test]
    fn differing_commitments_yield_unequal_chains() {
        // A store with one write yields a commitment different from the genesis digest.
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(vec![Object::new(
            ObjectId::new(1),
            Version::new(1),
            vec![1],
        )]));

        let mut first = CheckpointChain::default();
        let mut second = CheckpointChain::default();
        first.push(anchor(1), Digest::default());
        second.push(anchor(1), store.commitment());
        assert_ne!(first, second);
    }
}
