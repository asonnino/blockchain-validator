// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Certification: tallying checkpoint votes into certified checkpoints.

use std::{collections::VecDeque, sync::Arc};

use dag::{
    authority::{Authority, AuthoritySet},
    block::BlockReference,
    committee::{Committee, Stake},
};
use execution::crypto::Digest;

use crate::checkpoint::{CertifiedCheckpoint, Checkpoint};

/// Accumulates the stake attesting one value of one checkpoint, remembering the delivering
/// sub-dag of each counted vote as proof material.
struct CheckpointAccumulator {
    votes: AuthoritySet,
    stake: Stake,
    threshold: Stake,
    /// The delivering sub-dag of each counted vote, in commit order.
    delivered_in: Vec<BlockReference>,
}

impl CheckpointAccumulator {
    /// `voters` bounds the proof buffer: one counted vote per authority, so it allocates once
    /// and never regrows.
    fn new(threshold: Stake, voters: usize) -> Self {
        Self {
            votes: Default::default(),
            stake: 0,
            threshold,
            delivered_in: Vec::with_capacity(voters),
        }
    }

    fn add(&mut self, vote: Authority, committee: &Committee, subdag: BlockReference) -> bool {
        let stake = committee.get_stake(vote).expect("Authority not found");
        if self.votes.insert(vote) {
            self.stake += stake;
            self.delivered_in.push(subdag);
        }
        self.stake >= self.threshold
    }

    fn voted(&self, author: Authority) -> bool {
        self.votes.present().any(|voter| voter == author)
    }

    fn clear(&mut self) -> Vec<BlockReference> {
        self.votes.clear();
        self.stake = 0;
        std::mem::take(&mut self.delivered_in)
    }
}

/// A minted checkpoint with its vote tally: this validator's own value, one accumulator per
/// attested commitment, and the certificate once a quorum forms (held until the watermark
/// passes it).
struct PendingCheckpoint {
    local: Checkpoint,
    accumulators: Vec<(Digest, CheckpointAccumulator)>,
    certificate: Option<CertifiedCheckpoint>,
}

impl PendingCheckpoint {
    fn round(&self) -> u64 {
        self.local.round()
    }
}

pub struct CheckpointCertifier {
    committee: Arc<Committee>,
    /// The consensus protocol's quorum threshold, passed in by the caller.
    quorum: Stake,
    /// The minted-but-not-contiguously-certified window, in commit order.
    pending: VecDeque<PendingCheckpoint>,
    /// The highest contiguously certified checkpoint; `None` until the first certification.
    watermark: Option<CertifiedCheckpoint>,
}

impl CheckpointCertifier {
    pub fn new(committee: Arc<Committee>, quorum: Stake) -> Self {
        Self {
            committee,
            quorum,
            pending: VecDeque::new(),
            watermark: None,
        }
    }

    /// Mints this validator's checkpoint for the sub-dag anchored at `anchor`. Certification
    /// can never outrun minting: a vote only commits after the sub-dag it attests, which we
    /// process first.
    pub fn push(&mut self, anchor: BlockReference, commitment: Digest) {
        self.pending.push_back(PendingCheckpoint {
            local: Checkpoint::new(anchor, commitment),
            accumulators: Vec::with_capacity(self.committee.len()),
            certificate: None,
        });
    }

    /// This validator's checkpoints not yet contiguously certified, in commit order.
    pub fn pending(&self) -> impl Iterator<Item = &Checkpoint> {
        self.pending.iter().map(|entry| &entry.local)
    }

    /// Counts `author`'s vote for `checkpoint`, delivered in the committed sub-dag anchored at
    /// `subdag`. The first vote per authority and checkpoint wins.
    pub fn record(&mut self, subdag: BlockReference, author: Authority, checkpoint: Checkpoint) {
        let voted_anchor = checkpoint.anchor();
        // A vote cannot be witnessed before its anchor's sub-dag committed by construction.
        // Window entries are in commit order, so anchor rounds are non-decreasing: binary
        // search to the vote's round, then walk only the same-round (multi-leader) run.
        let start = self
            .pending
            .partition_point(|entry| entry.round() < voted_anchor.round);
        let entry = self
            .pending
            .range_mut(start..)
            .take_while(|entry| entry.round() == voted_anchor.round)
            .find(|entry| entry.local.anchor() == voted_anchor);
        let Some(entry) = entry else {
            let certified_round = self
                .watermark
                .as_ref()
                .map_or(0, |certified| certified.round());
            if voted_anchor.round > certified_round {
                tracing::warn!(%author, ?voted_anchor, "checkpoint vote for an unknown anchor");
            }
            return;
        };
        // Already certified, awaiting contiguity: an observed certificate can never change.
        if entry.certificate.is_some() {
            return;
        }

        // One pass: dedupe the author and locate this commitment's accumulator.
        let commitment = checkpoint.commitment();
        let mut target = None;
        for (index, (attested, accumulator)) in entry.accumulators.iter().enumerate() {
            if accumulator.voted(author) {
                if *attested != commitment {
                    tracing::warn!(%author, ?voted_anchor, "conflicting checkpoint vote");
                }
                return;
            }
            if *attested == commitment {
                target = Some(index);
            }
        }
        let index = match target {
            Some(index) => index,
            None => {
                let accumulator = CheckpointAccumulator::new(self.quorum, self.committee.len());
                entry.accumulators.push((commitment, accumulator));
                entry.accumulators.len() - 1
            }
        };
        let (_, accumulator) = &mut entry.accumulators[index];
        if !accumulator.add(author, &self.committee, subdag) {
            return;
        }
        let proof = accumulator.clear();
        // A disagreement with our own pending checkpoint means our execution diverged.
        if entry.local.commitment() != commitment {
            tracing::error!(
                ?voted_anchor,
                "local checkpoint diverges from the certified checkpoint"
            );
        }
        entry.certificate = Some(CertifiedCheckpoint::new(checkpoint, proof));

        // Advance the watermark over the contiguously certified prefix, reclaiming it.
        while self
            .pending
            .front()
            .is_some_and(|entry| entry.certificate.is_some())
        {
            self.watermark = self.pending.pop_front().and_then(|entry| entry.certificate);
        }
    }

    /// The highest checkpoint such that it and all earlier checkpoints are certified.
    pub fn highest_certified(&self) -> Option<&CertifiedCheckpoint> {
        self.watermark.as_ref()
    }

    /// A certifier over a [`Committee::new_test`] committee that already minted checkpoints
    /// `1..=minted` matching [`Checkpoint::new_for_test`]: votes are only admissible for
    /// locally witnessed checkpoints.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(stake: Vec<Stake>, quorum: Stake, minted: u64) -> Self {
        let mut certifier = Self::new(Committee::new_test(stake), quorum);
        for n in 1..=minted {
            let checkpoint = Checkpoint::new_for_test(n);
            certifier.push(checkpoint.anchor(), checkpoint.commitment());
        }
        certifier
    }
}

#[cfg(test)]
mod tests {
    use dag::{authority::Authority, block::BlockReference, committee::Committee};
    use execution::crypto::Digest;

    use crate::{certifier::CheckpointCertifier, checkpoint::Checkpoint};

    /// Shorthand for [`Checkpoint::new_for_test`]: the vote for the `n`-th checkpoint.
    fn vote(n: u64) -> Checkpoint {
        Checkpoint::new_for_test(n)
    }

    /// The anchor of the `n`-th sub-dag delivering votes, after the checkpointed ones.
    fn subdag(n: u64) -> BlockReference {
        BlockReference::new_test(0, 100 + n)
    }

    #[test]
    fn certification_prunes_the_pending_checkpoints() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 2);
        for authority in 0..3 {
            certifier.record(subdag(0), Authority::new(authority), vote(1));
        }
        assert_eq!(certifier.pending().collect::<Vec<_>>(), [&vote(2)]);
    }

    #[test]
    fn quorum_certifies_at_threshold_with_the_delivering_subdags_as_proof() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);
        certifier.record(subdag(0), Authority::new(0), vote(1));
        certifier.record(subdag(0), Authority::new(1), vote(1));
        assert_eq!(certifier.highest_certified(), None);

        certifier.record(subdag(1), Authority::new(2), vote(1));
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &vote(1));
        assert_eq!(certified.proof(), [subdag(0), subdag(0), subdag(1)]);

        // A late matching vote does not grow the proof.
        certifier.record(subdag(2), Authority::new(3), vote(1));
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.proof(), [subdag(0), subdag(0), subdag(1)]);
    }

    #[test]
    fn duplicate_votes_add_no_stake() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 2, 1);
        certifier.record(subdag(0), Authority::new(0), vote(1));
        certifier.record(subdag(1), Authority::new(0), vote(1));
        assert_eq!(certifier.highest_certified(), None);

        certifier.record(subdag(2), Authority::new(1), vote(1));
        assert!(certifier.highest_certified().is_some());
    }

    #[test]
    fn a_single_vote_crossing_quorum_certifies() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![5, 1, 1, 1], 5, 1);
        certifier.record(subdag(0), Authority::new(0), vote(1));

        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.proof(), [subdag(0)]);
    }

    #[test]
    fn minority_conflict_does_not_block_certification() {
        let diverged = || Checkpoint::new(vote(1).anchor(), Digest::new_for_test(1));

        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);
        certifier.record(subdag(9), Authority::new(0), diverged());
        certifier.record(subdag(0), Authority::new(1), vote(1));
        certifier.record(subdag(0), Authority::new(2), vote(1));
        certifier.record(subdag(1), Authority::new(3), vote(1));

        // The majority value certifies, and the diverged vote's sub-dag stays out of the proof.
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &vote(1));
        assert_eq!(certified.proof(), [subdag(0), subdag(0), subdag(1)]);
    }

    #[test]
    fn conflicting_commitments_cannot_both_certify() {
        let diverged = || Checkpoint::new(vote(1).anchor(), Digest::new_for_test(1));

        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);
        certifier.record(subdag(0), Authority::new(0), vote(1));
        certifier.record(subdag(0), Authority::new(1), vote(1));
        certifier.record(subdag(0), Authority::new(2), diverged());
        certifier.record(subdag(0), Authority::new(3), diverged());
        assert_eq!(certifier.highest_certified(), None);

        // A conflicting second vote is ignored: the first wins.
        certifier.record(subdag(1), Authority::new(0), diverged());
        assert_eq!(certifier.highest_certified(), None);
    }

    #[test]
    fn a_quorum_diverging_from_the_local_checkpoint_still_certifies() {
        let diverged = || Checkpoint::new(vote(1).anchor(), Digest::new_for_test(1));

        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);
        for authority in 0..3 {
            certifier.record(subdag(0), Authority::new(authority), diverged());
        }

        // The certificate carries the quorum's commitment, not ours, and the entry is
        // reclaimed: the quorum decides the truth even when we are the diverged one.
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &diverged());
        assert!(certifier.pending().next().is_none());
    }

    #[test]
    fn same_round_checkpoints_are_distinguished() {
        // Multi-leader protocols commit several anchors in the same round.
        let round = 7;
        let first = || Checkpoint::new(BlockReference::new_test(0, round), Digest::default());
        let second = || Checkpoint::new(BlockReference::new_test(1, round), Digest::default());
        let unminted = || Checkpoint::new(BlockReference::new_test(2, round), Digest::default());

        let mut certifier = CheckpointCertifier::new(Committee::new_test(vec![1; 4]), 3);
        certifier.push(first().anchor(), Digest::default());
        certifier.push(second().anchor(), Digest::default());

        // A same-round anchor that was never minted certifies nothing.
        for authority in 0..4 {
            certifier.record(subdag(9), Authority::new(authority), unminted());
        }
        assert_eq!(certifier.highest_certified(), None);

        // Both same-round checkpoints certify, even voted out of mint order.
        for authority in 0..3 {
            certifier.record(subdag(0), Authority::new(authority), second());
        }
        assert_eq!(certifier.highest_certified(), None);
        for authority in 0..3 {
            certifier.record(subdag(1), Authority::new(authority), first());
        }
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &second());
        assert!(certifier.pending().next().is_none());
    }

    #[test]
    fn votes_for_unminted_checkpoints_are_rejected() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);

        // Even a full quorum for a never-witnessed checkpoint allocates and certifies nothing.
        for authority in 0..4 {
            certifier.record(subdag(0), Authority::new(authority), vote(5));
        }
        assert_eq!(certifier.highest_certified(), None);
    }

    #[test]
    fn watermark_is_contiguous() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 3);
        for authority in 0..3 {
            certifier.record(subdag(0), Authority::new(authority), vote(2));
        }
        assert_eq!(certifier.highest_certified(), None);

        // A late vote for the certified-but-not-yet-contiguous checkpoint changes nothing.
        certifier.record(subdag(9), Authority::new(3), vote(2));

        for authority in 0..3 {
            certifier.record(subdag(1), Authority::new(authority), vote(1));
        }
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &vote(2));
        assert_eq!(certified.proof(), [subdag(0), subdag(0), subdag(0)]);

        // The watermark keeps advancing from where it stands.
        for authority in 0..3 {
            certifier.record(subdag(2), Authority::new(authority), vote(3));
        }
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.checkpoint(), &vote(3));
    }

    #[test]
    fn votes_below_the_watermark_are_ignored() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![1; 4], 3, 1);
        for authority in 0..3 {
            certifier.record(subdag(0), Authority::new(authority), vote(1));
        }

        // A full quorum re-voting a reclaimed checkpoint cannot alter the certificate.
        for authority in 0..4 {
            certifier.record(subdag(5), Authority::new(authority), vote(1));
        }
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.proof(), [subdag(0), subdag(0), subdag(0)]);
    }

    #[test]
    fn votes_are_stake_weighted() {
        let mut certifier = CheckpointCertifier::new_for_test(vec![5, 1, 1, 1], 5, 1);
        certifier.record(subdag(0), Authority::new(1), vote(1));
        certifier.record(subdag(0), Authority::new(2), vote(1));
        certifier.record(subdag(0), Authority::new(3), vote(1));
        assert_eq!(certifier.highest_certified(), None);

        // The heavy validator tips 3 stake over the threshold.
        certifier.record(subdag(1), Authority::new(0), vote(1));
        let certified = certifier.highest_certified().expect("certified checkpoint");
        assert_eq!(certified.proof().len(), 4);
    }
}
