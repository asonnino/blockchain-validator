// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Commit timestamps.

use std::{collections::HashMap, mem, sync::Arc};

use dag::{
    block::BlockReference,
    committee::{Committee, Stake},
    consensus::CommittedSubDag,
};

/// Times committed sub-dags as Sui does: the stake-weighted median of the timestamps of the
/// anchor's parents (the blocks it includes from the round below, one per authority), never
/// earlier than the previous commit's. A leader must include parents from a quorum of
/// authorities, so a minority of stake cannot carry the median past the honest parents' stamps.
// Todo: take the timestamp from mysticeti's sub-dags once they carry one
// (asonnino/mysticeti#339), and drop this clock.
pub struct CommitClock {
    committee: Arc<Committee>,
    /// Timestamps in milliseconds of the committed blocks from the latest anchor's round and the
    /// one below, the only ones that may parent a later anchor.
    recent: HashMap<BlockReference, u64>,
    /// The latest anchor's round.
    round: u64,
    latest_ms: u64,
    /// The anchor's parents as `(timestamp_ms, stake)`, and the authorities they cover; reused
    /// across commits.
    parents: Vec<(u64, Stake)>,
    covered: Vec<bool>,
}

impl CommitClock {
    pub fn new(committee: Arc<Committee>) -> Self {
        let covered = vec![false; committee.len()];
        Self {
            committee,
            recent: HashMap::new(),
            round: 0,
            latest_ms: 0,
            parents: Vec::new(),
            covered,
        }
    }

    /// Records `subdag`'s blocks and returns its
    /// [`CommitInfo::timestamp_ms`](execution::engine::CommitInfo::timestamp_ms). Call it for
    /// every committed sub-dag, in commit order.
    pub fn commit(&mut self, subdag: &CommittedSubDag) -> u64 {
        let round = subdag.anchor.round;
        // Later anchors are no older than this one, so older blocks never parent one.
        if round > self.round {
            self.round = round;
            self.recent.retain(|block, _| block.round + 1 >= round);
        }
        for block in subdag
            .blocks
            .iter()
            .filter(|block| block.round() + 1 >= round)
        {
            let timestamp_ms = block.timestamp().as_millis() as u64;
            self.recent.insert(*block.reference(), timestamp_ms);
        }
        // Sub-dags sort by round, so the anchor comes last.
        let anchor = subdag
            .blocks
            .iter()
            .rev()
            .find(|block| *block.reference() == subdag.anchor)
            .expect("sub-dags contain their anchor");

        self.parents.clear();
        self.covered.fill(false);
        let below = anchor
            .includes()
            .iter()
            .filter(|parent| parent.round + 1 == round);
        for parent in below {
            // An authority counts once, by the first of its blocks the anchor includes: the one
            // the anchor votes for.
            if mem::replace(&mut self.covered[parent.authority.index()], true) {
                continue;
            }
            let timestamp_ms = self.recent.get(parent);
            let stake = self.committee.get_stake(parent.authority);
            self.parents.push((
                *timestamp_ms.expect("parents commit no later than their anchor"),
                stake.expect("parents come from the committee"),
            ));
        }
        self.parents
            .sort_unstable_by_key(|(timestamp_ms, _)| *timestamp_ms);
        let half = self.parents.iter().map(|(_, stake)| stake).sum::<Stake>() / 2;
        let mut cumulative = 0;
        let median = self.parents.iter().find(|(_, stake)| {
            cumulative += stake;
            cumulative > half
        });
        if let Some((median_ms, _)) = median {
            self.latest_ms = self.latest_ms.max(*median_ms);
        }
        self.latest_ms
    }
}

#[cfg(test)]
mod tests {
    use dag::{
        authority::Authority,
        block::{Block, data::Data},
        committee::Committee,
        consensus::CommittedSubDag,
        crypto::{CryptoEngine, Signer},
    };
    use rand::{SeedableRng, rngs::StdRng};

    use crate::time::CommitClock;

    /// Far enough ahead to stand out, yet clear of overflow in nanoseconds.
    const FAR_MS: u64 = 1 << 40;

    fn signed(
        crypto: &CryptoEngine,
        author: usize,
        round: u64,
        includes: &[&Data<Block>],
        timestamp_ms: u64,
    ) -> Data<Block> {
        let includes = includes.iter().map(|parent| *parent.reference()).collect();
        let author = Authority::from(author);
        let timestamp_ns = timestamp_ms * 1_000_000;
        let block = Block::new(author, round, includes, Vec::new(), timestamp_ns, crypto);
        Data::new(block)
    }

    fn block(
        author: usize,
        round: u64,
        includes: &[&Data<Block>],
        timestamp_ms: u64,
    ) -> Data<Block> {
        signed(
            &CryptoEngine::disabled(),
            author,
            round,
            includes,
            timestamp_ms,
        )
    }

    /// One round-2 block per stamp, in author order.
    fn parents(timestamps_ms: &[u64]) -> Vec<Data<Block>> {
        let stamps = timestamps_ms.iter().enumerate();
        stamps
            .map(|(author, ms)| block(author, 2, &[], *ms))
            .collect()
    }

    fn subdag(anchor: Data<Block>, others: &[Data<Block>]) -> CommittedSubDag {
        CommittedSubDag::new(*anchor.reference(), [others, &[anchor]].concat())
    }

    fn clock(stakes: Vec<u64>) -> CommitClock {
        CommitClock::new(Committee::new_test(stakes))
    }

    #[test]
    fn a_minority_cannot_move_the_time() {
        let mut clock = clock(vec![1; 4]);
        // The anchor's author lists its own older block first, then three parents out of order,
        // one of them far ahead; only the parents' median counts.
        let own = block(3, 1, &[], FAR_MS);
        let parents = parents(&[1001, FAR_MS, 1000]);
        let includes = [&own, &parents[0], &parents[1], &parents[2]];
        let anchor = block(3, 3, &includes, FAR_MS);
        let committed = [vec![own], parents].concat();
        assert_eq!(clock.commit(&subdag(anchor, &committed)), 1001);
    }

    #[test]
    fn an_authority_counts_once() {
        let mut clock = clock(vec![1; 4]);
        let parents = parents(&[1000, 1001, 1002]);
        // Signed, so twins from the same author and round have distinct references.
        let crypto = CryptoEngine::enabled(Signer::new(&mut StdRng::seed_from_u64(0)));
        let twins: Vec<_> = (0..3)
            .map(|offset| signed(&crypto, 3, 2, &[], FAR_MS + offset))
            .collect();
        let mut includes: Vec<_> = parents.iter().chain(&twins).collect();
        includes.push(&twins[0]);
        let anchor = block(0, 3, &includes, 0);
        let committed = [parents, twins].concat();
        assert_eq!(clock.commit(&subdag(anchor, &committed)), 1002);
    }

    #[test]
    fn the_median_weighs_stake() {
        let mut clock = clock(vec![1, 1, 1, 4]);
        let parents = parents(&[1000, 2000, 3000, 1500]);
        let anchor = block(0, 3, &parents.iter().collect::<Vec<_>>(), 0);
        assert_eq!(clock.commit(&subdag(anchor, &parents)), 1500);
    }

    #[test]
    fn parents_committed_earlier_still_count() {
        let mut clock = clock(vec![1; 4]);
        let parents = parents(&[5000, 5001, 5002, 5003]);
        let includes: Vec<_> = parents.iter().collect();
        let first = block(0, 3, &includes, 0);
        let second = block(1, 3, &includes, 0);
        assert_eq!(clock.commit(&subdag(first, &parents)), 5002);
        assert_eq!(clock.commit(&subdag(second, &[])), 5002);
    }

    #[test]
    fn the_time_never_runs_backwards() {
        let mut clock = clock(vec![1; 4]);
        let parents = parents(&[5000; 4]);
        let includes: Vec<_> = parents.iter().collect();
        let first = block(0, 3, &includes, 1000);
        let others: Vec<_> = (1..4)
            .map(|author| block(author, 3, &includes, 1000))
            .collect();
        let round_three: Vec<_> = [&first].into_iter().chain(&others).collect();
        let anchor = block(1, 4, &round_three, 1000);
        assert_eq!(clock.commit(&subdag(first, &parents)), 5000);
        assert_eq!(clock.commit(&subdag(anchor, &others)), 5000);
        // Only blocks that may still parent an anchor stay recorded.
        assert!(clock.recent.keys().all(|block| block.round >= 3));
    }
}
