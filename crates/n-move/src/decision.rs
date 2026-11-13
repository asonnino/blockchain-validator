// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The decision rule: from the committed votes, each transaction ends final with a result, or
//! rejected. Every validator applies it to the same committed votes, so all decide alike.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    digest::{InputDigest, ResultDigest},
    vote::{Position, Vote},
};
use dag::{
    authority::Authority,
    committee::{Committee, Stake},
};

#[derive(PartialEq, Debug)]
pub enum Decision {
    /// The result reached the threshold.
    Final(ResultDigest),
    Rejected(Rejection),
}

/// Why a transaction was rejected.
#[derive(PartialEq, Debug)]
pub enum Rejection {
    /// No result could reach the threshold any more.
    Impossible,
    /// The transaction was still pending its cutoff rounds after its commit.
    Cutoff,
    /// A result would have reached the threshold, but on inputs an earlier decision changed.
    EarlierTransaction,
}

/// A committed vote, with the anchor round of the commit that carried it.
struct Ballot {
    author: Authority,
    input: InputDigest,
    result: ResultDigest,
    round: u64,
}

struct Pending {
    /// The anchor round of the transaction's commit.
    round: u64,
    /// In commit order, one per author.
    ballots: Vec<Ballot>,
}

/// Decides transactions in commit order: the caller records each committed sub-dag with
/// [`Decider::commit`], then decides with [`Decider::decide`], applying each decision to its state
/// before the next.
pub struct Decider {
    committee: Arc<Committee>,
    threshold: Stake,
    cutoff: u64,
    commits: u64,
    /// The latest commit's anchor round.
    round: u64,
    pending: BTreeMap<Position, Pending>,
}

impl Decider {
    /// `threshold` is the stake a result needs, and `cutoff` the rounds a transaction may stay
    /// pending after its commit.
    pub fn new(committee: Arc<Committee>, threshold: Stake, cutoff: u64) -> Self {
        assert!(
            (1..=committee.total_stake()).contains(&threshold),
            "unreachable threshold"
        );
        Self {
            committee,
            threshold,
            cutoff,
            commits: 0,
            round: 0,
            pending: BTreeMap::new(),
        }
    }

    /// Records a commit: the votes it carries, by author, then its `transactions`, which take the
    /// next commit's positions. Only an author's first vote per transaction counts, and votes on
    /// transactions not pending are dropped.
    pub fn commit(
        &mut self,
        round: u64,
        transactions: usize,
        votes: impl IntoIterator<Item = (Authority, Vote)>,
    ) {
        self.round = round;
        for (author, vote) in votes {
            let Some(pending) = self.pending.get_mut(&vote.position) else {
                continue;
            };
            if pending.ballots.iter().all(|ballot| ballot.author != author) {
                pending.ballots.push(Ballot {
                    author,
                    input: vote.input,
                    result: vote.result,
                    round,
                });
            }
        }
        let commit = self.commits;
        self.commits += 1;
        for index in 0..transactions {
            let position = Position {
                commit,
                index: index
                    .try_into()
                    .expect("commits hold fewer than 2^32 transactions"),
            };
            let pending = Pending {
                round,
                ballots: Vec::new(),
            };
            self.pending.insert(position, pending);
        }
    }

    /// The transaction [`Decider::decide`] decides next.
    pub fn head(&self) -> Option<Position> {
        self.pending.keys().next().copied()
    }

    /// Decides the oldest pending transaction, given the digest of its inputs on the state that
    /// the earlier decisions produced; votes count only on those inputs. `None` while undecided.
    pub fn decide(&mut self, input: InputDigest) -> Option<(Position, Decision)> {
        let (&position, pending) = self.pending.first_key_value()?;
        let decision = self.evaluate(pending, input)?;
        self.pending.pop_first();
        Some((position, decision))
    }

    fn evaluate(&self, pending: &Pending, input: InputDigest) -> Option<Decision> {
        let deadline = pending.round + self.cutoff;
        let (mut counted, mut all) = (Vec::new(), Vec::new());
        let mut voted = 0;
        for ballot in pending
            .ballots
            .iter()
            .filter(|ballot| ballot.round <= deadline)
        {
            let stake = self.committee.get_stake(ballot.author).unwrap_or(0);
            voted += stake;
            tally(&mut all, ballot.result, stake);
            if ballot.input == input && tally(&mut counted, ballot.result, stake) >= self.threshold
            {
                return Some(Decision::Final(ballot.result));
            }
        }
        let best = counted.iter().map(|(_, stake)| *stake).max().unwrap_or(0);
        let missing = self.committee.total_stake() - voted;
        let impossible = best + missing < self.threshold;
        if !impossible && self.round <= deadline {
            return None;
        }
        let reason = if all.iter().any(|(_, stake)| *stake >= self.threshold) {
            Rejection::EarlierTransaction
        } else if impossible {
            Rejection::Impossible
        } else {
            Rejection::Cutoff
        };
        Some(Decision::Rejected(reason))
    }
}

/// Adds `stake` to `result`'s tally, returning its new total.
fn tally(tallies: &mut Vec<(ResultDigest, Stake)>, result: ResultDigest, stake: Stake) -> Stake {
    match tallies.iter_mut().find(|(digest, _)| *digest == result) {
        Some((_, total)) => {
            *total += stake;
            *total
        }
        None => {
            tallies.push((result, stake));
            stake
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Decider, Decision, Rejection};
    use crate::{
        digest::{InputDigest, ResultDigest},
        vote::{Position, Vote},
    };
    use dag::{
        authority::Authority,
        committee::{Committee, Stake},
    };

    /// The input digest of the decided state.
    const INPUT: u8 = 0;

    fn decider(stakes: Vec<Stake>, threshold: Stake, cutoff: u64) -> Decider {
        Decider::new(Committee::new_test(stakes), threshold, cutoff)
    }

    fn position(index: u32) -> Position {
        Position { commit: 0, index }
    }

    /// `author`'s vote for `result` on the transaction at `position`, on `input`.
    fn vote_at(author: usize, position: Position, input: u8, result: u8) -> (Authority, Vote) {
        let vote = Vote {
            position,
            input: InputDigest::new_for_test(input),
            result: ResultDigest::new_for_test(result),
        };
        (Authority::from(author), vote)
    }

    /// `author`'s vote on transaction `index` of the first commit.
    fn vote(author: usize, index: u32, input: u8, result: u8) -> (Authority, Vote) {
        vote_at(author, position(index), input, result)
    }

    fn decide(decider: &mut Decider) -> Option<(Position, Decision)> {
        decider.decide(InputDigest::new_for_test(INPUT))
    }

    fn rejected(index: u32, rejection: Rejection) -> Option<(Position, Decision)> {
        Some((position(index), Decision::Rejected(rejection)))
    }

    #[test]
    fn transactions_take_positions_across_commits() {
        let mut decider = decider(vec![1; 4], 3, 2);
        decider.commit(10, 2, []);
        // Empty commits take a number too.
        decider.commit(11, 0, []);
        // Votes on a transaction's deadline round still count.
        let first =
            (0..3).flat_map(|author| [vote(author, 0, INPUT, 1), vote(author, 1, INPUT, 2)]);
        decider.commit(12, 1, first);
        let later = Position {
            commit: 2,
            index: 0,
        };
        decider.commit(14, 0, (0..3).map(|author| vote_at(author, later, INPUT, 3)));
        let decisions: Vec<_> = (0..3).map(|_| decide(&mut decider)).collect();
        let final_ = |position, result| {
            Some((
                position,
                Decision::Final(ResultDigest::new_for_test(result)),
            ))
        };
        let expected = [
            final_(position(0), 1),
            final_(position(1), 2),
            final_(later, 3),
        ];
        assert_eq!(decisions, expected);
    }

    #[test]
    fn at_or_below_half_the_first_result_to_reach_the_threshold_wins() {
        let mut decider = decider(vec![1; 5], 2, 10);
        decider.commit(1, 1, []);
        let votes = [1, 2, 2, 1, 1].into_iter().enumerate();
        decider.commit(
            2,
            0,
            votes.map(|(author, result)| vote(author, 0, INPUT, result)),
        );
        let final_ = Some((position(0), Decision::Final(ResultDigest::new_for_test(2))));
        assert_eq!(decide(&mut decider), final_);
    }

    #[test]
    fn impossibility_fires_on_the_commit_that_makes_it_apply() {
        let mut decider = decider(vec![1; 4], 3, 10);
        decider.commit(1, 1, []);
        decider.commit(2, 0, [vote(0, 0, INPUT, 1), vote(1, 0, INPUT, 2)]);
        assert_eq!(decide(&mut decider), None);
        decider.commit(3, 0, [vote(2, 0, INPUT, 3)]);
        assert_eq!(decide(&mut decider), rejected(0, Rejection::Impossible));
    }

    #[test]
    fn the_cutoff_rejects_and_later_votes_never_finalize() {
        let mut decider = decider(vec![1; 4], 3, 2);
        decider.commit(10, 1, []);
        decider.commit(11, 0, [vote(0, 0, INPUT, 1), vote(1, 0, INPUT, 1)]);
        decider.commit(12, 0, []);
        assert_eq!(decide(&mut decider), None);
        decider.commit(13, 0, [vote(2, 0, INPUT, 1)]);
        assert_eq!(decide(&mut decider), rejected(0, Rejection::Cutoff));
    }

    #[test]
    fn votes_on_stale_inputs_do_not_count() {
        let mut decider = decider(vec![1; 4], 3, 10);
        decider.commit(1, 1, []);
        let stale = 9;
        let votes = [
            vote(0, 0, INPUT, 1),
            vote(1, 0, stale, 1),
            vote(2, 0, INPUT, 1),
        ];
        decider.commit(2, 0, votes);
        assert_eq!(decide(&mut decider), None);
        decider.commit(3, 0, [vote(3, 0, INPUT, 1)]);
        let final_ = Some((position(0), Decision::Final(ResultDigest::new_for_test(1))));
        assert_eq!(decide(&mut decider), final_);
    }

    #[test]
    fn dependents_of_a_rejection_are_rejected_for_it() {
        let mut decider = decider(vec![1; 4], 3, 10);
        decider.commit(1, 2, []);
        // The first transaction splits its votes; each side executed the second on its own result.
        let first = [1, 1, 2, 2].into_iter().enumerate();
        let second = [11, 11, 12, 12].into_iter().enumerate();
        let votes = first
            .map(|(author, result)| vote(author, 0, INPUT, result))
            .chain(second.map(|(author, input)| vote(author, 1, input, 5)));
        decider.commit(2, 0, votes);
        assert_eq!(decide(&mut decider), rejected(0, Rejection::Impossible));
        assert_eq!(
            decide(&mut decider),
            rejected(1, Rejection::EarlierTransaction)
        );
    }

    #[test]
    fn the_cutoff_blames_stale_votes_that_would_have_decided() {
        let mut decider = decider(vec![1; 5], 3, 2);
        decider.commit(10, 1, []);
        let stale = 9;
        let votes = [
            vote(0, 0, stale, 1),
            vote(1, 0, stale, 1),
            vote(2, 0, INPUT, 1),
        ];
        decider.commit(11, 0, votes);
        assert_eq!(decide(&mut decider), None);
        decider.commit(13, 0, []);
        assert_eq!(
            decide(&mut decider),
            rejected(0, Rejection::EarlierTransaction)
        );
    }

    #[test]
    fn only_an_authors_first_vote_counts() {
        let mut decider = decider(vec![1; 4], 3, 10);
        decider.commit(1, 1, []);
        let votes = [
            vote(0, 0, INPUT, 1),
            vote(0, 0, INPUT, 1),
            vote(1, 0, INPUT, 1),
        ];
        decider.commit(2, 0, votes);
        decider.commit(3, 0, [vote(0, 0, INPUT, 2)]);
        assert_eq!(decide(&mut decider), None);
        decider.commit(4, 0, [vote(2, 0, INPUT, 1)]);
        let final_ = Some((position(0), Decision::Final(ResultDigest::new_for_test(1))));
        assert_eq!(decide(&mut decider), final_);
    }

    #[test]
    fn votes_weigh_their_authors_stake() {
        let mut decider = decider(vec![3, 1, 1, 1], 4, 10);
        decider.commit(1, 1, []);
        decider.commit(2, 0, [vote(0, 0, INPUT, 1)]);
        assert_eq!(decide(&mut decider), None);
        decider.commit(3, 0, [vote(1, 0, INPUT, 1)]);
        let final_ = Some((position(0), Decision::Final(ResultDigest::new_for_test(1))));
        assert_eq!(decide(&mut decider), final_);
    }

    #[test]
    fn later_transactions_wait_for_the_head_and_keep_their_timely_votes() {
        let mut decider = decider(vec![1; 4], 3, 2);
        decider.commit(10, 2, []);
        let head = [vote(0, 0, INPUT, 1), vote(1, 0, INPUT, 1)];
        let next = (0..3).map(|author| vote(author, 1, INPUT, 2));
        decider.commit(11, 0, head.into_iter().chain(next));
        assert_eq!(
            (decide(&mut decider), decider.head()),
            (None, Some(position(0)))
        );
        decider.commit(14, 0, []);
        assert_eq!(decide(&mut decider), rejected(0, Rejection::Cutoff));
        let final_ = Some((position(1), Decision::Final(ResultDigest::new_for_test(2))));
        assert_eq!(decide(&mut decider), final_);
        // Votes on decided transactions are dropped.
        decider.commit(15, 0, [vote(3, 0, INPUT, 1)]);
        assert_eq!(decider.head(), None);
    }
}
