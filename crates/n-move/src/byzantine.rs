// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! How a Byzantine validator votes. Everything else it does, executing, deciding, and minting
//! checkpoints, stays honest.

use dag::{authority::Authority, committee::Stake};
use execution::transaction::Transaction;

use crate::{
    digest::{ResultDigest, chance},
    vote::Vote,
};

pub struct Byzantine {
    pub strategy: Strategy,
    pub selective: Option<Selective>,
    /// Every Byzantine authority, so collusion counts only honest stake.
    pub authorities: Vec<Authority>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Strategy {
    /// The wrong result with the most honest stake, or the own result if no honest version is
    /// wrong: the worst case of the threshold analysis.
    Collude,
    /// A result no version computes, the same on every transaction.
    Fixed(ResultDigest),
    Withhold,
    /// The own result, `rounds` rounds later than an honest vote; never, past the last round.
    Late {
        rounds: u64,
    },
}

/// Byzantine on a seeded `share` of transactions, honest on the rest.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Selective {
    pub share: f64,
    pub seed: u64,
}

/// What a validator does with its vote on a transaction.
pub(crate) enum Ballot {
    Now(Vote),
    /// Cast once the anchor round reaches the first field.
    Due(u64, Vote),
    Withheld,
}

/// What collusion compares: the reference result, and every honest authority's result with its
/// stake.
pub(crate) struct Results {
    pub truth: ResultDigest,
    pub honest: Vec<(Stake, ResultDigest)>,
}

impl Byzantine {
    /// The ballot instead of `honest`, the vote on `transaction` an honest validator casts at
    /// `round`. Only collusion calls `results`.
    pub(crate) fn ballot(
        &self,
        honest: Vote,
        transaction: &Transaction,
        round: u64,
        results: impl FnOnce() -> Results,
    ) -> Ballot {
        if let Some(selective) = self.selective
            && chance(selective.seed, &[transaction.digest().as_bytes()]) >= selective.share
        {
            return Ballot::Now(honest);
        }
        match self.strategy {
            Strategy::Collude => Ballot::Now(Self::collude(honest, results())),
            Strategy::Fixed(result) => Ballot::Now(Vote { result, ..honest }),
            Strategy::Withhold => Ballot::Withheld,
            Strategy::Late { rounds } => match round.checked_add(rounds) {
                Some(due) => Ballot::Due(due, honest),
                None => Ballot::Withheld,
            },
        }
    }

    /// `honest` voting the wrong result with the most honest stake, the first in authority order
    /// on ties, if there is one.
    fn collude(honest: Vote, results: Results) -> Vote {
        let mut tally: Vec<(ResultDigest, Stake)> = Vec::new();
        let wrong = results
            .honest
            .into_iter()
            .filter(|(_, result)| *result != results.truth);
        for (stake, result) in wrong {
            match tally.iter_mut().find(|(tallied, _)| *tallied == result) {
                Some((_, total)) => *total += stake,
                None => tally.push((result, stake)),
            }
        }
        let largest = tally
            .into_iter()
            .reduce(|best, next| match next.1 > best.1 {
                true => next,
                false => best,
            });
        largest.map_or(honest, |(result, _)| Vote { result, ..honest })
    }
}

#[cfg(test)]
mod tests {
    use super::{Byzantine, Results};
    use crate::{
        digest::{InputDigest, ResultDigest},
        vote::{Position, Vote},
    };

    #[test]
    fn colluders_vote_the_wrong_result_with_the_most_honest_stake() {
        let [truth, own, a, b] = [0, 1, 2, 3].map(ResultDigest::new_for_test);
        let collude = |honest: &[(u64, ResultDigest)]| {
            let vote = Vote {
                position: Position {
                    commit: 0,
                    index: 0,
                },
                input: InputDigest::new_for_test(0),
                result: own,
            };
            let honest = honest.to_vec();
            Byzantine::collude(vote, Results { truth, honest }).result
        };
        // Stake adds up over authorities sharing a result.
        assert_eq!(collude(&[(2, a), (1, b), (2, b), (5, truth)]), b);
        // Ties go to the first in authority order.
        assert_eq!(collude(&[(1, b), (1, a)]), b);
        assert_eq!(collude(&[(1, a), (1, b)]), a);
        // With no honest result wrong, the own one.
        assert_eq!(collude(&[(1, truth)]), own);
    }
}
