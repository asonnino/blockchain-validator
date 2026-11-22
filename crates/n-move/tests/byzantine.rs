// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The threshold analysis of option A (`docs/e2e-design-v1.md` in MystenLabs/n-version-move)
//! against Byzantine voters: n validators, f Byzantine, w honest ones whose version is wrong, and
//! c = n - f - w correct ones. A wrong result can only become final when w + f ≥ t, the correct
//! one does when c ≥ t, and withheld or late votes delay rejections, never acceptances.

mod common;

use common::outcomes;
use execution::{object::ObjectId, transaction::Transaction};
use fake_engine::FakeTransaction;
use n_move::{
    ByzantineConfig, ClusterConfig, NMoveConfig, Outcome, Records, Rejection, Rule,
    SelectiveConfig, StrategyConfig, Threshold, assign,
};
use replica::config::PublicReplicaConfig;
use validator::engine::Engine;

/// Rounds a transaction may stay pending after its commit executes.
const CUTOFF: u64 = 10;
const SEED: u64 = 0;

/// f = 1 and W = 1: n = 2f + 2W + 1 with t = f + W + 1 for the simple majority, and
/// n = 3f + 2W + 1 with t = 2f + W + 1 for the honest majority.
#[derive(Clone, Copy, Debug)]
enum Committee {
    SimpleMajority,
    HonestMajority,
}

impl Committee {
    fn size(self) -> usize {
        match self {
            Self::SimpleMajority => 5,
            Self::HonestMajority => 6,
        }
    }

    fn rule(self) -> Rule {
        match self {
            Self::SimpleMajority => Rule::SimpleMajority,
            Self::HonestMajority => Rule::HonestMajority,
        }
    }

    /// The Byzantine validator.
    fn byzantine(self) -> usize {
        self.size() - 1
    }
}

/// `committee`'s config, where validators `wrong` share a wrong result on every transaction (one
/// cluster each), and the Byzantine validator votes as `strategy` says, if any.
fn config(
    committee: Committee,
    wrong: &[&[usize]],
    strategy: Option<StrategyConfig>,
    selective: Option<SelectiveConfig>,
) -> NMoveConfig {
    let size = committee.size();
    let assignment = assign(
        &PublicReplicaConfig::new_for_tests(size).committee(),
        size,
        SEED,
    );
    let clusters = wrong.iter().map(|authorities| ClusterConfig {
        versions: authorities
            .iter()
            .map(|authority| assignment[*authority].0)
            .collect(),
        probability: 1.0,
    });
    let byzantine = strategy.map(|strategy| ByzantineConfig {
        authorities: vec![committee.byzantine()],
        strategy,
        selective,
    });
    let mut config = NMoveConfig::default();
    config.seed = SEED;
    config.faults = 1;
    config.wrong = 1;
    config.threshold = Threshold::Rule(committee.rule());
    config.cutoff_rounds = CUTOFF;
    config.clusters = clusters.collect();
    config.byzantine = byzantine;
    config.recording()
}

/// Validator 0's records of `committee` deciding four creations under `config`.
fn run(committee: Committee, config: NMoveConfig) -> Records {
    let creations = (1..=4)
        .map(|id| FakeTransaction::success(vec![], vec![ObjectId::new(id)], vec![]).into())
        .collect::<Vec<Transaction>>();
    let processor = move |authority, public_config: &PublicReplicaConfig| {
        config
            .processor(authority, public_config)
            .expect("processor must build")
    };
    let mut results = common::run(committee.size(), SEED, processor, vec![creations]);
    results.swap_remove(0).2
}

fn all(records: &Records, expected: Outcome) -> bool {
    let decided = outcomes(records);
    decided.len() == 4 && decided.iter().all(|(_, outcome)| **outcome == expected)
}

fn none_wrong(records: &Records) -> bool {
    let decided = outcomes(records);
    let wrong = decided
        .iter()
        .any(|(_, outcome)| **outcome == Outcome::FinalWrong);
    decided.len() == 4 && !wrong
}

fn strategies() -> [(StrategyConfig, Option<SelectiveConfig>); 5] {
    let half = SelectiveConfig {
        share: 0.5,
        seed: 1,
    };
    [
        (StrategyConfig::Collude, None),
        (StrategyConfig::Collude, Some(half)),
        (StrategyConfig::Withhold, None),
        (StrategyConfig::Fixed(7), None),
        (StrategyConfig::Late(CUTOFF + 5), None),
    ]
}

#[test]
fn no_strategy_finalizes_a_wrong_result_below_the_threshold_or_blocks_a_correct_one() {
    // w + f < t throughout; c ≥ t in the first two rows (c = 3 of t = 3, and c = 4 of t = 4), and
    // c = 3 < t = 4 in the last, where the correct result needs the Byzantine vote.
    for (committee, wrong, correct) in [
        (Committee::SimpleMajority, &[0][..], true),
        (Committee::HonestMajority, &[0][..], true),
        (Committee::HonestMajority, &[0, 1][..], false),
    ] {
        for (strategy, selective) in strategies() {
            let records = run(
                committee,
                config(committee, &[wrong], Some(strategy), selective),
            );
            let expected = match correct {
                true => all(&records, Outcome::FinalCorrect),
                false => none_wrong(&records),
            };
            assert!(expected, "{committee:?} {strategy:?}: {records:?}");
        }
    }
}

#[test]
fn beyond_w_colluders_reaching_the_threshold_finalize_wrong_results() {
    // w = 2 > W: with the Byzantine vote, w + f = t = 3 under the simple majority. (Under the
    // honest majority, w + f = 3 < t = 4, which the first test covers.)
    let committee = Committee::SimpleMajority;
    let colluding = config(committee, &[&[0, 1]], Some(StrategyConfig::Collude), None);
    assert!(all(&run(committee, colluding), Outcome::FinalWrong));
}

#[test]
fn withheld_votes_delay_rejections() {
    // Two wrong against two correct: with every vote cast, no result can reach t = 3, so the
    // transactions are rejected as soon as the votes commit; a withheld vote could still tip
    // either side, so they wait for the cutoff.
    let committee = Committee::SimpleMajority;
    let fixed = run(
        committee,
        config(committee, &[&[0, 1]], Some(StrategyConfig::Fixed(7)), None),
    );
    let withheld = run(
        committee,
        config(committee, &[&[0, 1]], Some(StrategyConfig::Withhold), None),
    );
    assert!(all(&fixed, Outcome::Rejected(Rejection::Impossible)));
    assert!(all(&withheld, Outcome::Rejected(Rejection::Cutoff)));
    let delays = |records: &Records| {
        let transactions = records.transactions.iter();
        transactions
            .map(|record| record.decided_ms - record.executed_ms)
            .collect::<Vec<_>>()
    };
    let slowest_impossible = delays(&fixed).into_iter().max().unwrap();
    assert!(
        delays(&withheld)
            .into_iter()
            .all(|delay| delay > slowest_impossible)
    );
}

#[test]
fn votes_later_than_the_cutoff_turn_correct_results_into_rejections() {
    // c = 3 < t = 4 ≤ c + f: the correct result needs the Byzantine vote.
    let committee = Committee::HonestMajority;
    let late = |rounds| {
        config(
            committee,
            &[&[0, 1]],
            Some(StrategyConfig::Late(rounds)),
            None,
        )
    };
    assert!(all(&run(committee, late(2)), Outcome::FinalCorrect));
    assert!(all(
        &run(committee, late(CUTOFF + 5)),
        Outcome::Rejected(Rejection::Cutoff)
    ));
}
