// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Which version each validator runs.

use dag::committee::Committee;

use crate::digest::seeded;

/// One of the contract's versions, numbered from zero.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct VersionId(pub usize);

/// Assigns `versions` versions to the committee, indexed by authority, balancing the stake behind
/// each. Authorities go in descending stake, ties broken by `seed`, each to the version with the
/// least stake so far, ties to the lowest.
pub fn assign(committee: &Committee, versions: usize, seed: u64) -> Vec<VersionId> {
    assert!(
        (1..=committee.len()).contains(&versions),
        "every version needs a validator"
    );
    let mut order: Vec<_> = committee
        .authorities()
        .map(|authority| {
            let stake = committee.get_stake(authority).expect("committee member");
            let tiebreak = seeded(seed, &[&authority.index().to_be_bytes()]);
            (stake, tiebreak, authority)
        })
        .collect();
    order.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut stakes = vec![0; versions];
    let mut assignment = vec![VersionId(0); committee.len()];
    for (stake, _, authority) in order {
        let version = (0..versions)
            .min_by_key(|&version| stakes[version])
            .expect("at least one version");
        stakes[version] += stake;
        assignment[authority.index()] = VersionId(version);
    }
    assignment
}

#[cfg(test)]
mod tests {
    use dag::committee::Committee;

    use super::{VersionId, assign};

    #[test]
    fn as_many_versions_as_validators_is_a_permutation() {
        let committee = Committee::new_test(vec![1; 7]);
        let mut versions: Vec<_> = assign(&committee, 7, 3).iter().map(|v| v.0).collect();
        versions.sort();
        assert_eq!(versions, (0..7).collect::<Vec<_>>());
    }

    #[test]
    fn fewer_versions_split_the_stake_evenly() {
        let stakes = vec![5, 4, 3, 3, 2, 1, 1];
        let committee = Committee::new_test(stakes.clone());
        let assignment = assign(&committee, 3, 0);
        let mut per_version = [0; 3];
        for (VersionId(version), stake) in assignment.iter().zip(&stakes) {
            per_version[*version] += stake;
        }
        let (min, max) = (per_version.iter().min(), per_version.iter().max());
        assert!(max.unwrap() - min.unwrap() <= 1, "{per_version:?}");
    }

    #[test]
    fn assignments_follow_the_seed() {
        let committee = Committee::new_test(vec![1; 7]);
        assert_eq!(assign(&committee, 3, 1), assign(&committee, 3, 1));
        let first = assign(&committee, 7, 0);
        assert!((1..8).any(|seed| assign(&committee, 7, seed) != first));
    }
}
