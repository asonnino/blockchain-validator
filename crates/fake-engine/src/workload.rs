// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The fake engine's workload.

use std::collections::VecDeque;

use execution::{object::ObjectId, transaction::Transaction, workload::Workload};
use rand::{Rng, rngs::StdRng};

use crate::FakeTransaction;

/// Every transaction creates a fresh object or, at the configured share, updates one of the
/// stream's recent creations; its arguments are padded to the generator's size target.
pub struct FakeWorkload {
    /// The id stream, starting from the generator RNG's first draw.
    random: Option<u64>,
    /// The serialized size of a transaction before padding.
    overhead: usize,
    update_share: f64,
    /// The stream's latest creations, which updates pick from.
    recent: VecDeque<ObjectId>,
}

impl FakeWorkload {
    /// Object-id split: authority in the top 8 bits, the random stream in the low 56, so streams
    /// from different authorities can never collide.
    const AUTHORITY_SHIFT: u32 = 56;
    const RANDOM_MASK: u64 = (1 << Self::AUTHORITY_SHIFT) - 1;
    /// How many recent creations updates pick from.
    const RECENT: usize = 64;

    /// Makes `share` of the transactions, once the stream has created an object, read-write
    /// updates of its recent creations, so later transactions depend on earlier ones.
    pub fn with_updates(mut self, share: f64) -> Self {
        assert!((0.0..=1.0).contains(&share), "a share is a probability");
        self.update_share = share;
        self
    }

    fn creation(id: ObjectId, args: Vec<u8>) -> Transaction {
        FakeTransaction::success_with_args(vec![], vec![id], vec![], args).into()
    }
}

impl Default for FakeWorkload {
    fn default() -> Self {
        let bare = Self::creation(ObjectId::new(0), Vec::new());
        let overhead = bincode::serialized_size(&bare).expect("transactions serialize");
        Self {
            random: None,
            overhead: overhead as usize,
            update_share: 0.0,
            recent: VecDeque::with_capacity(Self::RECENT),
        }
    }
}

impl Workload for FakeWorkload {
    fn transaction(
        &mut self,
        namespace: u64,
        counter: u64,
        size: usize,
        rng: &mut StdRng,
    ) -> Transaction {
        assert!(
            size > self.overhead,
            "the {size}-byte size target must exceed the {}-byte transaction before padding",
            self.overhead
        );
        // Upstream's stream, wrapping: the first draw is uniform over `u64`, so a checked add
        // could overflow.
        let random = self.random.get_or_insert_with(|| rng.r#gen());
        *random = random.wrapping_add(counter);
        let id =
            ObjectId::new((namespace << Self::AUTHORITY_SHIFT) | (*random & Self::RANDOM_MASK));
        let padding = vec![0; size - self.overhead];
        // At a zero share the RNG draws nothing more, keeping the creations-only stream.
        if self.update_share > 0.0 && !self.recent.is_empty() && rng.gen_bool(self.update_share) {
            let updated = self.recent[rng.gen_range(0..self.recent.len())];
            let update = FakeTransaction::success_with_args(vec![], vec![], vec![updated], padding);
            // Repeated updates of one object differ only by their nonce.
            return Transaction::from(update).with_nonce(counter);
        }
        if self.recent.len() == Self::RECENT {
            self.recent.pop_front();
        }
        self.recent.push_back(id);
        Self::creation(id, padding)
    }
}

#[cfg(test)]
mod tests {
    use execution::{
        object::ObjectId,
        transaction::{AccessMode, Transaction},
        workload::Workload,
    };
    use rand::{SeedableRng, rngs::StdRng};

    use crate::FakeWorkload;

    /// Identical random streams under different namespaces stay disjoint: exactly the
    /// namespacing guarantee, since without the authority bits every id would collide.
    #[test]
    fn namespaces_separate_identical_random_streams() {
        let ids = |namespace| -> Vec<ObjectId> {
            let mut workload = FakeWorkload::default();
            let mut rng = StdRng::seed_from_u64(1);
            (0..100)
                .map(|counter| {
                    workload
                        .transaction(namespace, counter, 128, &mut rng)
                        .inputs()[0]
                        .0
                })
                .collect()
        };
        let (first, other) = (ids(1), ids(2));
        assert!(first.iter().all(|id| !other.contains(id)));
    }

    #[test]
    fn updates_follow_the_streams_own_creations() {
        let mut workload = FakeWorkload::default().with_updates(0.25);
        let mut rng = StdRng::seed_from_u64(1);
        let transactions: Vec<Transaction> = (0..200)
            .map(|counter| workload.transaction(1, counter, 128, &mut rng))
            .collect();
        let mut created = Vec::new();
        let mut updates = 0;
        let digests: Vec<_> = transactions
            .iter()
            .map(|tx| *tx.digest().as_bytes())
            .collect();
        for (index, digest) in digests.iter().enumerate() {
            assert!(
                !digests[..index].contains(digest),
                "transaction {index} repeats"
            );
        }
        for transaction in &transactions {
            assert_eq!(bincode::serialized_size(transaction).unwrap(), 128);
            match transaction.inputs() {
                [(id, AccessMode::WriteOnly)] => created.push(*id),
                [(id, AccessMode::ReadWrite)] => {
                    let recent = &created[created.len().saturating_sub(FakeWorkload::RECENT)..];
                    assert!(recent.contains(id), "updates target recent creations");
                    updates += 1;
                }
                inputs => panic!("unexpected inputs {inputs:?}"),
            }
        }
        assert!((25..75).contains(&updates), "{updates} updates");
    }
}
