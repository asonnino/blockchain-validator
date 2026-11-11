// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The fake engine's workload.

use execution::{object::ObjectId, transaction::Transaction, workload::Workload};
use rand::{Rng, rngs::StdRng};

use crate::FakeTransaction;

/// Every transaction creates a fresh object, its arguments padded to the generator's size target.
pub struct FakeWorkload {
    /// The id stream, starting from the generator RNG's first draw.
    random: Option<u64>,
    /// The serialized size of a creation before padding.
    overhead: usize,
}

impl FakeWorkload {
    /// Object-id split: authority in the top 8 bits, the random stream in the low 56, so streams
    /// from different authorities can never collide.
    const AUTHORITY_SHIFT: u32 = 56;
    const RANDOM_MASK: u64 = (1 << Self::AUTHORITY_SHIFT) - 1;

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
        Self::creation(id, vec![0; size - self.overhead])
    }
}

#[cfg(test)]
mod tests {
    use execution::{object::ObjectId, workload::Workload};
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
}
