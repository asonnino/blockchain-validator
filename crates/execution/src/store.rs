// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The versioned in-memory object store.

use std::collections::BTreeMap;

use digest::Digest as _;

use crate::{
    crypto::{Digest, Hasher},
    effects::ExecutionOutput,
    object::{Object, ObjectId, Version},
};

/// A read-only view of the store against which transactions execute.
pub trait StateView {
    /// The latest version of an object, or `None` if it was never written.
    fn latest(&self, id: &ObjectId) -> Option<&Object>;
}

/// An in-memory versioned object store.
///
/// Every write creates a new `(id, version)` entry; existing entries are never overwritten. The
/// store holds derived state only — durability is the job of the consensus WAL, which execution
/// replays on restart.
#[derive(Default, PartialEq, Debug)]
pub struct InMemoryStore {
    /// Every object version ever written.
    objects: BTreeMap<(ObjectId, Version), Object>,
    /// The highest written version of each object.
    latest: BTreeMap<ObjectId, Version>,
    /// Rolling commitment to the full ordered write history; a persistent backend must persist
    /// it in the same atomic batch as its delta's writes.
    commitment: Digest,
}

impl InMemoryStore {
    /// The object frozen at exactly `(id, version)`, or `None` if absent.
    pub fn get(&self, id: &ObjectId, version: Version) -> Option<&Object> {
        self.objects.get(&(*id, version))
    }

    /// Consumes an execution output: each written object moves into the store at its version,
    /// which becomes the latest version of its id. Aborted outputs carry no writes, so applying
    /// them is a no-op.
    pub fn apply(&mut self, output: ExecutionOutput) {
        for object in output.into_writes() {
            let (id, version) = (object.id(), object.version());
            let previous = self.latest.insert(id, version);
            assert!(
                previous.is_none_or(|p| p < version),
                "object versions must move forward"
            );

            // Only the trailing element is variable, so no length framing is needed.
            let mut hasher = Hasher::new();
            hasher.update(self.commitment.as_bytes());
            hasher.update(id.as_bytes());
            hasher.update(version.as_u64().to_be_bytes());
            hasher.update(object.contents());
            self.commitment = Digest::new(hasher.finalize().into());

            self.objects.insert((id, version), object);
        }
    }

    /// The commitment to the state: `H(previous ‖ id ‖ version ‖ contents)` folded over every
    /// write in execution order.
    pub fn commitment(&self) -> Digest {
        self.commitment
    }
}

impl StateView for InMemoryStore {
    fn latest(&self, id: &ObjectId) -> Option<&Object> {
        let version = self.latest.get(id)?;
        self.objects.get(&(*id, *version))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crypto::Digest,
        effects::{AbortReason, ExecutionOutput},
        object::{Object, ObjectId, Version},
        store::{InMemoryStore, StateView},
    };

    #[test]
    fn apply_moves_writes_in_and_bumps_latest() {
        let mut store = InMemoryStore::default();
        let id = ObjectId::new(1);
        store.apply(ExecutionOutput::success(vec![Object::new(
            id,
            Version::new(1),
            vec![1],
        )]));
        store.apply(ExecutionOutput::success(vec![Object::new(
            id,
            Version::new(2),
            vec![2],
        )]));

        assert_eq!(store.latest(&id).unwrap().contents(), [2]);
        assert_eq!(store.get(&id, Version::new(1)).unwrap().contents(), [1]);
        assert!(store.get(&id, Version::new(3)).is_none());
        assert!(store.latest(&ObjectId::new(9)).is_none());
    }

    #[test]
    fn aborted_outputs_leave_the_store_untouched() {
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::aborted(AbortReason::ExplicitAbort));
        assert_eq!(store, InMemoryStore::default());
    }

    #[test]
    #[should_panic(expected = "versions must move forward")]
    fn rewriting_a_version_panics() {
        let mut store = InMemoryStore::default();
        let object = || Object::new(ObjectId::new(1), Version::new(1), vec![1]);
        store.apply(ExecutionOutput::success(vec![object()]));
        store.apply(ExecutionOutput::success(vec![object()]));
    }

    #[test]
    fn commitments_track_the_write_history() {
        let write = |version: u64| {
            ExecutionOutput::success(vec![Object::new(
                ObjectId::new(1),
                Version::new(version),
                vec![version as u8],
            )])
        };
        let (mut first, mut second) = (InMemoryStore::default(), InMemoryStore::default());
        assert_eq!(first.commitment(), Digest::default());

        first.apply(write(1));
        second.apply(write(1));
        assert_eq!(first.commitment(), second.commitment());

        first.apply(write(2));
        assert_ne!(first.commitment(), second.commitment());
        second.apply(write(2));
        assert_eq!(first.commitment(), second.commitment());
    }

    #[test]
    fn commitments_flip_on_any_differing_write() {
        let commit = |id: u64, version: u64, contents: Vec<u8>| {
            let mut store = InMemoryStore::default();
            let object = Object::new(ObjectId::new(id), Version::new(version), contents);
            store.apply(ExecutionOutput::success(vec![object]));
            store.commitment()
        };
        let reference = commit(1, 1, vec![7]);
        assert_ne!(reference, commit(2, 1, vec![7]));
        assert_ne!(reference, commit(1, 2, vec![7]));
        assert_ne!(reference, commit(1, 1, vec![8]));
    }

    #[test]
    fn commitments_are_order_sensitive() {
        let first = || Object::new(ObjectId::new(1), Version::new(1), vec![1]);
        let second = || Object::new(ObjectId::new(2), Version::new(1), vec![2]);

        let mut forward = InMemoryStore::default();
        forward.apply(ExecutionOutput::success(vec![first()]));
        forward.apply(ExecutionOutput::success(vec![second()]));
        let mut reverse = InMemoryStore::default();
        reverse.apply(ExecutionOutput::success(vec![second()]));
        reverse.apply(ExecutionOutput::success(vec![first()]));

        assert_ne!(forward.commitment(), reverse.commitment());
    }

    #[test]
    fn commitments_ignore_batch_boundaries() {
        let first = || Object::new(ObjectId::new(1), Version::new(1), vec![1]);
        let second = || Object::new(ObjectId::new(2), Version::new(1), vec![2]);

        let mut batched = InMemoryStore::default();
        batched.apply(ExecutionOutput::success(vec![first(), second()]));
        let mut split = InMemoryStore::default();
        split.apply(ExecutionOutput::success(vec![first()]));
        split.apply(ExecutionOutput::success(vec![second()]));

        assert_eq!(batched.commitment(), split.commitment());
    }

    // Commitments are persisted and compared across nodes, so the exact bytes matter: pin them so
    // any change to the hash, field order, or endianness fails loudly.
    #[test]
    fn commitments_are_stable_across_releases() {
        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(vec![
            Object::new(ObjectId::new(1), Version::new(1), vec![1, 2, 3]),
            Object::new(ObjectId::new(2), Version::new(7), vec![]),
        ]));

        let hex: String = store
            .commitment()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            hex,
            "bc3d1e39e90374f882e0c4ecc1b5f7f3ed4638ad3eeaa165fd86a768ec5224df"
        );
    }
}
