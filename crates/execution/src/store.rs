// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The versioned in-memory object store.

use std::collections::BTreeMap;

use crate::{
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
            self.objects.insert((id, version), object);
        }
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
}
