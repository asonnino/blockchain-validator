// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The digests votes carry, and the seeded hash behind every deterministic choice.

use digest::Digest as _;
use execution::{
    crypto::Digest,
    effects::{AbortReason, ExecutionOutput, ExecutionStatus},
    store::StateView,
    transaction::Transaction,
};
use serde::{Deserialize, Serialize};

type Hasher = blake2::Blake2b<digest::consts::U32>;

/// The digest of a transaction's result, which versions must agree on: its status, failure detail
/// included, then its written objects, deletions, and events, in the order the output lists them.
/// Gas used is not compared.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
pub struct ResultDigest(Digest);

impl ResultDigest {
    pub fn new(output: &ExecutionOutput) -> Self {
        let mut hasher = Hasher::new();
        match output.status() {
            ExecutionStatus::Success => hasher.update([0]),
            ExecutionStatus::Aborted(reason) => {
                hasher.update([1]);
                match reason {
                    AbortReason::ExplicitAbort => hasher.update([0]),
                    AbortReason::MissingRead => hasher.update([1]),
                    AbortReason::IdInUse => hasher.update([2]),
                    AbortReason::UnknownFunction => hasher.update([3]),
                    AbortReason::InvalidTransaction => hasher.update([4]),
                    AbortReason::ExecutionFailure(detail) => {
                        hasher.update([5]);
                        update_bytes(&mut hasher, detail.contents());
                    }
                }
            }
        }
        update_len(&mut hasher, output.writes().len());
        for object in output.writes() {
            hasher.update(object.id().as_bytes());
            hasher.update(object.version().as_u64().to_be_bytes());
            update_bytes(&mut hasher, object.contents());
        }
        update_len(&mut hasher, output.deletes().len());
        for (id, version) in output.deletes() {
            hasher.update(id.as_bytes());
            hasher.update(version.as_u64().to_be_bytes());
        }
        update_len(&mut hasher, output.events().len());
        for event in output.events() {
            update_bytes(&mut hasher, event.contents());
        }
        Self(Digest::new(hasher.finalize().into()))
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(byte: u8) -> Self {
        Self(Digest::new([byte; 32]))
    }
}

/// The digest of a transaction's declared inputs as it finds them in `view`: each input absent,
/// deleted at a version, or live at a version with its contents.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
pub struct InputDigest(Digest);

impl InputDigest {
    pub fn new<V: StateView>(view: &V, transaction: &Transaction) -> Self {
        let mut hasher = Hasher::new();
        for (id, _) in transaction.inputs() {
            hasher.update(id.as_bytes());
            match (view.latest_version(id), view.latest(id)) {
                (None, _) => hasher.update([0]),
                (Some(version), None) => {
                    hasher.update([1]);
                    hasher.update(version.as_u64().to_be_bytes());
                }
                (Some(version), Some(object)) => {
                    hasher.update([2]);
                    hasher.update(version.as_u64().to_be_bytes());
                    update_bytes(&mut hasher, object.contents());
                }
            }
        }
        Self(Digest::new(hasher.finalize().into()))
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(byte: u8) -> Self {
        Self(Digest::new([byte; 32]))
    }
}

/// A uniform `u64` drawn from `seed` and `parts`.
pub(crate) fn seeded(seed: u64, parts: &[&[u8]]) -> u64 {
    let mut hasher = Hasher::new();
    hasher.update(seed.to_be_bytes());
    for part in parts {
        update_bytes(&mut hasher, part);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    u64::from_be_bytes(digest[..8].try_into().expect("digests hold 8 bytes"))
}

fn update_len(hasher: &mut Hasher, len: usize) {
    hasher.update((len as u64).to_be_bytes());
}

fn update_bytes(hasher: &mut Hasher, bytes: &[u8]) {
    update_len(hasher, bytes.len());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use execution::{
        effects::{AbortReason, Event, ExecutionOutput, FailureDetail},
        object::{Object, ObjectId, Version},
        store::InMemoryStore,
        transaction::{AccessMode, FunctionId, Transaction},
    };

    use super::{InputDigest, ResultDigest};

    fn write(contents: &[u8]) -> ExecutionOutput {
        let object = Object::new(ObjectId::new(1), Version::new(2), contents.to_vec());
        ExecutionOutput::success(vec![object])
    }

    #[test]
    fn results_differ_in_every_compared_part() {
        let failure = |bytes: &[u8]| {
            let detail = FailureDetail::new(bytes.to_vec());
            ExecutionOutput::aborted(AbortReason::ExecutionFailure(detail))
        };
        let delete = vec![(ObjectId::new(3), Version::new(2))];
        let other = Object::new(ObjectId::new(2), Version::new(2), b"a".to_vec());
        let digests = [
            ResultDigest::new(&write(b"a")),
            ResultDigest::new(&write(b"b")),
            ResultDigest::new(&ExecutionOutput::success(vec![other])),
            ResultDigest::new(&write(b"a").with_deletes(delete)),
            ResultDigest::new(&write(b"a").with_events(vec![Event::new(vec![1])])),
            ResultDigest::new(&ExecutionOutput::aborted(AbortReason::ExplicitAbort)),
            ResultDigest::new(&ExecutionOutput::aborted(AbortReason::MissingRead)),
            ResultDigest::new(&ExecutionOutput::aborted(AbortReason::IdInUse)),
            ResultDigest::new(&ExecutionOutput::aborted(AbortReason::UnknownFunction)),
            ResultDigest::new(&ExecutionOutput::aborted(AbortReason::InvalidTransaction)),
            ResultDigest::new(&failure(&[1])),
            ResultDigest::new(&failure(&[2])),
        ];
        for (index, digest) in digests.iter().enumerate() {
            assert!(!digests[..index].contains(digest), "digest {index} repeats");
        }
        assert_eq!(ResultDigest::new(&write(b"a")), digests[0]);
        assert_eq!(ResultDigest::new(&write(b"a").with_gas_used(7)), digests[0]);
    }

    #[test]
    fn inputs_differ_in_presence_version_and_contents() {
        let id = ObjectId::new(1);
        let read = Transaction::new(FunctionId::new(0), vec![(id, AccessMode::ReadOnly)], vec![]);
        let mut store = InMemoryStore::default();
        let mut digests = vec![InputDigest::new(&store, &read)];
        store.apply(write(b"a"));
        digests.push(InputDigest::new(&store, &read));
        let object = Object::new(id, Version::new(3), b"a".to_vec());
        store.apply(ExecutionOutput::success(vec![object]));
        digests.push(InputDigest::new(&store, &read));
        let object = Object::new(id, Version::new(4), b"b".to_vec());
        store.apply(ExecutionOutput::success(vec![object]));
        digests.push(InputDigest::new(&store, &read));
        let deletes = vec![(id, Version::new(5))];
        store.apply(ExecutionOutput::success(Vec::new()).with_deletes(deletes));
        digests.push(InputDigest::new(&store, &read));
        for (index, digest) in digests.iter().enumerate() {
            assert!(!digests[..index].contains(digest), "digest {index} repeats");
        }
        // A loser's state: the same object at the same version, with other contents.
        let mut other = InMemoryStore::default();
        other.apply(write(b"b"));
        let mut store = InMemoryStore::default();
        store.apply(write(b"a"));
        assert_ne!(
            InputDigest::new(&other, &read),
            InputDigest::new(&store, &read)
        );
    }
}
