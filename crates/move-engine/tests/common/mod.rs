// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Reads of the counter package's objects, shared by the test binaries.

use execution::{engine::ExecutionEngine, object::ObjectId, store::StateView};
use move_engine::MoveEngine;
use sui_types::object::Object;

/// The counters the package's `init` shared at genesis.
pub fn shared_counters(engine: &MoveEngine) -> Vec<ObjectId> {
    engine
        .genesis()
        .iter()
        .map(|object| bcs::from_bytes::<Object>(object.contents()).unwrap())
        .filter(|object| object.owner.is_shared())
        .map(|object| ObjectId::from_bytes(object.id().into_bytes()))
        .collect()
}

/// The counter's value: the `u64` after its 32-byte `UID` in the Move object's contents.
pub fn value<V: StateView>(view: &V, id: &ObjectId) -> u64 {
    let object: Object = bcs::from_bytes(view.latest(id).unwrap().contents()).unwrap();
    let contents = object.data.try_as_move().unwrap().contents();
    u64::from_le_bytes(contents[32..40].try_into().unwrap())
}
