// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Reads of the counter package's objects, shared by the test binaries.

// Each test binary compiles its own copy of this module and uses a subset of it.
#![allow(dead_code)]

use execution::{
    effects::{AbortReason, ExecutionStatus},
    engine::ExecutionEngine,
    object::ObjectId,
    store::StateView,
    transaction::{AccessMode, Transaction},
};
use move_engine::{MoveArgument, MoveCall, MoveEngine, MoveFailure};
use sui_types::{SUI_CLOCK_OBJECT_ID, base_types::ObjectID, object::Object};

/// A call to the counter module's `function`.
pub fn call(
    package: ObjectID,
    function: &str,
    inputs: Vec<(ObjectId, AccessMode)>,
    arguments: Vec<MoveArgument>,
) -> Transaction {
    let function = MoveEngine::function_id(package, "counter", function);
    let call = MoveCall { arguments };
    Transaction::new(function, inputs, bcs::to_bytes(&call).unwrap())
}

pub fn pure(value: u64) -> MoveArgument {
    MoveArgument::Pure(bcs::to_bytes(&value).unwrap())
}

/// The counters the package's `init` shared at genesis.
pub fn shared_counters(engine: &MoveEngine) -> Vec<ObjectId> {
    let package = engine.packages()[0];
    engine
        .genesis()
        .iter()
        .map(|object| bcs::from_bytes::<Object>(object.contents()).unwrap())
        .filter(|object| object.owner.is_shared())
        .filter(|object| {
            let address = object.type_().map(|ty| ObjectID::from(ty.address()));
            address == Some(package)
        })
        .map(|object| ObjectId::from_bytes(object.id().into_bytes()))
        .collect()
}

/// Sui's clock.
pub fn clock() -> ObjectId {
    ObjectId::from_bytes(SUI_CLOCK_OBJECT_ID.into_bytes())
}

/// What Sui reported about a failed call.
pub fn failure(status: ExecutionStatus) -> Option<MoveFailure> {
    let ExecutionStatus::Aborted(AbortReason::ExecutionFailure(detail)) = &status else {
        panic!("not an execution failure: {status:?}");
    };
    MoveFailure::decode(detail)
}

/// The counter's value: the `u64` after its 32-byte `UID` in the Move object's contents.
pub fn value<V: StateView>(view: &V, id: &ObjectId) -> u64 {
    let object: Object = bcs::from_bytes(view.latest(id).unwrap().contents()).unwrap();
    let contents = object.data.try_as_move().unwrap().contents();
    u64::from_le_bytes(contents[32..40].try_into().unwrap())
}
