// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Engines running other implementations of the counter package over its published state.

#![cfg(feature = "sui")]

mod common;

use common::value;
use execution::{
    crypto::Digest,
    effects::ExecutionStatus,
    scheduler::SequentialScheduler,
    transaction::{AccessMode, Transaction},
};
use move_binary_format::{
    CompiledModule,
    file_format::{Bytecode, FunctionDefinition, Visibility},
};
use move_core_types::identifier::Identifier;
use move_engine::{COUNTER, MoveArgument, MoveCall, MoveEngine};
use sui_types::{SUI_FRAMEWORK_PACKAGE_ID, base_types::ObjectID};

const PLUS_ONE: &[u8] = include_bytes!("packages/plus_one/counter.mv");
const NEW_FIELD: &[u8] = include_bytes!("packages/new_field/counter.mv");
const NEW_SIGNATURE: &[u8] = include_bytes!("packages/new_signature/counter.mv");
const NEW_FUNCTION: &[u8] = include_bytes!("packages/new_function/counter.mv");

/// The counter package's engine, running `substitute` in its place if any.
fn engine(substitute: Option<&[u8]>) -> MoveEngine {
    let mut engine = MoveEngine::new(vec![vec![COUNTER.to_vec()]]);
    if let Some(modules) = substitute {
        let package = engine.packages()[0];
        engine.substitute(package, vec![modules.to_vec()]).unwrap();
    }
    engine
}

/// Creates a counter and increments it by 5, returning the genesis commitment, the counter's
/// value, and the final commitment.
fn run(engine: MoveEngine) -> (Digest, u64, Digest) {
    let package = engine.packages()[0];
    let call = |function, inputs, arguments| {
        let function = MoveEngine::function_id(package, "counter", function);
        let args = bcs::to_bytes(&MoveCall { arguments }).unwrap();
        Transaction::new(function, inputs, args)
    };
    let mut scheduler = SequentialScheduler::new(engine);
    let genesis = scheduler.store().commitment();
    let create = call("create", Vec::new(), Vec::new());
    let id = MoveEngine::fresh_id(&create);
    let step = MoveArgument::Pure(bcs::to_bytes(&5u64).unwrap());
    let inputs = vec![(id, AccessMode::ReadWrite)];
    let increment = call("increment", inputs, vec![MoveArgument::Object(0), step]);
    for outcome in scheduler.execute([create, increment]) {
        assert_eq!(outcome.status, ExecutionStatus::Success);
    }
    let store = scheduler.store();
    (genesis, value(store, &id), store.commitment())
}

#[test]
fn implementations_share_genesis_but_not_effects() {
    let (genesis, value, commitment) = run(engine(None));
    let (other_genesis, other_value, other_commitment) = run(engine(Some(PLUS_ONE)));
    assert_eq!(genesis, other_genesis);
    assert_eq!((value, other_value), (5, 6));
    assert_ne!(commitment, other_commitment);
}

#[test]
fn the_published_bytecode_substitutes_for_itself() {
    assert_eq!(run(engine(Some(COUNTER))), run(engine(None)));
}

#[test]
fn incompatible_substitutes_are_rejected() {
    let mut engine = engine(None);
    let package = engine.packages()[0];
    for modules in [
        vec![NEW_FIELD.to_vec()],
        vec![NEW_SIGNATURE.to_vec()],
        vec![NEW_FUNCTION.to_vec()],
        vec![vec![1, 2, 3]],
        Vec::new(),
    ] {
        assert!(engine.substitute(package, modules).is_err());
    }
    for id in [SUI_FRAMEWORK_PACKAGE_ID, ObjectID::from_single_byte(0x42)] {
        assert!(engine.substitute(id, vec![COUNTER.to_vec()]).is_err());
    }

    // Nor may a substitute drop a published entry function.
    let mut wider = MoveEngine::new(vec![vec![NEW_FUNCTION.to_vec()]]);
    let package = wider.packages()[0];
    assert!(wider.substitute(package, vec![COUNTER.to_vec()]).is_err());
}

#[test]
fn substitutes_are_verified_as_at_publication() {
    let mut engine = engine(None);
    let package = engine.packages()[0];
    // Calling a function the framework lacks passes every check a module meets alone.
    let unlinked = edited(|module| {
        let emit = module
            .function_handles
            .iter()
            .find(|handle| module.identifier_at(handle.name).as_str() == "emit");
        let name = emit.unwrap().name;
        module.identifiers[name.0 as usize] = Identifier::new("nowhere").unwrap();
    });
    let error = engine.substitute(package, vec![unlinked]).unwrap_err();
    assert!(error.to_string().contains("fails verification"), "{error}");

    // A public `init` satisfies Move's verifier but not Sui's.
    let public_init = edited(|module| definition(module, "init").visibility = Visibility::Public);
    let error = engine.substitute(package, vec![public_init]).unwrap_err();
    assert!(error.to_string().contains("Sui's checks"), "{error}");

    let underflow = edited(|module| {
        let code = &mut definition(module, "increment").code.as_mut().unwrap().code;
        *code = vec![Bytecode::Pop, Bytecode::Ret];
    });
    let error = engine.substitute(package, vec![underflow]).unwrap_err();
    assert!(error.to_string().contains("fails verification"), "{error}");
}

/// `COUNTER` with `edit` applied to its compiled module.
fn edited(edit: impl FnOnce(&mut CompiledModule)) -> Vec<u8> {
    let mut module = CompiledModule::deserialize_with_defaults(COUNTER).unwrap();
    edit(&mut module);
    let mut bytes = Vec::new();
    module
        .serialize_with_version(module.version, &mut bytes)
        .unwrap();
    bytes
}

fn definition<'m>(module: &'m mut CompiledModule, name: &str) -> &'m mut FunctionDefinition {
    let index = module.function_defs.iter().position(|definition| {
        let handle = module.function_handle_at(definition.function);
        module.identifier_at(handle.name).as_str() == name
    });
    &mut module.function_defs[index.unwrap()]
}
