// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Engines running other implementations of the counter package over its published state.

#![cfg(feature = "sui")]

mod common;

use common::{call, failure, pure, shared_counters, value};
use execution::{
    crypto::Digest,
    effects::ExecutionStatus,
    object::ObjectId,
    scheduler::SequentialScheduler,
    transaction::{AccessMode, Transaction},
};
use move_binary_format::{
    CompiledModule,
    file_format::{Bytecode, FunctionDefinition, Visibility},
};
use move_core_types::{identifier::Identifier, vm_status::StatusCode};
use move_engine::{COUNTER, MoveArgument, MoveEngine, MoveFailure};
use sui_types::{
    SUI_FRAMEWORK_PACKAGE_ID, base_types::ObjectID, execution_status::ExecutionErrorKind as Kind,
};

const PLUS_ONE: &[u8] = include_bytes!("packages/plus_one/counter.mv");
const ABORT_CODE: &[u8] = include_bytes!("packages/abort_code/counter.mv");
const ENDLESS: &[u8] = include_bytes!("packages/endless/counter.mv");
const NEW_FIELD: &[u8] = include_bytes!("packages/new_field/counter.mv");
const NEW_SIGNATURE: &[u8] = include_bytes!("packages/new_signature/counter.mv");
const NEW_FUNCTION: &[u8] = include_bytes!("packages/new_function/counter.mv");

/// The counter package's engine, running `substitute` in its place if any.
fn engine(substitute: Option<&[u8]>) -> MoveEngine {
    let mut engine = MoveEngine::counter();
    if let Some(modules) = substitute {
        let package = engine.packages()[0];
        engine.substitute(package, vec![modules.to_vec()]).unwrap();
    }
    engine
}

fn create(package: ObjectID) -> Transaction {
    call(package, "create", Vec::new(), Vec::new())
}

/// Increments counter `id` by 5.
fn increment(package: ObjectID, id: ObjectId) -> Transaction {
    let inputs = vec![(id, AccessMode::ReadWrite)];
    call(
        package,
        "increment",
        inputs,
        vec![MoveArgument::Object(0), pure(5)],
    )
}

/// Creates a counter and increments it, returning the genesis commitment, the counter's value,
/// the final commitment, and the increment's gas.
fn run(engine: MoveEngine) -> (Digest, u64, Digest, u64) {
    let package = engine.packages()[0];
    let mut scheduler = SequentialScheduler::new(engine);
    let genesis = scheduler.store().commitment();
    let create = create(package);
    let id = MoveEngine::fresh_id(&create);
    let outcomes: Vec<_> = scheduler
        .execute([create, increment(package, id)])
        .collect();
    for outcome in &outcomes {
        assert_eq!(outcome.status, ExecutionStatus::Success);
    }
    let store = scheduler.store();
    let gas = outcomes[1].gas_used;
    (genesis, value(store, &id), store.commitment(), gas)
}

#[test]
fn implementations_share_genesis_but_not_effects() {
    let (genesis, value, commitment, gas) = run(engine(None));
    let (other_genesis, other_value, other_commitment, other_gas) = run(engine(Some(PLUS_ONE)));
    assert_eq!(genesis, other_genesis);
    assert_eq!((value, other_value), (5, 6));
    assert_ne!(commitment, other_commitment);
    // `plus_one` adds through a helper, which gas tells apart even for calls this cheap.
    assert!(gas < other_gas, "{gas} against {other_gas}");
}

#[test]
fn the_published_bytecode_substitutes_for_itself() {
    assert_eq!(run(engine(Some(COUNTER))), run(engine(None)));
}

/// Checks that a shared counter, still zero, was incremented.
fn check(engine: MoveEngine) -> ExecutionStatus {
    let (package, id) = (engine.packages()[0], shared_counters(&engine)[0]);
    let inputs = vec![(id, AccessMode::ReadOnly)];
    let check = call(
        package,
        "assert_incremented",
        inputs,
        vec![MoveArgument::Object(0)],
    );
    let mut scheduler = SequentialScheduler::new(engine);
    let mut outcomes = scheduler.execute([check]);
    outcomes.next().unwrap().status
}

#[test]
fn implementations_failing_differently_report_different_failures() {
    let (status, other) = (check(engine(None)), check(engine(Some(ABORT_CODE))));
    assert_ne!(status, other);
    let abort = |status| match failure(status).unwrap().status {
        Kind::MoveAbort(location, code) => (location.module, code),
        status => panic!("not a Move abort: {status:?}"),
    };
    let ((module, code), (other_module, other_code)) = (abort(status), abort(other));
    assert_eq!(module, other_module);
    assert_eq!((code, other_code), (7, 8));
}

#[test]
fn endless_calls_run_out_of_gas_without_writing() {
    // The default budget takes seconds to exhaust in debug builds.
    let run_out = |budget| {
        let engine = engine(Some(ENDLESS)).with_gas_budget(budget);
        let package = engine.packages()[0];
        let mut scheduler = SequentialScheduler::new(engine);
        let create = create(package);
        let id = MoveEngine::fresh_id(&create);
        let created = scheduler.execute([create]).next().unwrap();
        assert_eq!(created.status, ExecutionStatus::Success);
        let commitment = scheduler.store().commitment();
        let outcome = scheduler.execute([increment(package, id)]).next().unwrap();
        assert_eq!(scheduler.store().commitment(), commitment);
        let out_of_gas = MoveFailure {
            status: Kind::InsufficientGas,
            vm_status: Some(StatusCode::OUT_OF_GAS),
        };
        assert_eq!(failure(outcome.status), Some(out_of_gas));
        outcome.gas_used
    };
    assert!(run_out(10_000) < run_out(100_000));
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
