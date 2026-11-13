// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The counter package through the scheduler.

#![cfg(feature = "sui")]

mod common;

use common::{call, clock, failure, pure, shared_counters, value};
use execution::{
    effects::{AbortReason, ExecutionStatus},
    engine::CommitInfo,
    object::ObjectId,
    scheduler::SequentialScheduler,
    store::StateView,
    transaction::{AccessMode, Transaction},
};
use move_core_types::{ident_str, language_storage::ModuleId, vm_status::StatusCode};
use move_engine::{
    MoveArgument::{self, Object},
    MoveCall, MoveEngine, MoveFailure,
};
use sui_types::{
    SUI_FRAMEWORK_ADDRESS, SUI_FRAMEWORK_PACKAGE_ID,
    base_types::ObjectID,
    execution_status::{CommandArgumentError, ExecutionErrorKind as Kind, MoveLocationOpt},
};

fn scheduler() -> (SequentialScheduler<MoveEngine>, ObjectID) {
    let engine = MoveEngine::counter();
    let package = engine.packages()[0];
    (SequentialScheduler::new(engine), package)
}

fn counter_module(package: ObjectID) -> ModuleId {
    ModuleId::new(package.into(), ident_str!("counter").to_owned())
}

fn status(
    scheduler: &mut SequentialScheduler<MoveEngine>,
    transaction: Transaction,
) -> ExecutionStatus {
    let mut outcomes = scheduler.execute([transaction]);
    outcomes.next().unwrap().status
}

/// Creates an owned counter, distinct per `nonce`.
fn create(
    scheduler: &mut SequentialScheduler<MoveEngine>,
    package: ObjectID,
    nonce: u64,
) -> ObjectId {
    let create = call(package, "create", Vec::new(), Vec::new()).with_nonce(nonce);
    let id = MoveEngine::fresh_id(&create);
    assert_eq!(status(scheduler, create), ExecutionStatus::Success);
    id
}

#[test]
fn counters_are_created_incremented_and_destroyed() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    let created_at = scheduler.store().latest(&id).unwrap().version();
    assert_eq!(value(scheduler.store(), &id), 0);

    let inputs = || vec![(id, AccessMode::ReadWrite)];
    let increment = || call(package, "increment", inputs(), vec![Object(0), pure(5)]);
    let outcome = scheduler.execute([increment()]).next().unwrap();
    assert_eq!(outcome.status, ExecutionStatus::Success);
    assert_eq!(outcome.events.len(), 1);
    assert!(outcome.gas_used > 0);
    assert_eq!(value(scheduler.store(), &id), 5);
    assert!(scheduler.store().latest(&id).unwrap().version() > created_at);

    let destroy = call(package, "destroy", inputs(), vec![Object(0)]);
    assert_eq!(status(&mut scheduler, destroy), ExecutionStatus::Success);
    assert!(scheduler.store().latest(&id).is_none());
    let missing = ExecutionStatus::Aborted(AbortReason::MissingRead);
    assert_eq!(status(&mut scheduler, increment()), missing);
}

#[test]
fn move_aborts_report_their_code_and_write_nothing() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    let version = scheduler.store().latest(&id).unwrap().version();

    let inputs = vec![(id, AccessMode::ReadWrite)];
    let check = call(package, "assert_incremented", inputs, vec![Object(0)]);
    let outcome = scheduler.execute([check]).next().unwrap();
    assert!(outcome.gas_used > 0);
    let failure = failure(outcome.status).unwrap();
    let Kind::MoveAbort(location, 7) = failure.status else {
        panic!("not abort code 7: {failure:?}");
    };
    assert_eq!(location.module, counter_module(package));
    assert_eq!(
        location.function_name.as_deref(),
        Some("assert_incremented")
    );
    assert_eq!(failure.vm_status, Some(StatusCode::ABORTED));
    assert_eq!(scheduler.store().latest(&id).unwrap().version(), version);
}

#[test]
fn framework_aborts_report_the_framework_module() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    let inputs = vec![(id, AccessMode::ReadWrite)];
    let retrieve = call(package, "retrieve", inputs, vec![Object(0)]);
    let failure = failure(status(&mut scheduler, retrieve)).unwrap();
    // `dynamic_field::remove` aborts with `EFieldDoesNotExist`.
    let Kind::MoveAbort(location, 1) = failure.status else {
        panic!("not abort code 1: {failure:?}");
    };
    let module = ModuleId::new(
        SUI_FRAMEWORK_ADDRESS,
        ident_str!("dynamic_field").to_owned(),
    );
    assert_eq!(location.module, module);
}

#[test]
fn arithmetic_errors_report_their_kind() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    let increment = |by| {
        let inputs = vec![(id, AccessMode::ReadWrite)];
        call(package, "increment", inputs, vec![Object(0), pure(by)])
    };
    assert_eq!(
        status(&mut scheduler, increment(1)),
        ExecutionStatus::Success
    );
    let failure = failure(status(&mut scheduler, increment(u64::MAX))).unwrap();
    let Kind::MovePrimitiveRuntimeError(MoveLocationOpt(Some(location))) = failure.status else {
        panic!("not a located primitive runtime error: {failure:?}");
    };
    assert_eq!(location.module, counter_module(package));
    assert_eq!(location.function_name.as_deref(), Some("increment"));
    assert_eq!(failure.vm_status, Some(StatusCode::ARITHMETIC_ERROR));
    assert_eq!(value(scheduler.store(), &id), 1);
}

#[test]
fn malformed_calls_fail_without_writing() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    let commitment = scheduler.store().commitment();

    let owned = || vec![(id, AccessMode::ReadWrite)];
    let package_id = ObjectId::from_bytes(package.into_bytes());
    let undecodable = Transaction::new(
        MoveEngine::function_id(package, "counter", "increment"),
        owned(),
        vec![0xff],
    );
    let fresh = vec![(ObjectId::from_bytes([7; 32]), AccessMode::WriteOnly)];
    let increment = |inputs, arguments| call(package, "increment", inputs, arguments);
    let rejections = [
        undecodable,
        call(package, "create", fresh, Vec::new()),
        increment(owned(), vec![Object(1), pure(1)]),
        // Sui writes every owned input, so it cannot be declared read-only.
        increment(vec![(id, AccessMode::ReadOnly)], vec![Object(0), pure(1)]),
        increment(
            vec![(package_id, AccessMode::ReadWrite)],
            vec![Object(0), pure(1)],
        ),
        // Only commits advance the clock.
        call(
            package,
            "stamp",
            vec![
                (id, AccessMode::ReadWrite),
                (clock(), AccessMode::ReadWrite),
            ],
            vec![Object(0), Object(1)],
        ),
    ];
    for transaction in rejections {
        let outcome = scheduler.execute([transaction]).next().unwrap();
        assert_eq!((failure(outcome.status), outcome.gas_used), (None, 0));
    }
    // Sui decodes pure arguments itself.
    let malformed = increment(owned(), vec![Object(0), MoveArgument::Pure(vec![1, 2, 3])]);
    let failure = failure(status(&mut scheduler, malformed)).unwrap();
    let invalid = Kind::CommandArgumentError {
        arg_idx: 1,
        kind: CommandArgumentError::InvalidBCSBytes,
    };
    let invalid = MoveFailure {
        status: invalid,
        vm_status: None,
    };
    assert_eq!(failure, invalid);
    assert_eq!(scheduler.store().commitment(), commitment);
}

#[test]
fn shared_counters_honour_read_only_access() {
    let engine = MoveEngine::counter();
    let (package, id) = (engine.packages()[0], shared_counters(&engine)[0]);
    let mut scheduler = SequentialScheduler::new(engine);
    let write = vec![(id, AccessMode::ReadWrite)];
    let increment = call(package, "increment", write, vec![Object(0), pure(1)]);
    assert_eq!(status(&mut scheduler, increment), ExecutionStatus::Success);
    let version = scheduler.store().latest(&id).unwrap().version();

    let read = || vec![(id, AccessMode::ReadOnly)];
    let increment = call(package, "increment", read(), vec![Object(0), pure(1)]);
    let failure = failure(status(&mut scheduler, increment)).unwrap();
    let immutable = Kind::CommandArgumentError {
        arg_idx: 0,
        kind: CommandArgumentError::InvalidObjectByMutRef,
    };
    let immutable = MoveFailure {
        status: immutable,
        vm_status: None,
    };
    assert_eq!(failure, immutable);
    let check = call(package, "assert_incremented", read(), vec![Object(0)]);
    assert_eq!(status(&mut scheduler, check), ExecutionStatus::Success);
    assert_eq!(scheduler.store().latest(&id).unwrap().version(), version);
}

#[test]
fn stashed_counters_come_back_through_dynamic_fields() {
    let (mut scheduler, package) = scheduler();
    let parent = create(&mut scheduler, package, 0);
    let child = create(&mut scheduler, package, 1);
    let inputs = vec![(child, AccessMode::ReadWrite)];
    let increment = call(package, "increment", inputs, vec![Object(0), pure(3)]);
    assert_eq!(status(&mut scheduler, increment), ExecutionStatus::Success);
    let version = scheduler.store().latest(&child).unwrap().version();

    let both = || {
        vec![
            (parent, AccessMode::ReadWrite),
            (child, AccessMode::ReadWrite),
        ]
    };
    let stash = || call(package, "stash", both(), vec![Object(0), Object(1)]);
    assert_eq!(status(&mut scheduler, stash()), ExecutionStatus::Success);
    assert!(scheduler.store().latest(&child).is_none());

    let inputs = vec![(parent, AccessMode::ReadWrite)];
    let retrieve = call(package, "retrieve", inputs, vec![Object(0)]);
    assert_eq!(status(&mut scheduler, retrieve), ExecutionStatus::Success);
    assert!(scheduler.store().latest(&child).unwrap().version() > version);
    assert_eq!(value(scheduler.store(), &child), 3);

    // The field's id derives from the parent and key, so stashing again re-creates it.
    assert_eq!(status(&mut scheduler, stash()), ExecutionStatus::Success);
}

#[test]
fn replayed_creations_abort() {
    let (mut scheduler, package) = scheduler();
    let create = || call(package, "create", Vec::new(), Vec::new());
    let created = scheduler.execute([create()]).next().unwrap();
    assert_eq!(created.status, ExecutionStatus::Success);
    // The replay runs in full before aborting, so it uses the same gas.
    let replayed = scheduler.execute([create()]).next().unwrap();
    let in_use = ExecutionStatus::Aborted(AbortReason::IdInUse);
    assert_eq!(
        (replayed.status, replayed.gas_used),
        (in_use, created.gas_used)
    );
    // A fresh nonce makes the same call a new transaction.
    assert_eq!(
        status(&mut scheduler, create().with_nonce(1)),
        ExecutionStatus::Success
    );
}

#[test]
fn replayed_creations_abort_when_wrapped_at_birth() {
    let (mut scheduler, package) = scheduler();
    let parent = create(&mut scheduler, package, 0);
    let inputs = || vec![(parent, AccessMode::ReadWrite)];
    let stash = || call(package, "stash_new", inputs(), vec![Object(0)]);
    let fresh = MoveEngine::fresh_id(&stash());
    assert_eq!(status(&mut scheduler, stash()), ExecutionStatus::Success);
    assert!(scheduler.store().latest(&fresh).is_none());

    // Retrieving frees the key, so only the fresh id's use stops a replay from minting it again.
    let retrieve = call(package, "retrieve", inputs(), vec![Object(0)]);
    assert_eq!(status(&mut scheduler, retrieve), ExecutionStatus::Success);
    assert!(scheduler.store().latest(&fresh).is_some());
    let in_use = ExecutionStatus::Aborted(AbortReason::IdInUse);
    assert_eq!(status(&mut scheduler, stash()), in_use);
}

#[test]
fn the_clock_follows_commits() {
    let (mut scheduler, package) = scheduler();
    let id = create(&mut scheduler, package, 0);
    // The clock, like a counter, keeps its value right after its `UID`.
    assert_eq!(value(scheduler.store(), &clock()), 0);
    let inputs = vec![(id, AccessMode::ReadWrite), (clock(), AccessMode::ReadOnly)];
    let stamp = call(package, "stamp", inputs, vec![Object(0), Object(1)]);

    scheduler.begin_commit(&CommitInfo {
        timestamp_ms: 5_000,
    });
    assert_eq!(status(&mut scheduler, stamp), ExecutionStatus::Success);
    assert_eq!(value(scheduler.store(), &id), 5_000);

    scheduler.begin_commit(&CommitInfo {
        timestamp_ms: 6_000,
    });
    assert_eq!(value(scheduler.store(), &clock()), 6_000);
}

#[test]
fn transactions_read_the_clock_without_taking_it() {
    let (mut scheduler, _) = scheduler();
    scheduler.begin_commit(&CommitInfo {
        timestamp_ms: 5_000,
    });
    // Sui's `clock::borrow` reads the clock implicitly; without its pinned version, Sui's adapter
    // fails the transaction and panics in debug builds.
    let function = MoveEngine::function_id(SUI_FRAMEWORK_PACKAGE_ID, "clock", "borrow");
    let call = MoveCall {
        arguments: Vec::new(),
    };
    let borrow = Transaction::new(function, Vec::new(), bcs::to_bytes(&call).unwrap());
    assert_eq!(status(&mut scheduler, borrow), ExecutionStatus::Success);
}

#[test]
fn unknown_functions_abort() {
    let (mut scheduler, package) = scheduler();
    let missing = call(package, "missing", Vec::new(), Vec::new());
    let unknown = ExecutionStatus::Aborted(AbortReason::UnknownFunction);
    assert_eq!(status(&mut scheduler, missing), unknown);
}

#[test]
fn every_validator_starts_from_the_same_genesis() {
    let commitment = || scheduler().0.store().commitment();
    assert_eq!(commitment(), commitment());
}
