// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// A counter exercising pure and object arguments, owned and shared objects, writes, events,
/// aborts, deletions, and dynamic fields in the move-engine tests. The engine runs one call per
/// transaction, so objects go to the sender rather than back to a caller.
#[allow(lint(self_transfer))]
module counter::counter;

use sui::{dynamic_field, event};

public struct Counter has key, store {
    id: UID,
    value: u64,
}

public struct Incremented has copy, drop {
    value: u64,
}

/// Creates a counter owned by the sender.
public fun create(ctx: &mut TxContext) {
    transfer::transfer(Counter { id: object::new(ctx), value: 0 }, ctx.sender())
}

public fun create_shared(ctx: &mut TxContext) {
    transfer::share_object(Counter { id: object::new(ctx), value: 0 })
}

public fun increment(counter: &mut Counter, by: u64) {
    counter.value = counter.value + by;
    event::emit(Incremented { value: counter.value })
}

/// Aborts with code 7 while the counter is zero.
public fun assert_incremented(counter: &Counter) {
    assert!(counter.value > 0, 7)
}

public fun destroy(counter: Counter) {
    let Counter { id, value: _ } = counter;
    id.delete()
}

/// Wraps `counter` in a dynamic field of `parent`, under a fixed key.
public fun stash(parent: &mut Counter, counter: Counter) {
    dynamic_field::add(&mut parent.id, 0u8, counter)
}

/// Stashes a fresh counter, wrapping it as it is created.
public fun stash_new(parent: &mut Counter, ctx: &mut TxContext) {
    stash(parent, Counter { id: object::new(ctx), value: 0 })
}

/// Unwraps the counter `stash` put in `parent`, back to the sender.
public fun retrieve(parent: &mut Counter, ctx: &TxContext) {
    let counter: Counter = dynamic_field::remove(&mut parent.id, 0u8);
    transfer::transfer(counter, ctx.sender())
}
