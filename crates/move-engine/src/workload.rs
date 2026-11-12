// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The bundled counter package and its workload.

use execution::{
    object::ObjectId,
    transaction::{AccessMode, FunctionId, Transaction},
    workload::Workload,
};
use rand::{Rng, rngs::StdRng, seq::SliceRandom};
use sui_types::{SUI_CLOCK_OBJECT_ID, base_types::ObjectID};

use crate::{MoveArgument, MoveCall, MoveEngine};

/// The counter package's module (`packages/counter`), compiled. Rebuild it with `sui move build`
/// in that directory and copy `build/counter/bytecode_modules/counter.mv` next to the sources.
pub const COUNTER: &[u8] = include_bytes!("../packages/counter/counter.mv");

/// Calls into the counter package. Each authority's stream first creates a few counters. Then
/// 40% of its calls increment its own counters and 40% the shared ones, by random steps; 10%
/// stamp one of its counters with the clock's time, and the rest only read a shared counter. A
/// stream's transactions commit in submission order, so its counters exist before it uses them.
pub struct CounterWorkload {
    /// Shared by the package's `init` at genesis.
    shared: Vec<ObjectId>,
    owned: Vec<ObjectId>,
    create: Call,
    /// One per step, from 1 to `MAX_STEP`.
    increments: Vec<Call>,
    stamp: Call,
    read: Call,
}

/// A call into the counter package, resolved and encoded once, so each transaction only copies
/// its arguments.
struct Call {
    function: FunctionId,
    args: Vec<u8>,
}

impl Call {
    fn new(package: ObjectID, function: &str, arguments: Vec<MoveArgument>) -> Self {
        Self {
            function: MoveEngine::function_id(package, "counter", function),
            args: bcs::to_bytes(&MoveCall { arguments }).expect("calls serialize"),
        }
    }

    fn transaction(&self, inputs: Vec<(ObjectId, AccessMode)>, nonce: u64) -> Transaction {
        Transaction::new(self.function, inputs, self.args.clone()).with_nonce(nonce)
    }
}

impl CounterWorkload {
    const OWNED: u64 = 4;
    const MAX_STEP: u64 = 10;
    /// Nonce split: authority in the top 8 bits, so no two streams send the same transaction.
    const AUTHORITY_SHIFT: u32 = 56;

    /// A workload over the counter package, which must be the first package `engine` published.
    pub fn new(engine: &MoveEngine) -> Self {
        let package = engine.packages()[0];
        let shared: Vec<_> = engine
            .genesis_objects()
            .iter()
            .filter(|object| object.owner.is_shared())
            .filter(|object| {
                let address = object.type_().map(|ty| ObjectID::from(ty.address()));
                address == Some(package)
            })
            .map(|object| ObjectId::from_bytes(object.id().into_bytes()))
            .collect();
        assert!(
            !shared.is_empty(),
            "the first package must be the counter package"
        );
        let increments = (1..=Self::MAX_STEP)
            .map(|step| {
                let step = bcs::to_bytes(&step).expect("steps serialize");
                let arguments = vec![MoveArgument::Object(0), MoveArgument::Pure(step)];
                Call::new(package, "increment", arguments)
            })
            .collect();
        Self {
            shared,
            owned: Vec::with_capacity(Self::OWNED as usize),
            create: Call::new(package, "create", Vec::new()),
            increments,
            stamp: Call::new(
                package,
                "stamp",
                vec![MoveArgument::Object(0), MoveArgument::Object(1)],
            ),
            read: Call::new(package, "assert_incremented", vec![MoveArgument::Object(0)]),
        }
    }

    fn increment(&self, rng: &mut StdRng) -> &Call {
        self.increments.choose(rng).expect("steps start at one")
    }

    fn owned(&self, rng: &mut StdRng) -> ObjectId {
        *self
            .owned
            .choose(rng)
            .expect("the stream created its counters")
    }

    fn shared(&self, rng: &mut StdRng) -> ObjectId {
        *self.shared.choose(rng).expect("genesis shares counters")
    }
}

impl Workload for CounterWorkload {
    fn transaction(
        &mut self,
        namespace: u64,
        counter: u64,
        _: usize,
        rng: &mut StdRng,
    ) -> Transaction {
        let nonce = (namespace << Self::AUTHORITY_SHIFT) | counter;
        if counter < Self::OWNED {
            let create = self.create.transaction(Vec::new(), nonce);
            self.owned.push(MoveEngine::fresh_id(&create));
            return create;
        }
        let (call, inputs) = match rng.gen_range(0..10) {
            0..4 => (
                self.increment(rng),
                vec![(self.owned(rng), AccessMode::ReadWrite)],
            ),
            4..8 => (
                self.increment(rng),
                vec![(self.shared(rng), AccessMode::ReadWrite)],
            ),
            8 => {
                let clock = ObjectId::from_bytes(SUI_CLOCK_OBJECT_ID.into_bytes());
                let inputs = vec![
                    (self.owned(rng), AccessMode::ReadWrite),
                    (clock, AccessMode::ReadOnly),
                ];
                (&self.stamp, inputs)
            }
            _ => (&self.read, vec![(self.shared(rng), AccessMode::ReadOnly)]),
        };
        call.transaction(inputs, nonce)
    }
}

#[cfg(test)]
mod tests {
    use execution::{
        effects::ExecutionStatus, scheduler::SequentialScheduler, transaction::AccessMode,
        workload::Workload,
    };
    use rand::{SeedableRng, rngs::StdRng};

    use crate::{COUNTER, CounterWorkload, MoveEngine};

    /// Two authorities' streams, interleaved: every write succeeds, so streams neither repeat
    /// each other's transactions nor miss their own counters.
    #[test]
    fn interleaved_streams_succeed() {
        let engine = MoveEngine::new(vec![vec![COUNTER.to_vec()]]);
        let mut streams: Vec<_> = (0..2)
            .map(|namespace| {
                let rng = StdRng::seed_from_u64(namespace);
                (namespace, CounterWorkload::new(&engine), rng)
            })
            .collect();
        let mut scheduler = SequentialScheduler::new(engine);
        let mut reads = 0;
        for counter in 0..50 {
            for (namespace, workload, rng) in &mut streams {
                let transaction = workload.transaction(*namespace, counter, 0, rng);
                let read = matches!(transaction.inputs(), [(_, AccessMode::ReadOnly)]);
                let status = scheduler.execute([transaction]).next().unwrap().status;
                // A read aborts while its shared counter is still zero.
                if read {
                    reads += usize::from(status == ExecutionStatus::Success);
                } else {
                    assert_eq!(status, ExecutionStatus::Success);
                }
            }
        }
        assert!(reads > 0);
    }
}
