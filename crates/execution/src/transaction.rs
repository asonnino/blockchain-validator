// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client transactions and their byte codec.

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{
    object::{ObjectId, Version},
    store::StateView,
};

/// Transactions larger than this fail to decode. Bounds the allocations a malformed length
/// prefix can request, since transaction bytes come from untrusted clients via consensus.
pub const MAX_TRANSACTION_SIZE: u64 = 1024 * 1024;

/// A fixed-size reference to the function a transaction invokes.
///
/// Resolution is the engine's job (eventually a digest of the fully qualified move-native entry
/// point, resolved through its module cache); the rest of the pipeline treats it as an opaque
/// 32-byte key.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
pub struct FunctionId([u8; 32]);

impl FunctionId {
    /// Const so engines can declare well-known functions as constants.
    pub const fn new(value: u64) -> Self {
        let value = value.to_be_bytes();
        let mut bytes = [0; 32];
        let mut i = 0;
        while i < 8 {
            bytes[24 + i] = value[i];
            i += 1;
        }
        Self(bytes)
    }
}

/// How a transaction accesses a declared input object.
///
/// Object ids are unforgeable, so a fresh object cannot collide with an existing one.
#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub enum AccessMode {
    /// Shared access to an existing object.
    ReadOnly,
    /// Exclusive access to a freshly created object; the id guaranteed unused.
    WriteOnly,
    /// Exclusive access to an existing object.
    ReadWrite,
}

/// A transaction as understood by the execution layer: a function call.
///
/// `inputs` declares upfront every object the call touches — each id exactly once, with its
/// access mode; the scheduler orders transactions by these. `args` are the pure arguments,
/// opaque to everything but the engine.
#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct Transaction {
    function: FunctionId,
    inputs: Vec<(ObjectId, AccessMode)>,
    args: Vec<u8>,
}

impl Transaction {
    pub fn new(function: FunctionId, inputs: Vec<(ObjectId, AccessMode)>, args: Vec<u8>) -> Self {
        Self {
            function,
            inputs,
            args,
        }
    }

    pub fn function(&self) -> FunctionId {
        self.function
    }

    pub fn inputs(&self) -> &[(ObjectId, AccessMode)] {
        &self.inputs
    }

    /// Encodes the transaction into the opaque bytes carried by consensus.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialization should not fail")
    }

    /// Decodes a transaction from consensus payload bytes.
    pub fn from_bytes(bytes: &[u8]) -> bincode::Result<Self> {
        // Same format as `bincode::serialize` (fixint, trailing bytes tolerated), plus the
        // size limit.
        bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .with_limit(MAX_TRANSACTION_SIZE)
            .deserialize_from(bytes)
    }

    /// Statically verifies the transaction, independently of any state.
    pub fn verify(&self) -> bool {
        // No duplicate ids in the inputs list.
        self.inputs
            .iter()
            .enumerate()
            .all(|(index, (id, _))| !self.inputs[..index].iter().any(|(prior, _)| prior == id))

        // Todo: other checks here.
    }

    /// The version of every object this transaction writes: one past the highest version among
    /// its existing inputs. Creations contribute nothing — `WriteOnly` ids are fresh by
    /// construction. Returns `None` if a `ReadOnly` or `ReadWrite` input does not exist.
    pub fn next_version<V: StateView>(&self, view: &V) -> Option<Version> {
        let mut highest = Version::ZERO;
        for (id, mode) in &self.inputs {
            match mode {
                AccessMode::ReadOnly | AccessMode::ReadWrite => {
                    highest = highest.max(view.latest(id)?.version());
                }
                AccessMode::WriteOnly => (),
            }
        }
        Some(highest.next())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        object::ObjectId,
        transaction::{AccessMode, FunctionId, Transaction},
    };

    #[test]
    fn transactions_roundtrip_through_bytes() {
        let transaction = Transaction::new(
            FunctionId::new(7),
            vec![
                (ObjectId::new(1), AccessMode::ReadOnly),
                (ObjectId::new(2), AccessMode::WriteOnly),
            ],
            vec![1, 2, 3],
        );
        let decoded = Transaction::from_bytes(&transaction.to_bytes()).unwrap();
        assert_eq!(decoded, transaction);
    }

    #[test]
    fn garbage_bytes_fail_to_decode() {
        assert!(Transaction::from_bytes(&[0xFF]).is_err());
    }

    #[test]
    fn oversized_transactions_fail_to_decode() {
        let args = vec![0; 2 * super::MAX_TRANSACTION_SIZE as usize];
        let transaction = Transaction::new(FunctionId::new(7), vec![], args);
        assert!(Transaction::from_bytes(&transaction.to_bytes()).is_err());
    }

    #[test]
    fn function_ids_encode_like_object_ids() {
        let function = bincode::serialize(&FunctionId::new(0xA1B2C3)).unwrap();
        let object = bincode::serialize(&ObjectId::new(0xA1B2C3)).unwrap();
        assert_eq!(function, object);
    }

    #[test]
    fn verify_rejects_duplicate_ids_across_modes() {
        let id = ObjectId::new(1);
        let transaction = Transaction::new(
            FunctionId::new(0),
            vec![(id, AccessMode::ReadOnly), (id, AccessMode::ReadWrite)],
            vec![],
        );
        assert!(!transaction.verify());
    }

    #[test]
    fn verify_accepts_unique_inputs() {
        let transaction = Transaction::new(
            FunctionId::new(0),
            vec![
                (ObjectId::new(1), AccessMode::ReadOnly),
                (ObjectId::new(2), AccessMode::WriteOnly),
                (ObjectId::new(3), AccessMode::ReadWrite),
            ],
            vec![],
        );
        assert!(transaction.verify());
    }
}
