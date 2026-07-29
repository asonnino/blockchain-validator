// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The consensus payload envelope and its byte codec.

use bincode::Options;
use execution::transaction::Transaction;
use serde::{Deserialize, Serialize};

/// Payloads larger than this fail to decode. Bounds the allocations a malformed length prefix
/// can request, since payload bytes come from untrusted clients via consensus.
pub const MAX_PAYLOAD_SIZE: u64 = 1024 * 1024;

/// What every consensus payload carries: the submission timestamp, then the payload proper.
///
/// The layout is load-bearing: `timestamp_ms` must stay the first field of the outer struct so
/// that fixint bincode places it in the payload's first 8 little-endian bytes, where dag's
/// `Transaction::extract_timestamp` reads the submission time — mysticeti's commit-latency
/// metric works with no upstream changes. Nesting the other way (enum outside) would put the
/// variant tag there instead.
#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct Envelope {
    timestamp_ms: u64,
    payload: Payload,
}

/// The payload proper, dispatched where the sub-dag consumer decodes it.
#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub enum Payload {
    /// A client transaction bound for the execution engine.
    Execute(Transaction),
}

impl Envelope {
    pub fn new(timestamp_ms: u64, payload: Payload) -> Self {
        Self {
            timestamp_ms,
            payload,
        }
    }

    pub fn into_payload(self) -> Payload {
        self.payload
    }

    /// Encodes the envelope into the opaque bytes carried by consensus.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialization should not fail")
    }

    /// Decodes an envelope from consensus payload bytes.
    pub fn from_bytes(bytes: &[u8]) -> bincode::Result<Self> {
        // Same format as `bincode::serialize` (fixint, trailing bytes tolerated), plus the
        // size limit; the envelope's few bytes of overhead eat into the transaction budget.
        bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .with_limit(MAX_PAYLOAD_SIZE)
            .deserialize_from(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dag::block::transaction::Transaction as ConsensusTransaction;
    use execution::transaction::{FunctionId, Transaction};

    use crate::envelope::{Envelope, Payload};

    fn envelope(timestamp_ms: u64, args: Vec<u8>) -> Envelope {
        let transaction = Transaction::new(FunctionId::new(7), vec![], args);
        Envelope::new(timestamp_ms, Payload::Execute(transaction))
    }

    #[test]
    fn envelopes_roundtrip_through_bytes() {
        let envelope = envelope(1234, vec![1, 2, 3]);
        let decoded = Envelope::from_bytes(&envelope.to_bytes()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn timestamp_lands_where_upstream_reads_it() {
        let timestamp_ms = 123_456_789;
        let bytes = envelope(timestamp_ms, vec![1, 2, 3]).to_bytes();
        let transaction = ConsensusTransaction::new(bytes.into());
        assert_eq!(
            transaction.extract_timestamp(),
            Some(Duration::from_millis(timestamp_ms))
        );
    }

    #[test]
    fn garbage_bytes_fail_to_decode() {
        assert!(Envelope::from_bytes(&[0xFF; 16]).is_err());
    }

    #[test]
    fn oversized_envelopes_fail_to_decode() {
        let args = vec![0; 2 * super::MAX_PAYLOAD_SIZE as usize];
        assert!(Envelope::from_bytes(&envelope(1234, args).to_bytes()).is_err());
    }
}
