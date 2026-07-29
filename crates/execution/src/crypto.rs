// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic primitives for state commitments.

/// The concrete hasher used for state commitments (Blake2b truncated to 256 bits).
pub(crate) type Hasher = blake2::Blake2b<digest::consts::U32>;

/// A 32-byte Blake2b-256 hash. The default value is the zero digest, which also serves as the
/// genesis state commitment.
#[derive(Clone, Copy, PartialEq, Default, Debug)]
pub struct Digest([u8; 32]);

impl Digest {
    pub(crate) fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// An arbitrary digest distinct per `byte`, for tests that need unequal commitments.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(byte: u8) -> Self {
        Self([byte; 32])
    }
}
