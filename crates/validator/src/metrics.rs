// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator's metrics, mirroring `dag::metrics` conventions: one `Arc`-shared front
//! type, metric names as adjacent constants, registration into a caller-provided registry.

use std::{sync::Arc, time::Duration};

use prometheus::{Histogram, Registry, register_histogram_with_registry};

// Metric names.
const SUBDAG_EXECUTION_LATENCY_S: &str = "subdag_execution_latency_s";

pub struct ValidatorMetrics {
    subdag_execution_latency_s: Histogram,
}

impl ValidatorMetrics {
    pub fn new(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            subdag_execution_latency_s: register_histogram_with_registry!(
                SUBDAG_EXECUTION_LATENCY_S,
                "Commit delivery to execution and checkpoint inclusion latency per sub-dag (s)",
                // Local execution is sub-millisecond but commit-channel queueing under
                // backpressure can reach seconds: ~100 µs to ~13 s, exponential.
                prometheus::exponential_buckets(1e-4, 2.0, 18).unwrap(),
                registry,
            )
            .unwrap(),
        })
    }

    pub(crate) fn observe_subdag_execution_latency(&self, latency: Duration) {
        self.subdag_execution_latency_s
            .observe(latency.as_secs_f64());
    }

    /// The raw histogram, for test assertions (`get_sample_count`/`get_sample_sum`).
    pub fn subdag_execution_latency_s(&self) -> &Histogram {
        &self.subdag_execution_latency_s
    }
}
