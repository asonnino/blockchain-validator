// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The validator's metrics, mirroring `dag::metrics` conventions: one `Arc`-shared front
//! type, metric names as adjacent constants, registration into a caller-provided registry.

use std::{sync::Arc, time::Duration};

use prometheus::{Histogram, Registry, register_histogram_with_registry};

// Metric names.
const SUBDAG_EXECUTION_LATENCY_S: &str = "subdag_execution_latency_s";
const CHECKPOINT_CERTIFICATION_LATENCY_S: &str = "checkpoint_certification_latency_s";
const END_TO_END_LATENCY_S: &str = "end_to_end_latency_s";

/// Mysticeti's consensus-latency buckets: dense around the expected commit latency.
const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.2, 0.3, 0.35, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1., 1.25, 1.5, 1.75, 2., 3.0, 5., 10.,
];

pub struct ValidatorMetrics {
    subdag_execution_latency_s: Histogram,
    checkpoint_certification_latency_s: Histogram,
    end_to_end_latency_s: Histogram,
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
            checkpoint_certification_latency_s: register_histogram_with_registry!(
                CHECKPOINT_CERTIFICATION_LATENCY_S,
                "Checkpoint creation to quorum certification latency (s)",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            end_to_end_latency_s: register_histogram_with_registry!(
                END_TO_END_LATENCY_S,
                "Submission to certified-checkpoint inclusion latency (s)",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
        })
    }

    pub(crate) fn observe_subdag_execution_latency(&self, latency: Duration) {
        self.subdag_execution_latency_s
            .observe(latency.as_secs_f64());
    }

    pub(crate) fn observe_checkpoint_certification_latency(&self, latency: Duration) {
        self.checkpoint_certification_latency_s
            .observe(latency.as_secs_f64());
    }

    pub(crate) fn observe_end_to_end_latency(&self, latency: Duration) {
        self.end_to_end_latency_s.observe(latency.as_secs_f64());
    }

    /// The raw histograms, for test assertions (`get_sample_count`/`get_sample_sum`).
    pub fn subdag_execution_latency_s(&self) -> &Histogram {
        &self.subdag_execution_latency_s
    }

    pub fn checkpoint_certification_latency_s(&self) -> &Histogram {
        &self.checkpoint_certification_latency_s
    }

    pub fn end_to_end_latency_s(&self) -> &Histogram {
        &self.end_to_end_latency_s
    }
}
