//! Checkpoint engine.
//!
//! Modeled on Sui's Builder/Aggregator/Executor pipeline but collapsed for a
//! single fake flow: batch committed sub-dags into pending checkpoints, obtain
//! effects synchronously from the execution engine, and self-certify.
//! Maintains `highest_synced` and `highest_executed` watermarks from day one.
