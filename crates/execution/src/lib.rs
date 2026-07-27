//! Transaction execution.
//!
//! Defines the `ExecutionEngine` trait consumed by the checkpoint and
//! validator crates. The first implementation is a fake engine for demos and
//! simulation; a move-native backend comes later. Implementations must be
//! generic over `dag::context::Ctx` (no threads, disk, or wall-clock) so the
//! whole validator stays simulatable, and take explicit seeds/config for any
//! randomness (`Ctx` deliberately exposes no RNG).
