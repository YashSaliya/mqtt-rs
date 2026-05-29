//! SUBACK packet — separate file for re-export convenience.
//! The actual struct lives in subscribe.rs — this re-exports it.

// Re-exported from subscribe so the packet module is internally consistent.
pub use crate::packet::subscribe::{SubAck, SubAckReason};
