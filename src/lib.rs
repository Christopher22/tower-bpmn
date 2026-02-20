//! Deriving BPMN models from code.

#![deny(
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]
#![allow(private_bounds)]

mod bpmn;
/// Executor backends for running asynchronous process tasks.
pub mod executor;
pub(crate) mod petri_net;

pub use bpmn::*;
