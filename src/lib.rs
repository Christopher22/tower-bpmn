//! Deriving BPMN models from code.

#![forbid(unsafe_code)]
#![forbid(missing_docs)]

mod petri_net;

pub use petri_net::{Id, Marking, PetriNet, Place, Transition};
