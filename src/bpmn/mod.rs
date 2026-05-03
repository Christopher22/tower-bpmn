pub mod gateways;
pub mod messages;
mod process;
mod process_builder;
mod runtime;
pub(crate) mod steps;
#[cfg(test)]
mod tests;
mod value;
mod waitable;

use std::future::Future;
use std::ops::IndexMut;
use std::pin::Pin;

use crate::executor::Executor;

pub use self::process::{InvalidProcessNameError, MetaData, Process, ProcessName};
pub use self::process_builder::{ProcessBuilder, ProcessBuilderRef};
pub use self::runtime::{
    DynamicValue, Handle, Instance, InstanceId, InstanceNotRunning, InstanceSpawnError,
    InstanceStatus, Instances, ProcessError, RegisteredProcess, Runtime, Token, TokenId, storage,
};
pub use self::steps::{ExternalStep, ExternalStepData, Step, Steps, Type};
pub use self::value::{Extract, Value};
pub use self::waitable::{IncomingMessage, Timer, Waitable};

use self::storage::Storage;

/// Executor abstraction required by the BPMN runtime.
pub trait ExtendedExecutor<S: Storage>:
    'static + Sync + Send + Executor<()> + Executor<crate::petri_net::Marking<State<S>>>
{
}

impl<
    S: Storage,
    T: 'static + Sync + Send + Executor<()> + Executor<crate::petri_net::Marking<State<S>>>,
> ExtendedExecutor<S> for T
{
}

/// A "color" for tokens in a BPMN process. This is used to track the state of the process and determine which tasks are enabled within a correpsonding PetriNet.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum State<S: Storage> {
    /// No token is currently available at this place.
    #[default]
    Inactive,
    /// A transition has consumed the token and is currently executing.
    InProgress,
    /// Execution completed and produced a token at this place.
    Completed(Token<S>),
}

impl<S: Storage> Clone for State<S> {
    fn clone(&self) -> Self {
        match self {
            Self::Inactive => Self::Inactive,
            Self::InProgress => Self::InProgress,
            Self::Completed(_) => panic!("Clone is not supported."),
        }
    }
}

impl<S: Storage> crate::petri_net::Color for State<S> {
    const DEFAULT_REF: &'static Self = &State::Inactive;

    type State = Vec<Token<S>>;
    type Weight = ();
    type Id = ();

    fn is_transition_enabled<A: 'static>(
        transition: &crate::petri_net::Transition<A, Self>,
        marking: &crate::petri_net::Marking<Self>,
    ) -> bool {
        for arc in &transition.input {
            match marking[arc.target] {
                State::Completed(_) => {}
                _ => return false,
            }
        }
        // For XOR gateway branches, additionally check the guard condition using
        // runtime type introspection. This ensures only the selected branch is enabled.
        use std::any::Any;
        let action_any: &dyn Any = &transition.action;
        if let Some(BpmnStep::XorBranch(_, guard)) = action_any.downcast_ref::<BpmnStep<S>>() {
            if let Some(arc) = transition.input.first() {
                if let State::Completed(ref token) = marking[arc.target] {
                    return guard(token);
                }
            }
            return false;
        }
        true
    }

    fn update_input<A: 'static>(
        transition: &crate::petri_net::Transition<A, Self>,
        marking: &mut crate::petri_net::Marking<Self>,
    ) -> Option<Self::State> {
        if !Self::is_transition_enabled(transition, marking) {
            return None;
        }

        let tokens = transition
            .input
            .iter()
            .filter_map(|arc| {
                let state_old = marking.index_mut(arc.target);
                if let State::Completed(token) = std::mem::replace(state_old, State::InProgress) {
                    return Some(token);
                }
                None
            })
            .collect();

        Some(tokens)
    }

    fn update_output<A: 'static>(
        transition: &crate::petri_net::Transition<A, Self>,
        marking: &mut crate::petri_net::Marking<Self>,
        state: Self::State,
    ) {
        if state.len() != transition.output.len() {
            panic!(
                "The number of tokens produced by the transition does not match the number of output arcs"
            );
        }
        for arc in &transition.input {
            marking[arc.target] = State::Inactive;
        }
        for (token, arc) in state.into_iter().zip(transition.output.iter()) {
            marking[arc.target] = State::Completed(token);
        }
    }
}

pub(crate) enum BpmnStep<S: Storage> {
    And(usize),
    Task(Step, StepTaskFn<S>),
    Waitable(Step, StepWaitFn<S>),
    /// An exclusive-gateway branch. The guard closure returns `true` when this
    /// particular branch is the one selected by the XOR condition at run-time.
    /// `is_transition_enabled` uses it so the branch is invisible to the simulation
    /// unless it is the correct one, giving proper BPMN XOR-split semantics.
    XorBranch(Step, StepXorGuardFn<S>),
}

impl<S: Storage> std::fmt::Debug for BpmnStep<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And(arg0) => f.debug_tuple("And").field(arg0).finish(),
            Self::Task(_, _) => f.debug_tuple("Task").finish_non_exhaustive(),
            Self::Waitable(_, _) => f.debug_tuple("Waitable").finish_non_exhaustive(),
            Self::XorBranch(_, _) => f.debug_tuple("XorBranch").finish_non_exhaustive(),
        }
    }
}

pub(crate) type StepTaskFn<S> = Box<dyn Fn(Step, Vec<Token<S>>) -> Vec<Token<S>> + Send + Sync>;
pub(crate) type StepWaitFuture<S> = Pin<Box<dyn futures::Future<Output = Vec<Token<S>>> + Send>>;
pub(crate) type StepWaitFn<S> = Box<dyn Fn(Step, Vec<Token<S>>) -> StepWaitFuture<S> + Send + Sync>;
pub(crate) type StepXorGuardFn<S> = Box<dyn Fn(&Token<S>) -> bool + Send + Sync>;

impl<S: Storage> crate::petri_net::Callable<Vec<Token<S>>> for BpmnStep<S> {
    fn create_future(
        &self,
        state: Vec<Token<S>>,
    ) -> impl Future<Output = Vec<Token<S>>> + 'static + Send {
        match self {
            BpmnStep::And(num_copies) => {
                assert_eq!(
                    state.len(),
                    1,
                    "Exactly one token should be consumed by an AND step"
                );
                let token = state.into_iter().next().unwrap();
                Box::pin(futures::future::ready(
                    (0..*num_copies).map(|_| token.fork()).collect(),
                ))
            }
            BpmnStep::Task(name, task) => {
                let result = task(name.clone(), state);
                Box::pin(futures::future::ready(result))
            }
            BpmnStep::Waitable(name, future) => future(name.clone(), state),
            BpmnStep::XorBranch(name, _guard) => {
                // Guard already checked by is_transition_enabled; this branch is the
                // selected one. Pass the token through, recording the step name.
                Box::pin(futures::future::ready(
                    state
                        .into_iter()
                        .map(|token| token.set_output(name.clone(), ()))
                        .collect(),
                ))
            }
        }
    }
}
