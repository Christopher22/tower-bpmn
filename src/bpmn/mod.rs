pub mod gateways;
pub mod messages;
mod process;
mod runtime;
mod steps;
#[cfg(test)]
mod tests;
mod waitable;

use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{borrow::Cow, ops::IndexMut};

use crate::executor::Executor;
use crate::messages::SendableWithFixedTarget;

pub use self::process::{InvalidProcessNameError, MetaData, Process, ProcessName};
pub use self::runtime::{
    DynamicInput, DynamicValue, Handle, InMemory, InMemoryStorage, Instance, InstanceId,
    InstanceNotRunning, InstanceSpawnError, InstanceStatus, Instances, ProcessError,
    RegisteredProcess, ResumableProcess, ResumeError, Runtime, Sqlite, SqliteError,
    SqliteStorage, Storage, StorageBackend, Token, TokenId, Value,
};
pub use self::steps::{InvalidStep, Step, Steps, StepsBuilder, UnfinishedBuilder};
pub use self::waitable::{Bindable, IncomingMessage, Timer, Waitable};

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

type StepTaskFn<S> = Box<dyn Fn(Step, Vec<Token<S>>) -> Vec<Token<S>> + Send + Sync>;
type StepWaitFuture<S> = Pin<Box<dyn futures::Future<Output = Vec<Token<S>>> + Send>>;
type StepWaitFn<S> = Box<dyn Fn(Step, Vec<Token<S>>) -> StepWaitFuture<S> + Send + Sync>;
type StepXorGuardFn<S> = Box<dyn Fn(&Token<S>) -> bool + Send + Sync>;

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

/// Builder used to define BPMN processes declaratively.
pub struct ProcessBuilder<P: Process, E: Value, S: Storage> {
    meta_data: MetaData,
    process: std::marker::PhantomData<fn() -> P>,
    input_type: std::marker::PhantomData<fn() -> E>,
    petri_net: Arc<RwLock<crate::petri_net::PetriNet<BpmnStep<S>, State<S>>>>,
    start_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    current_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    message_manager: messages::MessageBroker,
    steps: StepsBuilder,
}

impl<P: Process, E: Value, S: Storage> std::fmt::Debug for ProcessBuilder<P, E, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBuilder").finish_non_exhaustive()
    }
}

impl<P: Process, E: Value, S: Storage> ProcessBuilder<P, E, S> {
    pub(crate) fn new(meta_data: MetaData, message_manager: messages::MessageBroker) -> Self {
        let mut petri_net = crate::petri_net::PetriNet::default();
        let current_place = petri_net.add_place(crate::petri_net::Place::new((), State::Inactive));

        // Add a special start step that is always enabled to kick off the process.
        let steps = StepsBuilder::default();
        steps.add_start();

        ProcessBuilder {
            meta_data,
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: Arc::new(RwLock::new(petri_net)),
            start_place: current_place,
            current_place,
            message_manager,
            steps,
        }
    }

    fn add_next_place(
        &mut self,
        step: BpmnStep<S>,
    ) -> crate::petri_net::Id<crate::petri_net::Place<State<S>>> {
        let mut petri_net = self.petri_net.write();
        let new_place = petri_net.add_place(crate::petri_net::Place::new((), State::Inactive));
        let transition = petri_net.add_transition(step);
        petri_net.connect_place(self.current_place, transition, ());
        petri_net.connect_transition(transition, new_place, ());

        new_place
    }

    fn wrap_function<const ADD_OUTPUT: bool, U: Value>(
        func: impl Fn(&Token<S>, E) -> U + 'static + Send + Sync,
    ) -> StepTaskFn<S> {
        Box::new(move |name: Step, state: Vec<Token<S>>| {
            assert!(
                state.len() == 1,
                "Exactly one token should be consumed by a task"
            );
            let token = state.into_iter().next().unwrap();
            let value: E = token
                .get_last()
                .expect("the input value should be present in the token history");
            let output = func(&token, value);
            if ADD_OUTPUT {
                vec![token.set_output(name, output)]
            } else {
                vec![token.set_output(name, ())]
            }
        })
    }

    /// Add a end. This is required to complete the process definition and make it executable.
    pub(super) fn add_end(mut self) -> Self {
        let end_place = self.add_next_place(BpmnStep::Task(
            self.steps.add_end(),
            Box::new(|name: Step, state: Vec<Token<S>>| {
                assert!(
                    state.len() == 1,
                    "Exactly one token should be consumed by a task"
                );
                vec![state.into_iter().next().unwrap().set_output(name, ())]
            }),
        ));
        self.current_place = end_place;
        self
    }

    /// Adds a service task that transforms the current payload.
    pub fn then<U: Value>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        func: impl Fn(&Token<S>, E) -> U + 'static + Send + Sync,
    ) -> ProcessBuilder<P, U, S> {
        let name = self.steps.add(name.into()).expect("failed to add step");
        ProcessBuilder {
            current_place: self
                .add_next_place(BpmnStep::Task(name, Self::wrap_function::<true, U>(func))),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            steps: self.steps,
            meta_data: self.meta_data,
        }
    }

    /// Adds a throw-message task that emits a message for another process.
    pub fn throw_message<P2: Process, SM: SendableWithFixedTarget<P2>>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        process: P2,
        func: impl Fn(&Token<S>, E) -> SM + 'static + Send + Sync,
    ) -> ProcessBuilder<P, E, S> {
        let name = self.steps.add(name.into()).expect("failed to add step");
        let sender = self.message_manager.get_messages_for_process(process);
        ProcessBuilder {
            current_place: self.add_next_place(BpmnStep::Task(
                name,
                Self::wrap_function::<false, _>(move |token, value| {
                    func(token, value).send(&sender);
                }),
            )),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            steps: self.steps,
            meta_data: self.meta_data,
        }
    }

    /// Splits the flow into `NUM` branches using the provided gateway.
    pub fn split<const NUM: usize, G: gateways::SplitableGateway<S>>(
        self,
        gateway: G,
    ) -> [ProcessBuilder<P, E, S>; NUM] {
        gateway
            .add_nodes(self.petri_net.write(), self.current_place, &self.steps)
            .map(|place| ProcessBuilder {
                current_place: place,
                process: self.process,
                input_type: std::marker::PhantomData,
                petri_net: self.petri_net.clone(),
                start_place: self.start_place,
                message_manager: self.message_manager.clone(),
                steps: self.steps.clone(),
                meta_data: self.meta_data.clone(),
            })
    }

    /// Adds a wait state that resumes once the given waitable completes.
    pub fn wait_for<P2: Process, O: Value, W: Waitable<P2, E, O> + Send + Sync + 'static>(
        mut self,
        mut waitable: W,
    ) -> ProcessBuilder<P, O, S> {
        let name = self.steps.add(waitable.name()).expect("failed to add step");

        waitable.bind_messages(name.clone(), &self.message_manager);
        let waitable = Arc::new(waitable);
        let generator = Box::new(move |name: Step, state: Vec<Token<S>>| {
            let waitable = waitable.clone();
            assert!(
                state.len() == 1,
                "Exactly one token should be consumed by a wait step"
            );
            let token = state.into_iter().next().unwrap();
            let value: E = token
                .get_last()
                .expect("the input value should be present in the token history");

            let token = token.set_output(name.clone(), ());
            let future: Pin<Box<dyn futures::Future<Output = Vec<Token<S>>> + Send>> =
                Box::pin(async move {
                    let output = waitable.wait_for(&token, value).await;
                    vec![token.set_output(name.clone(), output)]
                });
            future
        });
        ProcessBuilder {
            current_place: self.add_next_place(BpmnStep::Waitable(name, generator)),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            steps: self.steps,
            meta_data: self.meta_data,
        }
    }

    /// Joins parallel branches into one flow using the provided gateway.
    pub fn join<const NUM: usize, G: gateways::JoinableGateway<NUM, E, S>>(
        gateway: G,
        parts: [ProcessBuilder<P, E, S>; NUM],
    ) -> ProcessBuilder<P, G::Output, S> {
        assert_ne!(NUM, 0, "Cannot join zero branches");

        let first = &parts[0];
        for part in parts.iter().skip(1) {
            assert!(
                Arc::ptr_eq(&first.petri_net, &part.petri_net),
                "All process parts must belong to the same process definition"
            );
        }
        let current_places = parts.each_ref().map(|part| part.current_place);
        let current_place =
            gateway.add_nodes(first.petri_net.write(), current_places, &first.steps);

        // Deconstruct the array and discard everything except the first element.
        let first = parts.into_iter().next().unwrap();
        ProcessBuilder {
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: first.petri_net,
            start_place: first.start_place,
            current_place,
            message_manager: first.message_manager,
            steps: first.steps,
            meta_data: first.meta_data,
        }
    }
}
