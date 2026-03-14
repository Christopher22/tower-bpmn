/// Gateway primitives for splitting and joining BPMN control flow.
pub mod gateways;
mod messages;
mod process;
mod runtime;
#[cfg(test)]
mod tests;

use chrono::DateTime;
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{borrow::Cow, ops::IndexMut};

use crate::executor::Executor;

pub use self::messages::{CorrelationKey, Message, MessageManager, ProcessMessages, SendError};
pub use self::process::{InvalidProcessNameError, MetaData, Process, ProcessName};
pub use self::runtime::{
    Handle, InMemory, InMemoryStorage, Instance, InstanceId, InstanceNotRunning,
    InstanceSpawnError, InstanceStatus, Instances, ProcessError, RegisteredProcess,
    ResumableProcess, ResumeError, Runtime, RuntimeApiError, Storage, StorageBackend, Token,
    TokenId, Value,
};

/// Executor abstraction required by the BPMN runtime.
pub trait ExtendedExecutor<S: Storage>:
    'static + Send + Executor<()> + Executor<crate::petri_net::Marking<State<S>>>
{
}

impl<S: Storage, T: 'static + Send + Executor<()> + Executor<crate::petri_net::Marking<State<S>>>>
    ExtendedExecutor<S> for T
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
    type Id = String;

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
        if let Some(step) = action_any.downcast_ref::<Step<S>>() {
            if let Step::XorBranch(_, guard) = step {
                if let Some(arc) = transition.input.first() {
                    if let State::Completed(ref token) = marking[arc.target] {
                        return guard(token);
                    }
                }
                return false;
            }
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

pub(crate) enum Step<S: Storage> {
    And(usize),
    Task(String, StepTaskFn<S>),
    Waitable(String, StepWaitFn<S>),
    /// An exclusive-gateway branch. The guard closure returns `true` when this
    /// particular branch is the one selected by the XOR condition at run-time.
    /// `is_transition_enabled` uses it so the branch is invisible to the simulation
    /// unless it is the correct one, giving proper BPMN XOR-split semantics.
    XorBranch(String, Box<dyn Fn(&Token<S>) -> bool + Send + Sync>),
}

impl<S: Storage> std::fmt::Debug for Step<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And(arg0) => f.debug_tuple("And").field(arg0).finish(),
            Self::Task(_, _) => f.debug_tuple("Task").finish_non_exhaustive(),
            Self::Waitable(_, _) => f.debug_tuple("Waitable").finish_non_exhaustive(),
            Self::XorBranch(_, _) => f.debug_tuple("XorBranch").finish_non_exhaustive(),
        }
    }
}

type StepTaskFn<S> = Box<dyn Fn(&str, Vec<Token<S>>) -> Vec<Token<S>> + Send + Sync>;
type StepWaitFuture<S> = Pin<Box<dyn futures::Future<Output = Vec<Token<S>>> + Send>>;
type StepWaitFn<S> = Box<dyn Fn(&str, Vec<Token<S>>) -> StepWaitFuture<S> + Send + Sync>;

impl<S: Storage> crate::petri_net::Callable<Vec<Token<S>>> for Step<S> {
    fn create_future(
        &self,
        state: Vec<Token<S>>,
    ) -> impl Future<Output = Vec<Token<S>>> + 'static + Send {
        match self {
            Step::And(num_copies) => {
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
            Step::Task(name, task) => {
                let result = task(name, state);
                Box::pin(futures::future::ready(result))
            }
            Step::Waitable(name, future) => future(name, state),
            Step::XorBranch(name, _guard) => {
                // Guard already checked by is_transition_enabled; this branch is the
                // selected one. Pass the token through, recording the step name.
                let name = name.clone();
                Box::pin(futures::future::ready(
                    state
                        .into_iter()
                        .map(|token| token.set_output(&name, ()))
                        .collect(),
                ))
            }
        }
    }
}

pub(crate) const START_NAME: &str = "Start";

/// Builder used to define BPMN processes declaratively.
pub struct ProcessBuilder<P: Process, E: Value, S: Storage> {
    meta_data: MetaData,
    process: std::marker::PhantomData<fn() -> P>,
    input_type: std::marker::PhantomData<fn() -> E>,
    petri_net: Arc<RwLock<crate::petri_net::PetriNet<Step<S>, State<S>>>>,
    start_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    current_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    message_manager: MessageManager,
    used_names: std::collections::HashSet<String>,
}

impl<P: Process, E: Value, S: Storage> std::fmt::Debug for ProcessBuilder<P, E, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBuilder").finish_non_exhaustive()
    }
}

impl<P: Process, E: Value, S: Storage> ProcessBuilder<P, E, S> {
    pub(crate) fn new(meta_data: MetaData, message_manager: MessageManager) -> Self {
        let mut petri_net = crate::petri_net::PetriNet::default();
        let current_place =
            petri_net.add_place(crate::petri_net::Place::new(START_NAME, State::Inactive));
        ProcessBuilder {
            meta_data,
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: Arc::new(RwLock::new(petri_net)),
            start_place: current_place,
            current_place,
            message_manager,
            used_names: [START_NAME.to_string()].into_iter().collect(),
        }
    }

    fn add_next_place(
        &mut self,
        name: String,
        step: Step<S>,
    ) -> crate::petri_net::Id<crate::petri_net::Place<State<S>>> {
        // Ensure that task names are unique within the process definition for better observability and debugging.
        if self.used_names.contains(&name) {
            panic!("Duplicate task name: {name}");
        } else {
            self.used_names.insert(name.clone());
        }

        let mut petri_net = self.petri_net.write();
        let new_place =
            petri_net.add_place(crate::petri_net::Place::new(name.clone(), State::Inactive));
        let transition = petri_net.add_transition(step);
        petri_net.connect_place(self.current_place, transition, ());
        petri_net.connect_transition(transition, new_place, ());

        new_place
    }

    fn wrap_function<const ADD_OUTPUT: bool, U: Value>(
        func: impl Fn(&Token<S>, E) -> U + 'static + Send + Sync,
    ) -> StepTaskFn<S> {
        Box::new(move |name: &str, state: Vec<Token<S>>| {
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

    /// Adds a service task that transforms the current payload.
    pub fn then<U: Value>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        func: impl Fn(&Token<S>, E) -> U + 'static + Send + Sync,
    ) -> ProcessBuilder<P, U, S> {
        let name: String = name.into().into();
        ProcessBuilder {
            current_place: self.add_next_place(
                name.clone(),
                Step::Task(name, Self::wrap_function::<true, U>(func)),
            ),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            used_names: self.used_names,
            meta_data: self.meta_data,
        }
    }

    /// Adds a throw-message task that emits a message for another process.
    pub fn throw_message<P2: Process, V: Value>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        func: impl Fn(&Token<S>, E) -> Message<P2, V> + 'static + Send + Sync,
    ) -> ProcessBuilder<P, E, S> {
        let name: String = name.into().into();
        let sender = self.message_manager.get_messages_for_process::<P2>();
        ProcessBuilder {
            current_place: self.add_next_place(
                name.clone(),
                Step::Task(
                    name,
                    Self::wrap_function::<false, _>(move |token, value| {
                        let message = func(token, value);
                        sender.send(message.correlation_key, message.payload);
                    }),
                ),
            ),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            used_names: self.used_names,
            meta_data: self.meta_data,
        }
    }

    /// Splits the flow into `NUM` branches using the provided gateway.
    pub fn split<const NUM: usize, G: gateways::SplitableGateway<S>>(
        self,
        gateway: G,
    ) -> [ProcessBuilder<P, E, S>; NUM] {
        gateway
            .add_nodes(self.petri_net.write(), self.current_place)
            .map(|place| ProcessBuilder {
                current_place: place,
                process: self.process,
                input_type: std::marker::PhantomData,
                petri_net: self.petri_net.clone(),
                start_place: self.start_place,
                message_manager: self.message_manager.clone(),
                used_names: self.used_names.clone(),
                meta_data: self.meta_data.clone(),
            })
    }

    /// Adds a wait state that resumes once the given waitable completes.
    pub fn wait_for<P2: Process, O: Value, W: Waitable<P2, E, O> + Send + Sync + 'static>(
        mut self,
        mut waitable: W,
    ) -> ProcessBuilder<P, O, S> {
        waitable.bind_messages(self.message_manager.get_messages_for_process::<P2>());
        let waitable = Arc::new(waitable);
        let name = waitable.name().to_string();
        let generator = Box::new(move |name: &str, state: Vec<Token<S>>| {
            let waitable = waitable.clone();
            assert!(
                state.len() == 1,
                "Exactly one token should be consumed by a wait step"
            );
            let token = state.into_iter().next().unwrap();
            let value: E = token
                .get_last()
                .expect("the input value should be present in the token history");
            let output_name = name.to_string();
            let token = token.set_output(name, ());
            let future: Pin<Box<dyn futures::Future<Output = Vec<Token<S>>> + Send>> =
                Box::pin(async move {
                    let output = waitable.wait_for(&token, value).await;
                    vec![token.set_output(output_name, output)]
                });
            future
        });
        ProcessBuilder {
            current_place: self.add_next_place(name.clone(), Step::Waitable(name, generator)),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            used_names: self.used_names,
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
        let used_names = parts
            .iter()
            .flat_map(|part| part.used_names.iter())
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for part in parts.iter().skip(1) {
            assert!(
                Arc::ptr_eq(&first.petri_net, &part.petri_net),
                "All process parts must belong to the same process definition"
            );
        }
        let current_places = parts.each_ref().map(|part| part.current_place);
        let current_place = gateway.add_nodes(first.petri_net.write(), current_places);

        // Deconstruct the array and discard everything except the first element.
        let first = parts.into_iter().next().unwrap();
        ProcessBuilder {
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: first.petri_net,
            start_place: first.start_place,
            current_place,
            message_manager: first.message_manager,
            used_names,
            meta_data: first.meta_data,
        }
    }
}

/// Asynchronous wait abstraction used for BPMN waiting events.
pub trait Waitable<P: Process, T: Value, O: Value> {
    /// Future returned by the wait implementation.
    type Future: Future<Output = O> + Send;

    /// Human readable wait-step name written into token history.
    fn name(&self) -> &str;
    /// Optional hook to inject process-scoped message channels.
    fn bind_messages(&mut self, _messages: ProcessMessages) {}
    /// Starts waiting for a value and resolves to the produced output.
    fn wait_for<S: Storage>(&self, token: &Token<S>, value: T) -> Self::Future;
}

struct Timer(pub Cow<'static, str>);

impl<P: Process> Waitable<P, DateTime<chrono::Utc>, ()> for Timer {
    type Future = futures::future::Either<futures::future::Ready<()>, tokio::time::Sleep>;

    fn name(&self) -> &str {
        &self.0
    }

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        value: DateTime<chrono::Utc>,
    ) -> Self::Future {
        chrono::Utc::now()
            .signed_duration_since(value)
            .to_std()
            .map_or_else(
                |_| {
                    // If the value is in the past, return immediately.
                    futures::future::Either::Left(futures::future::ready(()))
                },
                |duration| {
                    // If the value is in the future, wait until then.
                    futures::future::Either::Right(tokio::time::sleep(duration))
                },
            )
    }
}

/// Waitable that resumes when a correlated message arrives.
#[derive(Debug)]
pub struct IncomingMessage<P: Process, E: Value>(
    P,
    Cow<'static, str>,
    Option<ProcessMessages>,
    std::marker::PhantomData<fn() -> E>,
);

impl<P: Process, E: Value> IncomingMessage<P, E> {
    /// Creates a new incoming-message waitable.
    pub fn new(process: P, name: impl Into<Cow<'static, str>>) -> Self {
        IncomingMessage(process, name.into(), None, std::marker::PhantomData)
    }
}

impl<P: Process, E: Value> Waitable<P, CorrelationKey, E> for IncomingMessage<P, E> {
    type Future = futures::future::BoxFuture<'static, E>;

    fn name(&self) -> &str {
        &self.1
    }

    fn bind_messages(&mut self, messages: ProcessMessages) {
        self.2 = Some(messages);
    }

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        correlation_key: CorrelationKey,
    ) -> Self::Future {
        let messages = self
            .2
            .clone()
            .expect("incoming message waitable must be bound to a runtime");

        let mut receiver = messages.subscribe();
        if let Some(message) = messages.receive::<E>(correlation_key) {
            return Box::pin(async move { message });
        }

        Box::pin(async move {
            loop {
                match receiver.recv().await {
                    Ok(key) if key == correlation_key => {
                        if let Some(message) = messages.receive::<E>(key) {
                            return message;
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("message channel closed before expected message arrived")
                    }
                }
            }
        })
    }
}
