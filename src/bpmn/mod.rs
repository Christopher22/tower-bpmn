mod engine;
mod messages;
mod token;

use chrono::DateTime;
use parking_lot::RwLock;
use std::sync::Arc;
use std::{borrow::Cow, ops::IndexMut};

use crate::{executor::Executor, petri_net::FirstCompetingStrategy};

pub use self::engine::Runtime;
pub use self::messages::{CorrelationKey, Message, MessageManager, ProcessMessages, SendError};
pub use self::token::{Token, Value};

pub mod gateways {
    use std::ops::DerefMut;

    use crate::petri_net::{Id, PetriNet, Place};

    use super::Value;

    pub trait Gateway: 'static + Sized {}

    pub trait SplitableGateway: Gateway {
        fn add_nodes<const NUM: usize>(
            &self,
            petri_net: impl DerefMut<Target = PetriNet<super::Step, super::State>>,
            current_place: Id<Place<super::State>>,
        ) -> [Id<Place<super::State>>; NUM];
    }

    pub trait JoinableGateway<const NUM: usize, V: super::Value>: Gateway {
        type Output: Value;
    }

    /// The BPMN "XOR" gateway, which can be used for both splitting and joining. For splitting, exactly one output branch will be executed based on the callback function. For joining, the gateway will wait until one of the input branches is completed before proceeding.
    #[derive(Debug)]
    pub struct Xor<C: 'static, V: Value>(C, std::marker::PhantomData<V>);

    impl<V: Value> Xor<(), V> {
        pub fn for_joining() -> Self {
            Xor((), std::marker::PhantomData)
        }
    }

    impl<C: Fn(&super::Token, V) -> usize, V: Value> Xor<C, V> {
        pub fn for_splitting(callback: C) -> Self {
            Xor(callback, std::marker::PhantomData)
        }
    }

    impl<C, V: Value> Gateway for Xor<C, V> {}
    impl<const NUM: usize, V: Value> JoinableGateway<NUM, V> for Xor<(), V> {
        type Output = V;
    }
    impl<V: super::Value, C: Fn(&super::Token, V) -> usize + Clone + Sync + Send + 'static>
        SplitableGateway for Xor<C, V>
    {
        fn add_nodes<const NUM: usize>(
            &self,
            mut petri_net: impl DerefMut<Target = PetriNet<super::Step, super::State>>,
            current_place: Id<Place<super::State>>,
        ) -> [Id<Place<super::State>>; NUM] {
            let petri_net = petri_net.deref_mut();
            std::array::from_fn(|i| {
                let new_place = petri_net.add_place(crate::petri_net::Place::new(
                    format!("XOR Output {}", i),
                    super::State::Inactive,
                ));
                let transition = petri_net.add_transition(super::Step::Task(
                    format!("XOR Transition {}", i),
                    Box::new({
                        let callback = self.0.clone();
                        move |_name: &str, state: Vec<super::Token>| {
                            assert!(
                                state.len() == 1,
                                "Exactly one token should be consumed by a task"
                            );
                            let token = state.into_iter().next().unwrap();
                            let value = token
                                .get_last::<V>()
                                .expect("the input value should be present in the token history");
                            if callback(&token, value) == i {
                                vec![token]
                            } else {
                                vec![]
                            }
                        }
                    }),
                ));
                petri_net.connect_place(current_place, transition, ());
                petri_net.connect_transition(transition, new_place, ());
                new_place
            })
        }
    }

    /// The BPMN "AND" gateway, which can be used for both splitting and joining. For splitting, all output branches will be executed in parallel. For joining, the gateway will wait until all input branches are completed before proceeding.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct And;

    impl Gateway for And {}
    impl SplitableGateway for And {
        fn add_nodes<const NUM: usize>(
            &self,
            mut petri_net: impl DerefMut<Target = PetriNet<super::Step, super::State>>,
            current_place: Id<Place<super::State>>,
        ) -> [Id<Place<super::State>>; NUM] {
            let petri_net = petri_net.deref_mut();
            let transition_and = petri_net.add_transition(super::Step::And(NUM));
            petri_net.connect_place(current_place, transition_and, ());

            std::array::from_fn(|i| {
                let next_place = petri_net.add_place(crate::petri_net::Place::new(
                    format!("AND Output {}", i),
                    super::State::Inactive,
                ));
                petri_net.connect_transition(transition_and, next_place, ());
                next_place
            })
        }
    }
    impl<const NUM: usize, V: Value> JoinableGateway<NUM, V> for And
    where
        [V; NUM]: Value,
    {
        type Output = [V; NUM];
    }
}

pub trait ExtendedExecutor:
    Send + Executor<()> + Executor<crate::petri_net::Marking<State>>
{
}

impl<T: Send + Executor<()> + Executor<crate::petri_net::Marking<State>>> ExtendedExecutor for T {}

impl Token {
    /// Get the name of the current task this token is at. This is used for querying the current state of the process instance.
    pub fn current_task(&self) -> &str {
        todo!()
    }
}

/// A "color" for tokens in a BPMN process. This is used to track the state of the process and determine which tasks are enabled within a correpsonding PetriNet.
#[derive(Debug, PartialEq, Eq)]
pub enum State {
    Inactive,
    InProgress,
    Completed(Token),
}

impl Clone for State {
    fn clone(&self) -> Self {
        match self {
            Self::Inactive => Self::Inactive,
            Self::InProgress => Self::InProgress,
            Self::Completed(_) => panic!("State containing a token should not be cloned"),
        }
    }
}

impl Default for State {
    fn default() -> Self {
        State::Inactive
    }
}

impl crate::petri_net::Color for State {
    const DEFAULT_REF: &'static Self = &State::Inactive;

    type State = Vec<Token>;
    type Weight = ();

    fn is_transition_enabled<A>(
        transition: &crate::petri_net::Transition<A, Self>,
        marking: &crate::petri_net::Marking<Self>,
    ) -> bool {
        for arc in &transition.input {
            match marking[arc.target] {
                State::Completed(_) => {}
                _ => return false,
            }
        }
        true
    }

    fn update_input<A>(
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

    fn update_output<A>(
        transition: &crate::petri_net::Transition<A, Self>,
        marking: &mut crate::petri_net::Marking<Self>,
        state: Self::State,
    ) {
        if state.len() != transition.output.len() {
            panic!(
                "The number of tokens produced by the transition does not match the number of output arcs"
            );
        }
        for arc in &transition.output {
            marking[arc.target] = State::Inactive;
        }
        for (token, arc) in state.into_iter().zip(transition.output.iter()) {
            marking[arc.target] = State::Completed(token);
        }
    }
}

pub trait Process: 'static + Sized {
    type Input: Value;
    type Output: Value;

    fn name(&self) -> &str;

    /// Define the process by building a process builder.
    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output>;
}

enum Step {
    And(usize),
    Task(
        String,
        Box<dyn Fn(&str, Vec<Token>) -> Vec<Token> + Send + Sync>,
    ),
    Waitable(
        String,
        Box<
            dyn Fn(&str, Vec<Token>) -> Box<dyn futures::Future<Output = Vec<Token>> + Send>
                + Send
                + Sync,
        >,
    ),
}

impl crate::petri_net::Callable<Vec<Token>> for Step {
    fn create_future(
        &self,
        state: Vec<Token>,
    ) -> impl std::future::Future<Output = Vec<Token>> + 'static + Send {
        match self {
            Step::And(num_copies) => {
                assert_eq!(
                    state.len(),
                    1,
                    "Exactly one token should be consumed by an AND step"
                );
                let token = state.into_iter().next().unwrap();
                futures::future::Either::Left(futures::future::ready(
                    (0..*num_copies).map(|_| token.clone()).collect(),
                ))
            }
            Step::Task(name, task) => {
                let result = task(name, state);
                futures::future::Either::Left(futures::future::ready(result))
            }
            Step::Waitable(name, future) => futures::future::Either::Right(future(name, state)),
        }
    }
}

pub struct ProcessBuilder<P: Process, E: Value> {
    process: std::marker::PhantomData<fn() -> P>,
    input_type: std::marker::PhantomData<fn() -> E>,
    pub(crate) petri_net: Arc<RwLock<crate::petri_net::PetriNet<Step, State>>>,
    pub(crate) start_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) current_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
    message_manager: MessageManager,
}

impl<'a, P: Process, E: Value> ProcessBuilder<P, E> {
    pub(crate) fn new(message_manager: MessageManager) -> Self {
        let mut petri_net = crate::petri_net::PetriNet::default();
        let current_place =
            petri_net.add_place(crate::petri_net::Place::new("Start", State::Inactive));
        ProcessBuilder {
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: Arc::new(RwLock::new(petri_net)),
            start_place: current_place.clone(),
            current_place,
            message_manager,
        }
    }

    fn add_next_place(
        &mut self,
        name: String,
        step: Step,
    ) -> crate::petri_net::Id<crate::petri_net::Place<State>> {
        let mut petri_net = self.petri_net.write();
        let new_place =
            petri_net.add_place(crate::petri_net::Place::new(name.clone(), State::Inactive));
        let transition = petri_net.add_transition(step);
        petri_net.connect_place(self.current_place, transition, ());
        petri_net.connect_transition(transition, new_place, ());

        new_place
    }

    fn wrap_function<const ADD_OUTPUT: bool, U: Value>(
        func: impl Fn(&Token, E) -> U + 'static + Send + Sync,
    ) -> Box<dyn Fn(&str, Vec<Token>) -> Vec<Token> + Send + Sync> {
        Box::new(move |name: &str, state: Vec<Token>| {
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
                vec![token]
            }
        })
    }

    pub fn then<U: Value>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        func: impl Fn(&Token, E) -> U + 'static + Send + Sync,
    ) -> ProcessBuilder<P, U> {
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
        }
    }

    pub fn throw_message<P2: Process, V: Value>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        func: impl Fn(&Token, E) -> Message<P2, V> + 'static + Send + Sync,
    ) -> ProcessBuilder<P, E> {
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
        }
    }

    pub fn split<const NUM: usize, G: gateways::SplitableGateway>(
        self,
        gateway: G,
    ) -> [ProcessBuilder<P, E>; NUM] {
        gateway
            .add_nodes(self.petri_net.write(), self.current_place)
            .map(|place| ProcessBuilder {
                current_place: place,
                process: self.process,
                input_type: std::marker::PhantomData,
                petri_net: self.petri_net.clone(),
                start_place: self.start_place,
                message_manager: self.message_manager.clone(),
            })
    }

    pub fn wait_for<P2: Process, W: Waitable<P2, E> + 'static>(
        self,
        waitable: W,
    ) -> ProcessBuilder<P, E> {
        let name = waitable.name().to_string();
        let generator = Box::new(move |name: &str, state: Vec<Token>| {
            assert!(
                state.len() == 1,
                "Exactly one token should be consumed by a wait step"
            );
            let token = state.into_iter().next().unwrap();
            let value: E = token
                .get_last()
                .expect("the input value should be present in the token history");
            Box::new(waitable.wait_for(&token, value))
        });
        ProcessBuilder {
            current_place: self.add_next_place(name.clone(), Step::Waitable(name, generator)),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
        }
    }

    pub fn join<const NUM: usize, G: gateways::JoinableGateway<NUM, E>>(
        gateway: G,
        parts: [ProcessBuilder<P, E>; NUM],
    ) -> ProcessBuilder<P, G::Output> {
        todo!()
    }
}

/// A instance of a process running in the background.
pub struct Instance<'a, A: ExtendedExecutor> {
    engine: &'a Runtime<A>,
    uuid: uuid::Uuid,
    simulation: <A as Executor<crate::petri_net::Marking<State>>>::TaskHandle,
}

impl<'a, A: ExtendedExecutor> Instance<'a, A> {
    fn new<V: Value>(
        engine: &'a Runtime<A>,
        process_raw: &self::engine::RawProcess,
        input: V,
    ) -> Self {
        let simulation = process_raw.instantiate(engine.executor.clone(), input);
        Self {
            engine,
            uuid: uuid::Uuid::new_v4(),
            simulation: engine.executor.spawn_task(simulation.run()),
        }
    }

    /// Wait for the process instance to complete and return the final context. The context can be used to query the final state of the process.
    pub async fn wait_for_completion(&self) -> Token {
        todo!()
    }

    /// Stop the process instance and return the context for resuming later.
    pub fn stop(self) -> Token {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstanceError {
    /// Not registered
    Unregistered,
    /// Given the context, the process instance has already completed.
    Completed,
    /// The context does not match the process instance.
    InvalidContext,
}

pub trait Waitable<P: Process, T: Value, O: Value> {
    type Future: std::future::Future<Output = O> + Send;

    fn name(&self) -> &str;
    fn wait_for<'a>(self, token: &'a Token, value: T) -> Self::Future;
}

struct Timer(pub Cow<'static, str>);

impl<P: Process> Waitable<P, DateTime<chrono::Utc>, ()> for Timer {
    type Future = futures::future::Either<futures::future::Ready<()>, tokio::time::Sleep>;

    fn name(&self) -> &str {
        &self.0
    }

    fn wait_for(self, _token: &Token, value: DateTime<chrono::Utc>) -> Self::Future {
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

#[derive(Debug, PartialEq, Eq)]
pub struct IncomingMessage<P: Process, E: Value>(
    P,
    Cow<'static, str>,
    std::marker::PhantomData<fn() -> E>,
);

impl<P: Process, E: Value> IncomingMessage<P, E> {
    pub fn new(process: P, name: impl Into<Cow<'static, str>>) -> Self {
        IncomingMessage(process, name.into(), std::marker::PhantomData)
    }
}

/// TODO
pub struct TodoFuture<E> {
    _marker: std::marker::PhantomData<fn() -> E>,
}

impl<E> futures::Future for TodoFuture<E> {
    type Output = E;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        todo!()
    }
}

impl<P: Process, E: Value> Waitable<P, CorrelationKey, E> for IncomingMessage<P, E> {
    type Future = TodoFuture<E>;

    fn name(&self) -> &str {
        &self.1
    }

    fn wait_for<'a>(self, _token: &'a Token, _value: CorrelationKey) -> Self::Future {
        todo!()
    }
}
