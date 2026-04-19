use parking_lot::RwLock;
use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;

use crate::petri_net::PetriNet;

use super::{
    BpmnStep, MetaData, Process, State, Step, StepTaskFn, Storage, Token, Type, Value, Waitable,
    gateways,
    messages::{self, SendableWithFixedTarget},
    steps,
    storage::NoOutput,
};

/// A (mutable) reference to a process builder used internally.
#[derive(Debug)]
pub struct ProcessBuilderRef<'a, S: Storage, T> {
    pub(crate) petri_net: parking_lot::RwLockWriteGuard<'a, PetriNet<BpmnStep<S>, State<S>>>,
    pub(crate) targets: T,
    pub(crate) steps: &'a steps::StepsBuilder,
}

/// Builder used to define BPMN processes declaratively.
pub struct ProcessBuilder<P: Process, E: Value, S: Storage> {
    /// Meta data of the process being built.
    pub meta_data: MetaData,
    process: std::marker::PhantomData<fn() -> P>,
    input_type: std::marker::PhantomData<fn() -> E>,
    pub(crate) petri_net: Arc<RwLock<PetriNet<BpmnStep<S>, State<S>>>>,
    pub(crate) start_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    pub(crate) current_place: crate::petri_net::Id<crate::petri_net::Place<State<S>>>,
    message_manager: messages::MessageBroker,
    pub(crate) steps: steps::StepsBuilder,
    output_type: Type,
}

impl<P: Process, E: Value, S: Storage> std::fmt::Debug for ProcessBuilder<P, E, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBuilder").finish_non_exhaustive()
    }
}

impl<P: Process, E: Value, S: Storage> ProcessBuilder<P, E, S> {
    pub(crate) fn new(meta_data: MetaData, message_manager: messages::MessageBroker) -> Self {
        let mut petri_net = PetriNet::default();
        let current_place = petri_net.add_place(crate::petri_net::Place::new((), State::Inactive));

        // Add a special start step that is always enabled to kick off the process.
        let steps = steps::StepsBuilder::default();
        steps
            .add_start::<E>(P::INITIAL_OWNER.clone())
            .expect("start already existing");

        ProcessBuilder {
            meta_data,
            process: std::marker::PhantomData,
            input_type: std::marker::PhantomData,
            petri_net: Arc::new(RwLock::new(petri_net)),
            start_place: current_place,
            current_place,
            message_manager,
            steps,
            output_type: Type::new::<E>(),
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
            self.steps.add_end(self.output_type.clone()),
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
        let name = self
            .steps
            .add::<U>(name.into())
            .expect("failed to add step");
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
            output_type: Type::new::<U>(),
        }
    }

    /// Adds a throw-message task that emits a message for another process.
    pub fn throw_message<P2: Process, SM: SendableWithFixedTarget<P2>>(
        mut self,
        name: impl Into<Cow<'static, str>>,
        process: P2,
        func: impl Fn(&Token<S>, E) -> SM + 'static + Send + Sync,
    ) -> ProcessBuilder<P, E, S> {
        let name = self
            .steps
            .add::<NoOutput>(name.into())
            .expect("failed to add step");
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
            output_type: Type::new::<NoOutput>(),
        }
    }

    /// Splits the flow into `NUM` branches using the provided gateway.
    pub fn split<const NUM: usize, G: gateways::SplitableGateway<S>>(
        self,
        gateway: G,
    ) -> [ProcessBuilder<P, E, S>; NUM] {
        gateway
            .add_nodes(ProcessBuilderRef {
                petri_net: self.petri_net.write(),
                targets: self.current_place,
                steps: &self.steps,
            })
            .map(|place| ProcessBuilder {
                current_place: place,
                process: self.process,
                input_type: std::marker::PhantomData,
                petri_net: self.petri_net.clone(),
                start_place: self.start_place,
                message_manager: self.message_manager.clone(),
                steps: self.steps.clone(),
                meta_data: self.meta_data.clone(),
                output_type: self.output_type.clone(),
            })
    }

    /// Adds a wait state that resumes once the given waitable completes.
    pub fn wait_for<P2: Process, O: Value, W: Waitable<P2, E, O> + Send + Sync + 'static>(
        mut self,
        mut waitable: W,
    ) -> ProcessBuilder<P, O, S> {
        let step = waitable.register(&mut self.steps, &self.message_manager);
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
            let future: Pin<Box<dyn Future<Output = Vec<Token<S>>> + Send>> =
                Box::pin(async move {
                    let output = waitable.wait_for(&token, value).await;
                    vec![token.set_output(name.clone(), output)]
                });
            future
        });
        ProcessBuilder {
            current_place: self.add_next_place(BpmnStep::Waitable(step, generator)),
            process: self.process,
            input_type: std::marker::PhantomData,
            petri_net: self.petri_net,
            start_place: self.start_place,
            message_manager: self.message_manager,
            steps: self.steps,
            meta_data: self.meta_data,
            output_type: Type::new::<O>(),
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
        let current_place = gateway.add_nodes(ProcessBuilderRef {
            petri_net: first.petri_net.write(),
            targets: current_places,
            steps: &first.steps,
        });

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
            output_type: Type::new::<G::Output>(),
        }
    }
}
