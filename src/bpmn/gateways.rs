//! Gateway primitives for splitting and joining BPMN control flow.

use std::{borrow::Cow, ops::DerefMut};

use crate::{
    bpmn::{ProcessBuilderRef, Storage, steps::Step, storage::NoOutput},
    petri_net::{Id, Place},
};

use super::Value;

/// Marker trait for BPMN gateways.
pub trait Gateway<S: Storage>: 'static + Sized {}

/// Gateway that splits one incoming branch into multiple outgoing branches.
pub trait SplitableGateway<S: Storage>: Gateway<S> {
    /// Adds the required Petri-net nodes for this split gateway.
    fn add_nodes<const NUM: usize>(
        &self,
        process_builder: ProcessBuilderRef<S, Id<Place<super::State<S>>>>,
    ) -> [Id<Place<super::State<S>>>; NUM];
}

/// Gateway that joins multiple incoming branches into one outgoing branch.
pub trait JoinableGateway<const NUM: usize, V: Value, S: Storage>: Gateway<S> {
    /// Output value produced after joining all required branches.
    type Output: Value;

    /// Adds the required Petri-net nodes for this join gateway.
    fn add_nodes(
        self,
        process_builder: ProcessBuilderRef<S, [Id<Place<super::State<S>>>; NUM]>,
    ) -> Id<Place<super::State<S>>>;
}

/// The BPMN "XOR" gateway, which can be used for both splitting and joining. For splitting, exactly one output branch will be executed based on the callback function. For joining, the gateway will wait until one of the input branches is completed before proceeding.
#[derive(Debug)]
pub struct Xor<C: 'static, V: Value, S: Storage>(
    Cow<'static, str>,
    C,
    std::marker::PhantomData<fn(S) -> V>,
);

impl<V: Value, S: Storage> Xor<(), V, S> {
    /// Creates an XOR gateway configured for joining branches.
    pub fn for_joining(name: impl Into<Cow<'static, str>>) -> Self {
        Xor(name.into(), (), std::marker::PhantomData)
    }
}

impl<S: Storage, C: Fn(&super::Token<S>, V) -> usize, V: Value> Xor<C, V, S> {
    /// Creates an XOR gateway configured for splitting branches.
    pub fn for_splitting(name: impl Into<Cow<'static, str>>, callback: C) -> Self {
        Xor(name.into(), callback, std::marker::PhantomData)
    }
}

impl<C, V: Value, S: Storage> Gateway<S> for Xor<C, V, S> {}

impl<S: Storage, const NUM: usize, V: Value> JoinableGateway<NUM, V, S> for Xor<(), V, S> {
    type Output = V;

    fn add_nodes(
        self,
        mut process_builder: ProcessBuilderRef<S, [Id<Place<super::State<S>>>; NUM]>,
    ) -> Id<Place<super::State<S>>> {
        let petri_net = process_builder.petri_net.deref_mut();
        let output = petri_net.add_place(Place::new((), super::State::Inactive));

        let join_name = process_builder
            .steps
            .add::<NoOutput>(self.0)
            .expect("valid name");
        for place in process_builder.targets.into_iter() {
            let transition = petri_net.add_transition(super::BpmnStep::Task(
                join_name.clone(),
                Box::new(|name: Step, state: Vec<super::Token<S>>| {
                    state
                        .into_iter()
                        .map(|token| token.set_output(name.clone(), ()))
                        .collect()
                }),
            ));
            petri_net.connect_place(place, transition, ());
            petri_net.connect_transition(transition, output, ());
        }

        output
    }
}

impl<S: Storage, V: Value, C: Fn(&super::Token<S>, V) -> usize + Clone + Sync + Send + 'static>
    SplitableGateway<S> for Xor<C, V, S>
{
    fn add_nodes<const NUM: usize>(
        &self,
        mut process_builder: ProcessBuilderRef<S, Id<Place<super::State<S>>>>,
    ) -> [Id<Place<super::State<S>>>; NUM] {
        let petri_net = process_builder.petri_net.deref_mut();
        std::array::from_fn(|i| {
            let new_place = petri_net.add_place(Place::new((), super::State::Inactive));
            // Use XorBranch so that only the selected branch transition is enabled
            // according to the BPMN XOR-split standard: exactly one outgoing path fires.
            let transition = petri_net.add_transition(super::BpmnStep::XorBranch(
                process_builder
                    .steps
                    .add::<NoOutput>(format!("{}: {}", self.0, i))
                    .expect("valid name"),
                Box::new({
                    let callback = self.1.clone();
                    move |token: &super::Token<S>| {
                        let value = token
                            .get_last::<V>()
                            .expect("the input value should be present in the token history");
                        callback(token, value) == i
                    }
                }),
            ));
            petri_net.connect_place(process_builder.targets, transition, ());
            petri_net.connect_transition(transition, new_place, ());
            new_place
        })
    }
}

/// The BPMN "AND" gateway, which can be used for both splitting and joining. For splitting, all output branches will be executed in parallel. For joining, the gateway will wait until all input branches are completed before proceeding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct And(pub Cow<'static, str>);

impl And {
    /// Creates a new AND gateway with the given name.
    pub const fn new(name: &'static str) -> Self {
        And(Cow::Borrowed(name))
    }
}

impl<S: Storage> Gateway<S> for And {}

impl<S: Storage> SplitableGateway<S> for And {
    fn add_nodes<const NUM: usize>(
        &self,
        mut process_builder: ProcessBuilderRef<S, Id<Place<super::State<S>>>>,
    ) -> [Id<Place<super::State<S>>>; NUM] {
        let petri_net = process_builder.petri_net.deref_mut();
        let transition_and = petri_net.add_transition(super::BpmnStep::And(NUM));
        petri_net.connect_place(process_builder.targets, transition_and, ());

        std::array::from_fn(|_| {
            let next_place = petri_net.add_place(Place::new((), super::State::Inactive));
            petri_net.connect_transition(transition_and, next_place, ());
            next_place
        })
    }
}

impl<S: Storage, const NUM: usize, V: Value> JoinableGateway<NUM, V, S> for And
where
    [V; NUM]: Value,
{
    type Output = [V; NUM];

    fn add_nodes(
        self,
        mut process_builder: ProcessBuilderRef<S, [Id<Place<super::State<S>>>; NUM]>,
    ) -> Id<Place<super::State<S>>> {
        let petri_net = process_builder.petri_net.deref_mut();
        let output = petri_net.add_place(Place::new((), super::State::Inactive));

        let transition = petri_net.add_transition(super::BpmnStep::Task(
            process_builder
                .steps
                .add::<[V; NUM]>(self.0)
                .expect("valid name"),
            Box::new(|name: Step, state: Vec<super::Token<S>>| {
                assert_eq!(
                    state.len(),
                    NUM,
                    "AND join must receive exactly {NUM} tokens"
                );
                let output_values: [V; NUM] = match state
                    .iter()
                    .map(|token| {
                        token
                            .get_last::<V>()
                            .expect("each joined token must contain the expected value")
                    })
                    .collect::<Vec<_>>()
                    .try_into()
                {
                    Ok(values) => values,
                    Err(_) => panic!("array length checked above"),
                };
                let token = state
                    .into_iter()
                    .next()
                    .expect("array length checked above");
                vec![token.set_output(name, output_values)]
            }),
        ));

        for place in process_builder.targets {
            petri_net.connect_place(place, transition, ());
        }
        petri_net.connect_transition(transition, output, ());
        output
    }
}
