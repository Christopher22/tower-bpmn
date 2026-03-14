use std::ops::DerefMut;

use crate::{
    Storage,
    petri_net::{Id, PetriNet, Place},
};

use super::Value;

/// Marker trait for BPMN gateways.
pub trait Gateway<S: Storage>: 'static + Sized {}

/// Gateway that splits one incoming branch into multiple outgoing branches.
pub trait SplitableGateway<S: Storage>: Gateway<S> {
    /// Adds the required Petri-net nodes for this split gateway.
    fn add_nodes<const NUM: usize>(
        &self,
        petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_place: Id<Place<super::State<S>>>,
    ) -> [Id<Place<super::State<S>>>; NUM];
}

/// Gateway that joins multiple incoming branches into one outgoing branch.
pub trait JoinableGateway<const NUM: usize, V: Value, S: Storage>: Gateway<S> {
    /// Output value produced after joining all required branches.
    type Output: Value;

    /// Adds the required Petri-net nodes for this join gateway.
    fn add_nodes(
        self,
        petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_places: [Id<Place<super::State<S>>>; NUM],
    ) -> Id<Place<super::State<S>>>;
}

/// The BPMN "XOR" gateway, which can be used for both splitting and joining. For splitting, exactly one output branch will be executed based on the callback function. For joining, the gateway will wait until one of the input branches is completed before proceeding.
#[derive(Debug)]
pub struct Xor<C: 'static, V: Value, S: Storage>(C, std::marker::PhantomData<fn(S) -> V>);

impl<V: Value, S: Storage> Xor<(), V, S> {
    /// Creates an XOR gateway configured for joining branches.
    pub fn for_joining() -> Self {
        Xor((), std::marker::PhantomData)
    }
}

impl<S: Storage, C: Fn(&super::Token<S>, V) -> usize, V: Value> Xor<C, V, S> {
    /// Creates an XOR gateway configured for splitting branches.
    pub fn for_splitting(callback: C) -> Self {
        Xor(callback, std::marker::PhantomData)
    }
}

impl<C, V: Value, S: Storage> Gateway<S> for Xor<C, V, S> {}

impl<S: Storage, const NUM: usize, V: Value> JoinableGateway<NUM, V, S> for Xor<(), V, S> {
    type Output = V;

    fn add_nodes(
        self,
        mut petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_places: [Id<Place<super::State<S>>>; NUM],
    ) -> Id<Place<super::State<S>>> {
        let petri_net = petri_net.deref_mut();
        let output = petri_net.add_place(Place::new("XOR Join Output", super::State::Inactive));

        for (_, place) in current_places.into_iter().enumerate() {
            let transition = petri_net.add_transition(super::Step::Task(
                "XOR Join".to_string(),
                Box::new(|name: &str, state: Vec<super::Token<S>>| {
                    state
                        .into_iter()
                        .map(|token| token.set_output(name, ()))
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
        mut petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_place: Id<Place<super::State<S>>>,
    ) -> [Id<Place<super::State<S>>>; NUM] {
        let petri_net = petri_net.deref_mut();
        std::array::from_fn(|i| {
            let new_place = petri_net.add_place(Place::new(
                format!("XOR Output {i}"),
                super::State::Inactive,
            ));
            // Use XorBranch so that only the selected branch transition is enabled
            // according to the BPMN XOR-split standard: exactly one outgoing path fires.
            let transition = petri_net.add_transition(super::Step::XorBranch(
                format!("XOR Transition {i}"),
                Box::new({
                    let callback = self.0.clone();
                    move |token: &super::Token<S>| {
                        let value = token
                            .get_last::<V>()
                            .expect("the input value should be present in the token history");
                        callback(token, value) == i
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct And;

impl<S: Storage> Gateway<S> for And {}

impl<S: Storage> SplitableGateway<S> for And {
    fn add_nodes<const NUM: usize>(
        &self,
        mut petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_place: Id<Place<super::State<S>>>,
    ) -> [Id<Place<super::State<S>>>; NUM] {
        let petri_net = petri_net.deref_mut();
        let transition_and = petri_net.add_transition(super::Step::And(NUM));
        petri_net.connect_place(current_place, transition_and, ());

        std::array::from_fn(|i| {
            let next_place = petri_net.add_place(Place::new(
                format!("AND Output {i}"),
                super::State::Inactive,
            ));
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
        mut petri_net: impl DerefMut<Target = PetriNet<super::Step<S>, super::State<S>>>,
        current_places: [Id<Place<super::State<S>>>; NUM],
    ) -> Id<Place<super::State<S>>> {
        let petri_net = petri_net.deref_mut();
        let output = petri_net.add_place(Place::new("AND Join Output", super::State::Inactive));

        let transition = petri_net.add_transition(super::Step::Task(
            "AND Join".to_string(),
            Box::new(|name: &str, state: Vec<super::Token<S>>| {
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

        for place in current_places {
            petri_net.connect_place(place, transition, ());
        }
        petri_net.connect_transition(transition, output, ());
        output
    }
}
