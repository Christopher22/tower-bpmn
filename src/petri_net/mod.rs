mod color;
mod simulation;
mod store;

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Index, IndexMut},
};

#[allow(unused_imports)]
pub use self::color::{Color, UsizeWeight};
#[allow(unused_imports)]
pub use self::simulation::{Callable, CompetingStrategy, FirstCompetingStrategy, Simulation};
pub use self::store::{Entry, Id, Store};

/// A place in a Petri net.
#[derive(Debug, Clone)]
pub struct Place<C: Color> {
    /// The name of the place.
    pub name: C::Id,
    /// The initial number of tokens in the place.
    initial_tokens: C,
}

impl<C: Color> Place<C> {
    /// Create a new place with the given name and initial tokens.
    pub fn new(name: impl Into<C::Id>, initial_tokens: C) -> Self {
        Place {
            name: name.into(),
            initial_tokens,
        }
    }
}

/// A transition in a Petri net.
pub struct Transition<A, C: Color> {
    /// The action to perform when the transition fires.
    pub action: A,
    pub(crate) input: Vec<Arc<Place<C>, C>>,
    pub(crate) output: Vec<Arc<Place<C>, C>>,
}

impl<A, C: Color> std::fmt::Debug for Transition<A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transition").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Arc<T, C: Color> {
    pub target: Id<T>,
    pub weights: C::Weight,
}

/// The marking of a Petri net, representing the distribution of tokens across places.
#[derive(Debug, Clone)]
pub struct Marking<C: Color>(BTreeMap<Id<Place<C>>, C>);

impl<C: Color> Marking<C> {
    /// Create an empty marking with no tokens.
    pub const fn empty() -> Self {
        Marking(BTreeMap::new())
    }

    /// Iterate over places that currently have non-default color values.
    pub fn iter(&self) -> impl Iterator<Item = (&Id<Place<C>>, &C)> {
        self.0.iter()
    }
}

impl<C: Color> Index<Id<Place<C>>> for Marking<C> {
    type Output = C;

    fn index(&self, index: Id<Place<C>>) -> &Self::Output {
        self.0.get(&index).unwrap_or(C::DEFAULT_REF)
    }
}

impl<C: Color> IndexMut<Id<Place<C>>> for Marking<C> {
    fn index_mut(&mut self, index: Id<Place<C>>) -> &mut Self::Output {
        self.0.entry(index).or_default()
    }
}

/// A Petri net.
#[derive(Debug)]
pub struct PetriNet<A, C: Color> {
    places: Store<Place<C>>,
    transitions: Store<Transition<A, C>>,
}

/// A tuple representing two connected places with a transition in between.
pub type ConnectedPlaces<A, C> = (Id<Place<C>>, Id<Transition<A, C>>, Id<Place<C>>);

impl<A, C: Color> PetriNet<A, C> {
    /// Add a place to the Petri net.
    pub fn add_place(&mut self, place: Place<C>) -> Id<Place<C>> {
        self.places.push(place)
    }

    /// Add a transition to the Petri net.
    pub fn add_transition(&mut self, action: A) -> Id<Transition<A, C>> {
        self.transitions.push(Transition {
            input: Vec::new(),
            output: Vec::new(),
            action,
        })
    }

    /// Convenience function to add two connected places with a transition in between, all with default weights.
    pub fn add_connected_places(
        &mut self,
        p1: Place<C>,
        p2: Place<C>,
        action: A,
    ) -> ConnectedPlaces<A, C> {
        let place_from = self.add_place(p1);
        let place_to = self.add_place(p2);
        let transition = self.add_transition(action);
        self.connect_place(place_from, transition, C::Weight::default());
        self.connect_transition(transition, place_to, C::Weight::default());
        (place_from, transition, place_to)
    }

    /// Connect a place to a transition with the given number of tokens.
    pub fn connect_place(
        &mut self,
        from: Id<Place<C>>,
        to: Id<Transition<A, C>>,
        weights: C::Weight,
    ) -> bool {
        let transition = &mut self.transitions[to];
        if transition.input.iter().any(|arc| arc.target == from) {
            return false;
        }
        transition.input.push(Arc {
            target: from,
            weights,
        });
        true
    }

    /// Connect a transition to a place with the given number of tokens.
    pub fn connect_transition(
        &mut self,
        from: Id<Transition<A, C>>,
        to: Id<Place<C>>,
        weights: C::Weight,
    ) -> bool {
        let transition = &mut self.transitions[from];
        if transition.output.iter().any(|arc| arc.target == to) {
            return false;
        }
        transition.output.push(Arc {
            target: to,
            weights,
        });
        true
    }

    /// Get the initial marking of the Petri net.
    pub fn initial_marking(&self) -> Marking<C> {
        Marking(
            self.places
                .iter()
                .filter_map(|entry| {
                    if &entry.item.initial_tokens != C::DEFAULT_REF {
                        Some((entry.id, entry.item.initial_tokens.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    /// Iterate over the places in the Petri net.
    pub fn places(&self) -> impl Iterator<Item = Entry<'_, Place<C>>> {
        self.places.iter()
    }

    /// Iterate over the transitions in the Petri net.
    pub fn transitions(&self) -> impl Iterator<Item = Entry<'_, Transition<A, C>>> {
        self.transitions.iter()
    }
}

impl<A, C: Color> Default for PetriNet<A, C> {
    fn default() -> Self {
        PetriNet {
            places: Store::default(),
            transitions: Store::default(),
        }
    }
}

impl<A, C: Color> Index<Id<Place<C>>> for PetriNet<A, C> {
    type Output = Place<C>;

    fn index(&self, index: Id<Place<C>>) -> &Self::Output {
        &self.places[index]
    }
}

impl<A, C: Color> Index<Id<Transition<A, C>>> for PetriNet<A, C> {
    type Output = Transition<A, C>;

    fn index(&self, index: Id<Transition<A, C>>) -> &Self::Output {
        &self.transitions[index]
    }
}

pub enum EnabledTransitions<'a, A, C: Color> {
    Independent(Entry<'a, Transition<A, C>>),
    Competing(Vec<Entry<'a, Transition<A, C>>>),
}

impl<A, C: Color> EnabledTransitions<'_, A, C> {
    pub fn find_all<'a>(
        petri_net: &'a PetriNet<A, C>,
        marking: &Marking<C>,
    ) -> impl ExactSizeIterator<Item = EnabledTransitions<'a, A, C>> {
        let mut transitions = std::collections::HashMap::new();

        for transition in petri_net.transitions() {
            if C::is_transition_enabled(&transition, marking) {
                let inputs: BTreeSet<Id<Place<C>>> =
                    transition.input.iter().map(|arc| arc.target).collect();
                transitions
                    .entry(inputs)
                    .or_insert_with(|| Vec::with_capacity(3))
                    .push(transition);
            }
        }

        transitions.into_values().map(|mut transitions| {
            if transitions.len() == 1 {
                EnabledTransitions::Independent(transitions.pop().unwrap())
            } else {
                EnabledTransitions::Competing(transitions)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_marking() {
        let mut petri_net = PetriNet::<fn(), usize>::default();
        let p1 = petri_net.add_place(Place::new("Place 1", 2));
        let p2 = petri_net.add_place(Place::new("Place 2", 0));
        let p3 = petri_net.add_place(Place::new("Place 3", 5));

        let marking = petri_net.initial_marking();
        assert_eq!(marking[p1], 2);
        assert_eq!(marking[p2], 0);
        assert_eq!(marking[p3], 5);
    }

    #[test]
    fn test_transition_firing() {
        let mut petri_net = PetriNet::default();
        let (_, trans_s_p1, _) = petri_net.add_connected_places(
            Place::new("Place 1", 1),
            Place::new("Place 2", 0),
            || {},
        );

        let trans_s_p1 = &petri_net[trans_s_p1];
        let mut marking = petri_net.initial_marking();
        assert!(usize::is_transition_enabled(trans_s_p1, &marking));
        assert!(usize::update_input(trans_s_p1, &mut marking).is_some());
        assert!(!usize::is_transition_enabled(trans_s_p1, &marking));
        usize::update_output(trans_s_p1, &mut marking, ());
        assert!(!usize::is_transition_enabled(trans_s_p1, &marking));
    }

    #[test]
    fn test_duplicate_place_connection_is_rejected() {
        let mut petri_net = PetriNet::<fn(), usize>::default();
        let p1 = petri_net.add_place(Place::new("P1", 1));
        let p2 = petri_net.add_place(Place::new("P2", 0));
        let t = petri_net.add_transition(|| {});

        assert!(petri_net.connect_place(p1, t, UsizeWeight::DEFAULT));
        assert!(!petri_net.connect_place(p1, t, UsizeWeight::DEFAULT));
        assert!(petri_net.connect_transition(t, p2, UsizeWeight::DEFAULT));
    }

    #[test]
    fn test_duplicate_transition_connection_is_rejected() {
        let mut petri_net = PetriNet::<fn(), usize>::default();
        let p1 = petri_net.add_place(Place::new("P1", 1));
        let p2 = petri_net.add_place(Place::new("P2", 0));
        let t = petri_net.add_transition(|| {});

        assert!(petri_net.connect_place(p1, t, UsizeWeight::DEFAULT));
        assert!(petri_net.connect_transition(t, p2, UsizeWeight::DEFAULT));
        assert!(!petri_net.connect_transition(t, p2, UsizeWeight::DEFAULT));
    }

    #[test]
    fn test_no_enabled_transitions_for_empty_marking() {
        let mut petri_net = PetriNet::<fn(), usize>::default();
        petri_net.add_connected_places(Place::new("Start", 0), Place::new("End", 0), || {});

        let enabled: Vec<_> = EnabledTransitions::find_all(&petri_net, &Marking::empty()).collect();
        assert!(enabled.is_empty());
    }

    #[test]
    fn test_enabled_transitions() {
        let mut petri_net = PetriNet::<fn(), usize>::default();
        let (_, trans_s_p1, p1) =
            petri_net.add_connected_places(Place::new("Start", 1), Place::new("P1", 1), || {});
        let p2 = petri_net.add_place(Place::new("P2", 0));
        let (p3, _, _) =
            petri_net.add_connected_places(Place::new("P3", 0), Place::new("End", 0), || {});

        let trans_p1_p2 = petri_net.add_transition(|| {});
        assert!(petri_net.connect_place(p1, trans_p1_p2, UsizeWeight::DEFAULT));
        assert!(petri_net.connect_transition(trans_p1_p2, p2, UsizeWeight::DEFAULT));

        let trans_p1_p3 = petri_net.add_transition(|| {});
        assert!(petri_net.connect_place(p1, trans_p1_p3, UsizeWeight::DEFAULT));
        assert!(petri_net.connect_transition(trans_p1_p3, p3, UsizeWeight::DEFAULT));

        let enabled_transitions: Vec<_> =
            EnabledTransitions::find_all(&petri_net, &petri_net.initial_marking()).collect();

        assert_eq!(enabled_transitions.len(), 2);
        assert!(
            enabled_transitions
                .iter()
                .any(|value| matches!(value, EnabledTransitions::Competing(v) if v.len() == 2 && v.iter().any(|t| t.id == trans_p1_p2) && v.iter().any(|t| t.id == trans_p1_p3)))
        );
        assert!(enabled_transitions.iter().any(
            |value| matches!(value, EnabledTransitions::Independent(t) if t.id == trans_s_p1)
        ));
    }
}
