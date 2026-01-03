mod simulation;
mod store;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZero,
    ops::{Index, IndexMut},
};

pub use self::simulation::{CompetingStrategy, Executor, FirstCompetingStrategy, Simulation};
pub use self::store::{Entry, Id, Store};

/// A place in a Petri net.
#[derive(Debug, Clone)]
pub struct Place {
    /// The name of the place.
    pub name: String,
    /// The initial number of tokens in the place.
    pub initial_tokens: usize,
}

impl Place {
    /// Create a new place with the given name and initial tokens.
    pub fn new(name: impl Into<String>, initial_tokens: usize) -> Self {
        Place {
            name: name.into(),
            initial_tokens,
        }
    }
}

/// A transition in a Petri net.
#[derive(Debug, Clone)]
pub struct Transition {
    /// The action to perform when the transition fires.
    pub action: fn(),
    input: Vec<Arc<Place>>,
    output: Vec<Arc<Place>>,
}

impl Transition {
    /// Check if the transition can fire given the marking.
    pub fn is_enabled(&self, marking: &Marking) -> bool {
        for arc in &self.input {
            if marking[arc.target] < arc.weights {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct Arc<T> {
    pub target: Id<T>,
    pub weights: usize,
}

/// The marking of a Petri net, representing the distribution of tokens across places.
#[derive(Debug, Clone)]
pub struct Marking(BTreeMap<Id<Place>, usize>);

impl Marking {
    /// Create an empty marking with no tokens.
    pub const fn empty() -> Self {
        Marking(BTreeMap::new())
    }

    /// Update the marking by firing the given transition, iff it is enabled.
    pub fn update_input(&mut self, transition: &Transition) -> bool {
        if !transition.is_enabled(self) {
            return false;
        }
        for arc in &transition.input {
            self.0
                .entry(arc.target)
                .and_modify(|e| *e = e.saturating_sub(arc.weights));
        }
        true
    }

    /// Update the marking by adding tokens from the given transition.
    pub fn update_output(&mut self, transition: &Transition) {
        for arc in &transition.output {
            self.0
                .entry(arc.target)
                .and_modify(|e| *e += arc.weights)
                .or_insert(arc.weights);
        }
    }

    /// Check if this marking is included in another marking.
    pub fn is_included_in(&self, other: &Marking) -> bool {
        for (place_id, &tokens) in &self.0 {
            let other_tokens = other[*place_id];
            if tokens > other_tokens {
                return false;
            }
        }
        true
    }
}

impl Index<Id<Place>> for Marking {
    type Output = usize;

    fn index(&self, index: Id<Place>) -> &Self::Output {
        const DEFAULT: usize = 0;
        self.0.get(&index).unwrap_or(&DEFAULT)
    }
}

impl IndexMut<Id<Place>> for Marking {
    fn index_mut(&mut self, index: Id<Place>) -> &mut Self::Output {
        self.0.entry(index).or_insert(0)
    }
}

/// A Petri net.
#[derive(Debug, Clone, Default)]
pub struct PetriNet {
    places: Store<Place>,
    transitions: Store<Transition>,
}

impl PetriNet {
    /// The default weight for arcs.
    pub const DEFAULT_WEIGHT: NonZero<usize> = NonZero::new(1).unwrap();

    /// Add a place to the Petri net.
    pub fn add_place(&mut self, place: Place) -> Id<Place> {
        self.places.push(place)
    }

    /// Add a transition to the Petri net.
    pub fn add_transition(&mut self, callback: fn()) -> Id<Transition> {
        self.transitions.push(Transition {
            input: Vec::new(),
            output: Vec::new(),
            action: callback,
        })
    }

    /// Convenience function to add two connected places with a transition in between, all with weight of 1.
    pub fn add_connected_places(
        &mut self,
        p1: Place,
        p2: Place,
        callback: fn(),
    ) -> (Id<Place>, Id<Transition>, Id<Place>) {
        let place_from = self.add_place(p1);
        let place_to = self.add_place(p2);
        let transition = self.add_transition(callback);
        self.connect_place(place_from, transition, Self::DEFAULT_WEIGHT);
        self.connect_transition(transition, place_to, Self::DEFAULT_WEIGHT);
        (place_from, transition, place_to)
    }

    /// Connect a place to a transition with the given number of tokens.
    pub fn connect_place(
        &mut self,
        from: Id<Place>,
        to: Id<Transition>,
        weights: NonZero<usize>,
    ) -> bool {
        let transition = &mut self.transitions[to];
        if transition.input.iter().any(|arc| arc.target == from) {
            return false;
        }
        transition.input.push(Arc {
            target: from,
            weights: weights.get(),
        });
        true
    }

    /// Connect a transition to a place with the given number of tokens.
    pub fn connect_transition(
        &mut self,
        from: Id<Transition>,
        to: Id<Place>,
        weights: NonZero<usize>,
    ) -> bool {
        let transition = &mut self.transitions[from];
        if transition.output.iter().any(|arc| arc.target == to) {
            return false;
        }
        transition.output.push(Arc {
            target: to,
            weights: weights.get(),
        });
        true
    }

    /// Get the initial marking of the Petri net.
    pub fn initial_marking(&self) -> Marking {
        Marking(
            self.places
                .iter()
                .filter_map(|entry| {
                    if entry.item.initial_tokens > 0 {
                        Some((entry.id, entry.item.initial_tokens))
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    /// Iterate over the places in the Petri net.
    pub fn places(&self) -> impl Iterator<Item = Entry<'_, Place>> {
        self.places.iter()
    }

    /// Iterate over the transitions in the Petri net.
    pub fn transitions(&self) -> impl Iterator<Item = Entry<'_, Transition>> {
        self.transitions.iter()
    }
}

impl Index<Id<Place>> for PetriNet {
    type Output = Place;

    fn index(&self, index: Id<Place>) -> &Self::Output {
        &self.places[index]
    }
}

impl Index<Id<Transition>> for PetriNet {
    type Output = Transition;

    fn index(&self, index: Id<Transition>) -> &Self::Output {
        &self.transitions[index]
    }
}

pub enum EnabledTransitions<'a> {
    Independent(Entry<'a, Transition>),
    Competing(Vec<Entry<'a, Transition>>),
}

impl EnabledTransitions<'_> {
    pub fn find_all<'a>(
        petri_net: &'a PetriNet,
        marking: Marking,
    ) -> impl ExactSizeIterator<Item = EnabledTransitions<'a>> {
        let mut transitions = std::collections::HashMap::new();

        for transition in petri_net.transitions() {
            if transition.is_enabled(&marking) {
                let inputs: BTreeSet<Id<Place>> =
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
        let mut petri_net = PetriNet::default();
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
        assert!(trans_s_p1.is_enabled(&marking));
        assert!(marking.update_input(trans_s_p1));
        assert!(!trans_s_p1.is_enabled(&marking));
        marking.update_output(trans_s_p1);
        assert!(!trans_s_p1.is_enabled(&marking));
    }

    #[test]
    fn test_marking_is_included() {
        let mut marking1 = Marking::empty();
        marking1[Id::new_test(0)] = 3;
        marking1[Id::new_test(1)] = 5;

        let mut marking2 = Marking::empty();
        marking2[Id::new_test(0)] = 4;
        marking2[Id::new_test(1)] = 5;
        marking2[Id::new_test(2)] = 2;

        assert!(marking1.is_included_in(&marking2));
        assert!(!marking2.is_included_in(&marking1));
    }

    #[test]
    fn test_enabled_transitions() {
        let mut petri_net = PetriNet::default();

        let (_, trans_s_p1, p1) =
            petri_net.add_connected_places(Place::new("Start", 1), Place::new("P1", 1), || {});
        let p2 = petri_net.add_place(Place::new("P2", 0));
        let (p3, _, _) =
            petri_net.add_connected_places(Place::new("P3", 0), Place::new("End", 0), || {});

        let trans_p1_p2 = petri_net.add_transition(|| {});
        assert!(petri_net.connect_place(p1, trans_p1_p2, PetriNet::DEFAULT_WEIGHT));
        assert!(petri_net.connect_transition(trans_p1_p2, p2, PetriNet::DEFAULT_WEIGHT));

        let trans_p1_p3 = petri_net.add_transition(|| {});
        assert!(petri_net.connect_place(p1, trans_p1_p3, PetriNet::DEFAULT_WEIGHT));
        assert!(petri_net.connect_transition(trans_p1_p3, p3, PetriNet::DEFAULT_WEIGHT));

        let enabled_transitions: Vec<_> =
            EnabledTransitions::find_all(&petri_net, petri_net.initial_marking()).collect();

        assert_eq!(enabled_transitions.len(), 2);
        assert!(
            enabled_transitions
                .iter()
                .find(|value| matches!(value, EnabledTransitions::Competing(v) if v.len() == 2 && v.iter().any(|t| t.id == trans_p1_p2) && v.iter().any(|t| t.id == trans_p1_p3)))
                .is_some()
        );
        assert!(
            enabled_transitions
                .iter()
                .find(|value| matches!(value, EnabledTransitions::Independent(t) if t.id == trans_s_p1))
                .is_some()
        );
    }
}
