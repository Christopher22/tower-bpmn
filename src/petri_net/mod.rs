mod store;

use std::{
    collections::BTreeMap,
    num::NonZero,
    ops::{Index, IndexMut},
};

pub use self::store::{Id, Store};

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
                .filter_map(|(id, place)| {
                    if place.initial_tokens > 0 {
                        Some((id, place.initial_tokens))
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    /// Iterate over the places in the Petri net.
    pub fn places(&self) -> impl Iterator<Item = (Id<Place>, &Place)> {
        self.places.iter()
    }

    /// Iterate over the transitions in the Petri net.
    pub fn transitions(&self) -> impl Iterator<Item = (Id<Transition>, &Transition)> {
        self.transitions.iter()
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
        let p1 = petri_net.add_place(Place::new("Place 1", 3));
        let p2 = petri_net.add_place(Place::new("Place 2", 0));
        let t1 = petri_net.add_transition(|| {
            println!("Transition fired");
        });
        assert!(petri_net.connect_place(p1, t1, NonZero::new(2).unwrap()));
        assert!(petri_net.connect_transition(t1, p2, NonZero::new(1).unwrap()));
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
}
