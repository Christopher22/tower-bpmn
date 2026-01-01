mod store;

use std::{collections::BTreeMap, ops::Index};

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
    pub fn can_fire(&self, marking: &Marking) -> bool {
        for arc in &self.input {
            if marking[arc.target] < arc.tokens {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct Arc<T> {
    pub target: Id<T>,
    pub tokens: usize,
}

/// The marking of a Petri net, representing the distribution of tokens across places.
#[derive(Debug, Clone)]
pub struct Marking(BTreeMap<Id<Place>, usize>);

impl Marking {
    /// Update the marking by firing the given transition, iff it can fire.
    pub fn update(&mut self, transition: &Transition) -> bool {
        if !transition.can_fire(self) {
            return false;
        }
        for arc in &transition.input {
            self.0
                .entry(arc.target)
                .and_modify(|e| *e = e.saturating_sub(arc.tokens));
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

/// A Petri net.
#[derive(Debug, Clone)]
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
    pub fn connect_place(&mut self, from: Id<Place>, to: Id<Transition>, tokens: usize) -> bool {
        let transition = &mut self.transitions[to];
        if transition.input.iter().any(|arc| arc.target == from) {
            return false;
        }
        transition.input.push(Arc {
            target: from,
            tokens,
        });
        true
    }

    /// Connect a transition to a place with the given number of tokens.
    pub fn connect_transition(
        &mut self,
        from: Id<Transition>,
        to: Id<Place>,
        tokens: usize,
    ) -> bool {
        let transition = &mut self.transitions[from];
        if transition.output.iter().any(|arc| arc.target == to) {
            return false;
        }
        transition.output.push(Arc { target: to, tokens });
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
}

impl Default for PetriNet {
    fn default() -> Self {
        PetriNet {
            places: Store::default(),
            transitions: Store::default(),
        }
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
        assert!(petri_net.connect_place(p1, t1, 2));
        assert!(petri_net.connect_transition(t1, p2, 1));
    }
}
