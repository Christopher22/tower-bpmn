use super::{Marking, Transition};

/// A color for tokens in a colored Petri net.
pub trait Color: 'static + Eq + Sized + Default + std::fmt::Debug + Clone {
    /// The default color value. This is used when a place has no tokens.
    const DEFAULT_REF: &'static Self;

    /// Optional type used for the state carried by tokens.
    type State: std::fmt::Debug + Send;

    /// The type used for the weight of arcs.
    type Weight: std::fmt::Debug + Default + Send;

    /// The type used for identifying places.
    type Id: std::fmt::Debug;

    /// Check if the transition can fire given the marking.
    fn is_transition_enabled<A>(transition: &Transition<A, Self>, marking: &Marking<Self>) -> bool;

    /// Update the marking by firing the given transition, iff it is enabled.
    fn update_input<A>(
        transition: &Transition<A, Self>,
        marking: &mut Marking<Self>,
    ) -> Option<Self::State>;

    /// Update the marking by adding tokens from the given transition.
    fn update_output<A>(
        transition: &Transition<A, Self>,
        marking: &mut Marking<Self>,
        state: Self::State,
    );
}

impl Color for usize {
    const DEFAULT_REF: &'static Self = &0;

    type State = ();
    type Weight = UsizeWeight;
    type Id = String;

    fn is_transition_enabled<A>(transition: &Transition<A, Self>, marking: &Marking<Self>) -> bool {
        for arc in &transition.input {
            if marking[arc.target] < arc.weights.0.get() {
                return false;
            }
        }
        true
    }

    fn update_input<A>(
        transition: &Transition<A, Self>,
        marking: &mut Marking<Self>,
    ) -> Option<Self::State> {
        if !Self::is_transition_enabled(transition, marking) {
            return None;
        }
        for arc in &transition.input {
            marking
                .0
                .entry(arc.target)
                .and_modify(|e| *e = e.saturating_sub(arc.weights.0.get()));
        }
        Some(())
    }

    fn update_output<A>(
        transition: &Transition<A, Self>,
        marking: &mut Marking<Self>,
        _state: Self::State,
    ) {
        for arc in &transition.output {
            marking
                .0
                .entry(arc.target)
                .and_modify(|e| *e += arc.weights.0.get())
                .or_insert(arc.weights.0.get());
        }
    }
}

/// Weights for arcs using `usize` tokens which must be non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UsizeWeight(pub std::num::NonZero<usize>);

impl UsizeWeight {
    /// The default weight of 1.
    pub const DEFAULT: Self = UsizeWeight(std::num::NonZero::new(1).unwrap());
}

impl Default for UsizeWeight {
    fn default() -> Self {
        Self::DEFAULT
    }
}
