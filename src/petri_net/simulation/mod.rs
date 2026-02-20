mod tasks;

use futures::StreamExt;

use self::tasks::{Tasks, Update};
use super::{Color, EnabledTransitions, Entry, Id, Marking, PetriNet, Transition};
use crate::executor::Executor;

/// A action stored for a transition which could be executed asynchronously.
pub trait Callable<S: 'static + Send>: 'static {
    /// Create a future that executes the action with the given state.
    fn create_future(&self, state: S) -> impl std::future::Future<Output = S> + 'static + Send;
}

impl Callable<()> for fn() {
    fn create_future(&self, _state: ()) -> impl std::future::Future<Output = ()> + 'static + Send {
        let func = *self;
        async move { func() }
    }
}

impl<S: 'static + Send> Callable<S> for fn(S) -> S {
    fn create_future(&self, state: S) -> impl std::future::Future<Output = S> + 'static + Send {
        let func = *self;
        async move { func(state) }
    }
}

impl Callable<()> for fn() -> futures::future::BoxFuture<'static, ()> {
    fn create_future(&self, _state: ()) -> impl std::future::Future<Output = ()> + 'static + Send {
        (*self)()
    }
}

impl<S: 'static + Send> Callable<S> for fn(S) -> futures::future::BoxFuture<'static, S> {
    fn create_future(&self, state: S) -> impl std::future::Future<Output = S> + 'static + Send {
        (*self)(state)
    }
}

/// The Strategy for resolving competing transitions.
pub trait CompetingStrategy {
    /// Select one transition from the competing ones. Transition is guaranteed to have at least one element.
    fn select<'a, T>(&self, transitions: Vec<Entry<'a, T>>) -> Entry<'a, T>;
}

/// A strategy that selects a random transition among competing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstCompetingStrategy;

impl CompetingStrategy for FirstCompetingStrategy {
    fn select<'a, T>(&self, mut transitions: Vec<Entry<'a, T>>) -> Entry<'a, T> {
        transitions.pop().unwrap()
    }
}

/// A run of a Petri net simulation.
pub struct Simulation<E: Executor<()>, S: CompetingStrategy, A, C: Color> {
    petri_net: std::sync::Arc<PetriNet<A, C>>,
    marking: Marking<C>,
    receiver: futures::channel::mpsc::UnboundedReceiver<Update<A, C>>,
    tasks: Tasks<E, A, C>,
    competing_strategy: S,
}

impl<E: Executor<()>, S: CompetingStrategy, A, C: Color> Simulation<E, S, A, C> {
    /// Prepare a new simulation for the given Petri net.
    pub fn new(
        executor: E,
        petri_net: std::sync::Arc<PetriNet<A, C>>,
        competing_strategy: S,
    ) -> Self {
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        let marking = petri_net.initial_marking();
        Simulation {
            petri_net,
            marking,
            receiver,
            tasks: Tasks::new(executor, sender),
            competing_strategy,
        }
    }
}

impl<E: Executor<()>, S: CompetingStrategy, A: Callable<C::State>, C: Color>
    Simulation<E, S, A, C>
{
    /// Run all enabled transitions and return whether there are no more tasks running.
    fn run_all_transactions(&mut self) -> bool {
        for enabled_transaction in
            EnabledTransitions::find_all(self.petri_net.as_ref(), self.marking.clone())
        {
            let transition = match enabled_transaction {
                EnabledTransitions::Independent(transition) => transition,
                EnabledTransitions::Competing(transitions) => {
                    self.competing_strategy.select(transitions)
                }
            };

            if let Some(state) = C::update_input(&transition, &mut self.marking) {
                self.tasks.spawn(transition, state);
            }
        }
        self.tasks.is_empty()
    }

    /// Run the simulation until completion.
    pub async fn run(mut self) -> Marking<C> {
        if self.run_all_transactions() {
            return self.marking;
        }

        while let Some(update) = self.receiver.next().await {
            // Update the marking
            update
                .finish(&mut self.tasks, &self.petri_net, &mut self.marking)
                .await;

            // Exit if there are no more tasks running
            if self.run_all_transactions() {
                break;
            }
        }

        self.marking
    }
}

impl<E: Executor<()>, S: CompetingStrategy, A, C: Color> std::ops::Deref
    for Simulation<E, S, A, C>
{
    type Target = Marking<C>;

    fn deref(&self) -> &Self::Target {
        &self.marking
    }
}

impl<E: Executor<()>, S: CompetingStrategy, A, C: Color> std::ops::DerefMut
    for Simulation<E, S, A, C>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.marking
    }
}

#[cfg(test)]
mod tests {
    use crate::{petri_net::Place, petri_net::UsizeWeight};

    use super::*;

    #[test]
    fn test_simple() {
        let (start_id, end_id);
        let petri_net = std::sync::Arc::new({
            let mut petri_net = PetriNet::<fn(), usize>::default();
            let (start, _, end) = petri_net.add_connected_places(
                Place::new("Start", 1),
                Place::new("End", 0),
                || println!("Transition fired"),
            );
            start_id = start;
            end_id = end;
            petri_net
        });
        assert_eq!(petri_net.initial_marking()[start_id], 1);
        assert_eq!(petri_net.initial_marking()[end_id], 0);

        let mut pool = futures::executor::LocalPool::new();
        let simulation = Simulation::new(pool.spawner(), petri_net, FirstCompetingStrategy);
        let marking = pool.run_until(simulation.run());
        assert_eq!(marking[start_id], 0);
        assert_eq!(marking[end_id], 1);
    }

    #[test]
    fn test_concurrent() {
        let (start1, start2, end);
        let petri_net = std::sync::Arc::new({
            let mut petri_net = PetriNet::<fn(), usize>::default();
            start1 = petri_net.add_place(Place::new("Start 1", 1));
            start2 = petri_net.add_place(Place::new("Start 2", 1));
            end = petri_net.add_place(Place::new("End", 0));

            let t1 = petri_net.add_transition(|| println!("Transition 1 fired"));
            let t2 = petri_net.add_transition(|| println!("Transition 2 fired"));

            assert!(petri_net.connect_place(start1, t1, UsizeWeight::DEFAULT));
            assert!(petri_net.connect_place(start2, t2, UsizeWeight::DEFAULT));
            assert!(petri_net.connect_transition(t1, end, UsizeWeight::DEFAULT));
            assert!(petri_net.connect_transition(t2, end, UsizeWeight::DEFAULT));
            petri_net
        });
        assert_eq!(petri_net.initial_marking()[end], 0);

        let mut pool = futures::executor::LocalPool::new();
        let simulation = Simulation::new(pool.spawner(), petri_net, FirstCompetingStrategy);
        let marking = pool.run_until(simulation.run());
        assert_eq!(marking[start1], 0);
        assert_eq!(marking[start2], 0);
        assert_eq!(marking[end], 2);
    }

    #[test]
    fn test_competing() {
        let (start, end);
        let petri_net = std::sync::Arc::new({
            let mut petri_net = PetriNet::<fn(), usize>::default();
            (start, _, end) = petri_net.add_connected_places(
                Place::new("Start", 1),
                Place::new("End", 0),
                || println!("Transition 1 fired"),
            );
            let alternative = petri_net.add_transition(|| println!("Transition 2 fired"));
            assert!(petri_net.connect_place(start, alternative, UsizeWeight::DEFAULT));
            assert!(petri_net.connect_transition(alternative, end, UsizeWeight::DEFAULT));
            petri_net
        });

        let mut pool = futures::executor::LocalPool::new();
        let simulation = Simulation::new(pool.spawner(), petri_net, FirstCompetingStrategy);
        let marking = pool.run_until(simulation.run());

        assert_eq!(marking[start], 0);
        assert_eq!(marking[end], 1);
    }
}
