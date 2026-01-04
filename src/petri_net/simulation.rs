use std::{collections::HashMap, hash::Hash};

use futures::{FutureExt, StreamExt, task::Spawn};

use super::{Color, EnabledTransitions, Entry, Id, Marking, PetriNet, Transition};

pub trait Callable: 'static + Clone + Send + Fn() {}

impl<T> Callable for T where T: 'static + Clone + Send + Fn() {}

/// A backend executor for running tasks.
pub trait Executor {
    /// The handle type for spawned tasks.
    type TaskHandle;

    /// Spawn a new asynchronous task.
    fn spawn_task(
        &self,
        value: impl std::future::Future<Output = ()> + 'static + Send,
    ) -> Self::TaskHandle;

    /// Stop a running task.
    fn stop(&self, task: Self::TaskHandle) -> impl std::future::Future<Output = ()>;
}

impl Executor for futures::executor::LocalSpawner {
    type TaskHandle = ();

    fn spawn_task(
        &self,
        value: impl std::future::Future<Output = ()> + 'static + Send,
    ) -> Self::TaskHandle {
        self.spawn_obj(value.boxed().into()).unwrap();
    }

    async fn stop(&self, _task: Self::TaskHandle) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TaskId(usize);

struct Update<A, C: Color> {
    task_id: TaskId,
    transition_id: Id<Transition<A, C>>,
    new_state: C::State,
}

struct Tasks<E: Executor, A, C: Color> {
    tasks: HashMap<TaskId, E::TaskHandle>,
    sender: futures::channel::mpsc::UnboundedSender<Update<A, C>>,
    executor: E,
}

impl<E: Executor, A, C: Color> Tasks<E, A, C> {
    fn new(executor: E, sender: futures::channel::mpsc::UnboundedSender<Update<A, C>>) -> Self {
        Tasks {
            tasks: HashMap::new(),
            sender,
            executor,
        }
    }

    async fn remove(&mut self, task_id: &TaskId) {
        if let Some(value) = self.tasks.remove(task_id) {
            self.executor.stop(value).await;
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl<E: Executor, A: Callable, C: Color> Tasks<E, A, C> {
    fn spawn(&mut self, transition: Entry<'_, Transition<A, C>>, state: C::State) -> TaskId {
        let task_id = TaskId(self.tasks.len());
        let sender = self.sender.clone();
        let callback = transition.item.action.clone();
        let transition_id = transition.id;
        self.tasks.insert(
            task_id,
            self.executor.spawn_task(async move {
                (callback)();
                if sender
                    .unbounded_send(Update {
                        task_id,
                        transition_id,
                        new_state: state,
                    })
                    .is_err()
                {
                    // The receiver has been dropped
                }
            }),
        );
        task_id
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
pub struct Simulation<E: Executor, S: CompetingStrategy, A, C: Color> {
    petri_net: std::sync::Arc<PetriNet<A, C>>,
    marking: Marking<C>,
    receiver: futures::channel::mpsc::UnboundedReceiver<Update<A, C>>,
    tasks: Tasks<E, A, C>,
    competing_strategy: S,
}

impl<E: Executor, S: CompetingStrategy, A, C: Color> Simulation<E, S, A, C> {
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

impl<E: Executor, S: CompetingStrategy, A: Callable, C: Color> Simulation<E, S, A, C> {
    /// Run the simulation until completion.
    pub async fn run(mut self) -> Marking<C> {
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
        if self.tasks.is_empty() {
            return self.marking;
        }

        while let Some(update) = self.receiver.next().await {
            // Remove the completed task
            self.tasks.remove(&update.task_id).await;

            // Update the marking
            C::update_output(
                &self.petri_net[update.transition_id],
                &mut self.marking,
                update.new_state,
            );

            // Spawn new tasks for enabled transitions
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

            // Exit if there are no more tasks running
            if self.tasks.is_empty() {
                break;
            }
        }

        self.marking
    }
}

#[cfg(test)]
mod tests {
    use crate::{Place, petri_net::UsizeWeight};

    use super::*;

    #[test]
    fn test_simple() {
        let (start_id, end_id);
        let petri_net = std::sync::Arc::new({
            let mut petri_net = PetriNet::default();
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
