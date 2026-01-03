use std::{collections::HashMap, hash::Hash};

use futures::{FutureExt, StreamExt, task::Spawn};

use super::{EnabledTransitions, Entry, Id, Marking, PetriNet, Transition};

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

struct Update {
    task_id: TaskId,
    transition_id: Id<Transition>,
}

struct Tasks<E: Executor> {
    tasks: HashMap<TaskId, E::TaskHandle>,
    sender: futures::channel::mpsc::UnboundedSender<Update>,
    executor: E,
}

impl<E: Executor> Tasks<E> {
    fn new(executor: E, sender: futures::channel::mpsc::UnboundedSender<Update>) -> Self {
        Tasks {
            tasks: HashMap::new(),
            sender,
            executor,
        }
    }

    fn spawn(&mut self, transition: Entry<'_, Transition>) -> TaskId {
        let task_id = TaskId(self.tasks.len());
        let sender = self.sender.clone();
        let callback = transition.item.action;
        let transition_id = transition.id;
        self.tasks.insert(
            task_id,
            self.executor.spawn_task(async move {
                (callback)();
                if sender
                    .unbounded_send(Update {
                        task_id,
                        transition_id,
                    })
                    .is_err()
                {
                    // The receiver has been dropped
                }
            }),
        );
        task_id
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

/// The Strategy for resolving competing transitions.
pub trait CompetingStrategy {
    /// Select one transition from the competing ones. Transition is guaranteed to have at least one element.
    fn select<'a, T: Clone>(&self, transitions: Vec<Entry<'a, T>>) -> Entry<'a, T>;
}

/// A strategy that selects a random transition among competing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstCompetingStrategy;

impl CompetingStrategy for FirstCompetingStrategy {
    fn select<'a, T: Clone>(&self, mut transitions: Vec<Entry<'a, T>>) -> Entry<'a, T> {
        transitions.pop().unwrap()
    }
}

/// A run of a Petri net simulation.
pub struct Simulation<E: Executor, C: CompetingStrategy> {
    petri_net: std::sync::Arc<PetriNet>,
    marking: Marking,
    receiver: futures::channel::mpsc::UnboundedReceiver<Update>,
    tasks: Tasks<E>,
    competing_strategy: C,
}

impl<E: Executor, C: CompetingStrategy> Simulation<E, C> {
    /// Prepare a new simulation for the given Petri net.
    pub fn new(executor: E, petri_net: std::sync::Arc<PetriNet>, competing_strategy: C) -> Self {
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

    /// Run the simulation until completion.
    pub async fn run(mut self) -> Marking {
        for enabled_transaction in
            EnabledTransitions::find_all(self.petri_net.as_ref(), self.marking.clone())
        {
            let transition = match enabled_transaction {
                EnabledTransitions::Independent(transition) => transition,
                EnabledTransitions::Competing(transitions) => {
                    self.competing_strategy.select(transitions)
                }
            };
            if self.marking.update_input(&transition.item) {
                self.tasks.spawn(transition);
            }
        }
        if self.tasks.is_empty() {
            return self.marking;
        }

        while let Some(update) = self.receiver.next().await {
            // Remove the completed task
            self.tasks.remove(&update.task_id).await;

            // Update the marking
            self.marking
                .update_output(&self.petri_net[update.transition_id]);

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
                if self.marking.update_input(&transition.item) {
                    self.tasks.spawn(transition);
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
    use crate::Place;

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
            let mut petri_net = PetriNet::default();
            start1 = petri_net.add_place(Place::new("Start 1", 1));
            start2 = petri_net.add_place(Place::new("Start 2", 1));
            end = petri_net.add_place(Place::new("End", 0));

            let t1 = petri_net.add_transition(|| println!("Transition 1 fired"));
            let t2 = petri_net.add_transition(|| println!("Transition 2 fired"));

            assert!(petri_net.connect_place(start1, t1, PetriNet::DEFAULT_WEIGHT));
            assert!(petri_net.connect_place(start2, t2, PetriNet::DEFAULT_WEIGHT));
            assert!(petri_net.connect_transition(t1, end, PetriNet::DEFAULT_WEIGHT));
            assert!(petri_net.connect_transition(t2, end, PetriNet::DEFAULT_WEIGHT));
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
            let mut petri_net = PetriNet::default();
            (start, _, end) = petri_net.add_connected_places(
                Place::new("Start", 1),
                Place::new("End", 0),
                || println!("Transition 1 fired"),
            );
            let alternative = petri_net.add_transition(|| println!("Transition 2 fired"));
            assert!(petri_net.connect_place(start, alternative, PetriNet::DEFAULT_WEIGHT));
            assert!(petri_net.connect_transition(alternative, end, PetriNet::DEFAULT_WEIGHT));
            petri_net
        });

        let mut pool = futures::executor::LocalPool::new();
        let simulation = Simulation::new(pool.spawner(), petri_net, FirstCompetingStrategy);
        let marking = pool.run_until(simulation.run());

        assert_eq!(marking[start], 0);
        assert_eq!(marking[end], 1);
    }
}
