use crate::petri_net;

use super::{Callable, Color, Entry, Executor, Id, PetriNet, Transition};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TaskId(usize);

/// An update message sent when a task is completed.
pub struct Update<A, C: Color> {
    task_id: TaskId,
    pub transition_id: Id<Transition<A, C>>,
    new_state: C::State,
}

impl<A, C: Color> Update<A, C> {
    /// Finish processing the update the marking.
    pub async fn finish<E: Executor>(
        self,
        tasks: &mut Tasks<E, A, C>,
        petri_net: &PetriNet<A, C>,
        marking: &mut petri_net::Marking<C>,
    ) {
        // Remove the completed task
        tasks.remove(&self.task_id).await;
        C::update_output(&petri_net[self.transition_id], marking, self.new_state);
    }
}

pub struct Tasks<E: Executor, A, C: Color> {
    tasks: HashMap<TaskId, E::TaskHandle>,
    sender: futures::channel::mpsc::UnboundedSender<Update<A, C>>,
    executor: E,
}

impl<E: Executor, A, C: Color> Tasks<E, A, C> {
    pub fn new(executor: E, sender: futures::channel::mpsc::UnboundedSender<Update<A, C>>) -> Self {
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

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl<E: Executor, A: Callable<C::State>, C: Color> Tasks<E, A, C> {
    pub fn spawn(&mut self, transition: Entry<'_, Transition<A, C>>, state: C::State) {
        let task_id = TaskId(self.tasks.len());
        let sender = self.sender.clone();
        let callback = transition.item.action.create_future(state);
        let transition_id = transition.id;
        self.tasks.insert(
            task_id,
            self.executor.spawn_task(async move {
                let state = callback.await;
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
    }
}
