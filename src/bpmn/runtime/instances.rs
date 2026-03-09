use dashmap::DashMap;

use crate::{ExtendedExecutor, RegisteredProcess, Value};

use super::{Instance, InstanceId};

/// A collection of process instances for a specific registered process definition.
#[derive(Debug)]
pub struct Instances<E: ExtendedExecutor> {
    /// The registered process definition for this set of instances.
    pub registered_process: RegisteredProcess<E>,
    instances: DashMap<InstanceId, Instance<E>>,
    executor: E,
}

impl<E: ExtendedExecutor> Instances<E> {
    /// Create a new instance object.
    pub fn new(registered_process: RegisteredProcess<E>, executor: E) -> Self {
        Instances {
            registered_process,
            instances: DashMap::new(),
            executor,
        }
    }

    /// Returns an iterator over tracked process instances.
    pub fn iter(&self) -> impl Iterator<Item = impl std::ops::Deref<Target = Instance<E>>> {
        self.instances.iter()
    }

    /// Returns one tracked instance by id.
    pub fn get(&self, id: InstanceId) -> Option<impl std::ops::Deref<Target = Instance<E>>> {
        self.instances.get(&id)
    }

    /// Try to wait for a specific instance to complete and return the final context. Returns None if the instance is not found, or Some(Err) if the instance is not running.
    pub async fn wait_for_completion(
        &self,
        id: InstanceId,
    ) -> Option<Result<super::Token, super::InstanceNotRunning>> {
        match self.instances.get_mut(&id) {
            Some(mut instance) => Some(instance.wait_for_completion().await),
            None => None,
        }
    }

    /// Run a new instance. Used internally by the runtime.
    /// This should not be called directly, because the value is not checked and will panic if not match the registered process.
    pub(super) fn run<V: Value>(&self, input: V) -> InstanceId {
        let instance = Instance::new(&self.registered_process, self.executor.clone(), input);
        let id = instance.id;
        self.instances.insert(id, instance);
        id
    }
}

/// Errors while creating or resolving an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstanceSpawnError {
    /// Not registered
    Unregistered,
    /// Given the context, the process instance has already completed.
    Completed,
    /// The context does not match the process instance.
    InvalidContext,
}
