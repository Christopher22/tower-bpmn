mod dynamic;
mod instance;
mod instances;
mod registered_process;
mod storage;
mod token;

use serde_json::Value as JsonValue;
use std::{collections::HashMap, sync::Arc};

use super::messages::{Context, Message, MessageBroker, MessageError};
use super::{ExtendedExecutor, Process, ProcessBuilder, ProcessName};

pub use dynamic::{DynamicInput, DynamicValue};
pub use instance::{Handle, Instance, InstanceId, InstanceNotRunning, InstanceStatus};
pub use instances::{InstanceSpawnError, Instances};
pub use registered_process::RegisteredProcess;
pub use storage::{
    InMemory, InMemoryStorage, ResumableProcess, ResumeError, Sqlite, SqliteError, SqliteStorage,
    Storage, StorageBackend,
};
pub use token::{Token, TokenId, Value};

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    /// Message broker for inter-process communication.
    pub messages: MessageBroker,
    registered_processes: HashMap<ProcessName, Arc<Instances<E, B>>>,
    executor: E,
    storage_backend: B,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Runtime<E, B> {
    /// Creates a new runtime with the provided executor backend.
    pub fn new(executor: E, storage_backend: B) -> Self {
        Runtime {
            registered_processes: HashMap::new(),
            messages: MessageBroker::new(),
            executor,
            storage_backend,
        }
    }

    /// Registers a process definition in the runtime.
    pub fn register_process<P: Process + Clone + Send + Sync>(
        &mut self,
        process: P,
    ) -> Result<(), ProcessError> {
        let metadata = process.metadata().clone();
        let process_name = ProcessName::from(&metadata);

        let raw_process = {
            let builder = ProcessBuilder::new(metadata, self.messages.clone());
            RegisteredProcess::try_from(process.define(builder))?
        };

        let instances = Arc::new(Instances::new(
            raw_process,
            self.executor.clone(),
            self.storage_backend.clone(),
        ));
        self.messages.register_spawn(&process, instances.clone());
        self.registered_processes.insert(process_name, instances);

        Ok(())
    }

    /// Resume all unfinished instances provided by the storage backend, returning the IDs of the resumed instances.
    pub fn resume_unfinished_instances(&self) -> Result<Vec<InstanceId>, ResumeError> {
        self.storage_backend
            .unfinished_instances()
            .into_iter()
            .map(
                |(process_name, instance_id)| match self.registered_processes.get(&process_name) {
                    Some(instances) => instances.resume(instance_id),
                    None => Err(ResumeError::NotFound),
                },
            )
            .collect()
    }

    /// Return all registered processes.
    pub fn registered_processes(&self) -> impl Iterator<Item = &RegisteredProcess<B>> {
        self.registered_processes
            .values()
            .map(|value| &value.registered_process)
    }

    /// Query a registered process by its name. Returns None if the process is not found.
    pub fn get_registered_process(&self, name: &ProcessName) -> Option<&RegisteredProcess<B>> {
        self.registered_processes
            .get(name)
            .map(|instances| &instances.registered_process)
    }

    /// Query the instances of a registered process by its name. Returns None if the process is not found.
    pub fn get_instances(&self, process_name: &ProcessName) -> Option<&Instances<E, B>> {
        self.registered_processes
            .get(process_name)
            .map(|instances| instances.as_ref())
    }

    /// Wait for a specific instance to complete and return the final context. Returns None if the instance is not found, or Some(Err) if the instance is not running.
    pub async fn wait_for_completion<P: Process>(
        &self,
        process: &P,
        instance_id: InstanceId,
    ) -> Option<Result<Token<B::Storage>, InstanceNotRunning>> {
        match self
            .registered_processes
            .values()
            .find(|instances| instances.registered_process.matches(process))
        {
            Some(instances) => instances.wait_for_completion(instance_id).await,
            None => None,
        }
    }

    /// Starts a registered process by its name using JSON input.
    pub fn run_dynamic(
        &self,
        process_name: ProcessName,
        input: JsonValue,
    ) -> Result<InstanceId, InstanceSpawnError> {
        self.run_dynamic_with_context(process_name, input, Context::default())
    }

    /// Starts a registered process by its name using JSON input and explicit message context.
    pub fn run_dynamic_with_context(
        &self,
        process_name: ProcessName,
        input: JsonValue,
        context: Context,
    ) -> Result<InstanceId, InstanceSpawnError> {
        let mut message = Message::for_dynamic_starting(process_name, input);
        message.context = context;
        match self.messages.send(message) {
            Ok(spawned_process) => spawned_process,
            Err(MessageError::NoTarget) => Err(InstanceSpawnError::Unregistered),
            Err(error) => {
                panic!("Unexpected error while starting process: {error}")
            }
        }
    }

    /// Run a process with the given input. The process will be executed in the background, and an instance handle will be returned for waiting for completion or resuming later.
    pub fn run<P: Process>(
        &self,
        process: P,
        input: P::Input,
    ) -> Result<InstanceId, InstanceSpawnError> {
        match self
            .registered_processes
            .get(&ProcessName::from(process.metadata()))
        {
            Some(registered_process) => Ok(registered_process.run(input)),
            None => Err(InstanceSpawnError::Unregistered),
        }
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for Runtime<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("registered_processes", &self.registered_processes.len())
            .finish_non_exhaustive()
    }
}

impl<E: Default + ExtendedExecutor<B::Storage>, B: Default + StorageBackend> Default
    for Runtime<E, B>
{
    fn default() -> Self {
        Self::new(E::default(), B::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors while registering a process definition.
pub enum ProcessError {
    /// A split builder branch escaped and prevented process finalization.
    DanglingProcessPart,
}
