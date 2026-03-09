mod instance;
mod instances;
mod registered_process;
mod token;

use serde_json::Value as JsonValue;
use std::{any::TypeId, collections::HashMap};

use crate::{ExtendedExecutor, Message, MessageManager, Process, ProcessBuilder, SendError};

pub use instance::{Handle, Instance, InstanceId, InstanceNotRunning, InstanceStatus};
pub use instances::{InstanceSpawnError, Instances};
pub use registered_process::{RegisteredProcess, RuntimeApiError};
pub use token::{SharedHistory, Token, TokenId, Value};

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor> {
    registered_processes: HashMap<String, Instances<E>>,
    message_manager: MessageManager,
    executor: E,
}

impl<E: ExtendedExecutor> std::fmt::Debug for Runtime<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("registered_processes", &self.registered_processes.len())
            .finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor> Runtime<E> {
    /// Creates a new runtime with the provided executor backend.
    pub fn new(executor: E) -> Self {
        Runtime {
            registered_processes: HashMap::new(),
            message_manager: MessageManager::new(),
            executor,
        }
    }

    /// Registers a process definition in the runtime.
    pub fn register_process<P: Process + Clone + Send + Sync>(
        &mut self,
        process: P,
    ) -> Result<(), ProcessError> {
        let metadata = process.metadata().clone();
        let process_name = metadata.name.to_string();
        // Prepare dynamic dispatch
        let process_for_start = process.clone();
        let process_for_message = process.clone();
        let dynamic_api =
            registered_process::DynamicCaller::new(process_for_start, process_for_message);

        let raw_process = {
            let builder = ProcessBuilder::new(metadata, self.message_manager.clone());
            RegisteredProcess::try_from((process.define(builder), dynamic_api))?
        };

        self.registered_processes.insert(
            process_name,
            Instances::new(raw_process, self.executor.clone()),
        );
        Ok(())
    }

    /// Return all registered processes.
    pub fn registered_processes(&self) -> impl Iterator<Item = &RegisteredProcess<E>> {
        self.registered_processes
            .values()
            .map(|value| &value.registered_process)
    }

    /// Return all tracked process instances.
    pub fn instances(&self) -> impl Iterator<Item = &Instances<E>> {
        self.registered_processes.values()
    }

    /// Returns the places where the specified instance currently has token branches.
    pub fn instance_places(&self, instance_id: InstanceId) -> Option<Vec<String>> {
        self.instances().find_map(|instances| {
            instances
                .get(instance_id)
                .map(|instance| instance.current_places())
        })
    }

    /// Wait for a specific instance to complete and return the final context. Returns None if the instance is not found, or Some(Err) if the instance is not running.
    pub async fn wait_for_completion<P: Process>(
        &self,
        process: &P,
        instance_id: InstanceId,
    ) -> Option<Result<Token, InstanceNotRunning>> {
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
        process_name: &str,
        input: JsonValue,
    ) -> Result<InstanceId, RuntimeApiError> {
        let process = self
            .registered_processes
            .get(process_name)
            .ok_or(RuntimeApiError::Unregistered)?;
        process.registered_process.run_dynamic(self, input)
    }

    /// Sends a message to a registered process by name using JSON payload.
    pub fn send_message_dynamic(
        &self,
        process_name: &str,
        correlation_key: super::CorrelationKey,
        payload: JsonValue,
    ) -> Result<(), RuntimeApiError> {
        let process = self
            .registered_processes
            .get(process_name)
            .ok_or(RuntimeApiError::Unregistered)?;
        process
            .registered_process
            .send_message_dynamic(self, correlation_key, payload)
    }

    /// Run a process with the given input. The process will be executed in the background, and an instance handle will be returned for waiting for completion or resuming later.
    /// Actually, this is just a shorthand for sending a message to the process with the input as payload and without message correlation.
    pub fn run<P: Process>(
        &self,
        process: P,
        input: P::Input,
    ) -> Result<InstanceId, InstanceSpawnError> {
        match self
            .registered_processes
            .get(process.metadata().name.as_ref())
        {
            Some(registered_process)
                if registered_process.registered_process.process_type == TypeId::of::<P>() =>
            {
                Ok(registered_process.run(input))
            }
            Some(_) => Err(InstanceSpawnError::InvalidContext),
            None => Err(InstanceSpawnError::Unregistered),
        }
    }

    /// Send a message to a process as defined in BPMN.
    /// If there is a process waiting for the message, the message will be delivered to the process and the process will be resumed if it is waiting.
    /// If the type and name matches the start event of a process, a new instance of the process will be started with the message payload as input.
    pub fn send_message<P: Process, V: Value>(
        &self,
        message: Message<P, V>,
    ) -> Result<(), SendError> {
        self.message_manager.send_message(message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Errors while registering a process definition.
pub enum ProcessError {
    /// A split builder branch escaped and prevented process finalization.
    DanglingProcessPart,
}
