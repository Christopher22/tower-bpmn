mod instances;
mod registered_process;
mod token;

use serde_json::Value as JsonValue;
use std::{any::TypeId, collections::HashMap, hash::Hash, sync::Arc};

use crate::{ExtendedExecutor, Message, MessageManager, Process, ProcessBuilder, SendError};

pub use instances::{Instance, InstanceId, InstanceStatus, Instances, RuntimeInstance};
pub use registered_process::{RegisteredProcess, RuntimeApiError};
pub use token::{SharedHistory, Token, TokenId, Value};

pub(self) trait Observer {
    fn report_start(&self, instance_id: InstanceId, process: String, history: SharedHistory);
    fn report_task_completed(&self, instance_id: InstanceId, token_id: TokenId, task: &str);
    fn report_end(&self, instance_id: InstanceId);
    fn report_stop(&self, instance_id: InstanceId);
}

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor> {
    pub(crate) executor: E,
    registered_processes: HashMap<String, RegisteredProcess<E>>,
    message_manager: MessageManager,
    /// Currently running instances tracked by the runtime, which can be queried for their status and history.
    pub instances: Instances,
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
            executor,
            registered_processes: HashMap::new(),
            message_manager: MessageManager::new(),
            instances: Instances::new(),
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

        self.registered_processes.insert(process_name, raw_process);
        Ok(())
    }

    /// Return names of all registered processes.
    pub fn registered_processes(&self) -> impl Iterator<Item = &RegisteredProcess<E>> {
        self.registered_processes.values()
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
        process.run_dynamic(self, input)
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
        process.send_message_dynamic(self, correlation_key, payload)
    }

    /// Run a process with the given input. The process will be executed in the background, and an instance handle will be returned for waiting for completion or resuming later.
    /// Actually, this is just a shorthand for sending a message to the process with the input as payload and without message correlation.
    pub fn run<P: Process>(
        &self,
        process: P,
        input: P::Input,
    ) -> Result<Instance<'_, E>, InstanceError> {
        match self
            .registered_processes
            .get(process.metadata().name.as_ref())
        {
            Some(raw_process) if raw_process.process_type == TypeId::of::<P>() => {
                let uuid = InstanceId::new();

                // Prepare the shared storage with an observer for updating the instance status when tasks are completed.
                let instances = self.instances.clone();
                let end_name = raw_process.end_place().name.clone();
                let process_name = raw_process.meta_data.name.to_string();
                let shared_storage = SharedHistory::new().with_observer(Arc::new(
                    move |token: TokenId, place: &str| {
                        if place == super::START_NAME {
                            instances.report_start(
                                uuid,
                                process_name.clone(),
                                SharedHistory::new(),
                            );
                        } else if place == end_name {
                            instances.report_end(uuid);
                        } else {
                            instances.report_task_completed(uuid, token, place);
                        }
                    },
                ));

                let instance =
                    Instance::new(uuid, self, raw_process, input, shared_storage.clone());

                Ok(instance)
            }
            Some(_) => Err(InstanceError::InvalidContext),
            None => Err(InstanceError::Unregistered),
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

/// Errors while creating or resolving an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstanceError {
    /// Not registered
    Unregistered,
    /// Given the context, the process instance has already completed.
    Completed,
    /// The context does not match the process instance.
    InvalidContext,
}
