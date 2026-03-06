mod token;

use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::{any::TypeId, collections::HashMap, hash::Hash, sync::Arc};
use uuid::Uuid;

use crate::{
    ExtendedExecutor, Message, MessageManager, MetaData, Process, ProcessBuilder, SendError, State,
    Step,
    petri_net::{FirstCompetingStrategy, PetriNet, Simulation},
};

pub use token::{SharedHistory, Token, TokenId, Value};

/// A raw process definition, which is used internally for storing the process definition and creating process instances.
/// It contains the PetriNet representation of the process, the start and end places of the process, and the type information of the process and its input.
struct RawProcess<E: ExtendedExecutor> {
    pub(crate) meta_data: MetaData,
    pub(crate) petri_net: Arc<PetriNet<Step, State>>,
    pub(crate) start: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) end: crate::petri_net::Id<crate::petri_net::Place<State>>,
    process_type: TypeId,
    input_type: TypeId,
    dynamic_api: DynamicCaller<E>,
}

impl<E: ExtendedExecutor> RawProcess<E> {
    /// Create a new raw process from a PetriNet representation and type information.
    pub fn new<P: Process>(
        meta_data: MetaData,
        petri_net: Arc<PetriNet<Step, State>>,
        start_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
        current_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
        dynamic_api: DynamicCaller<E>,
    ) -> Self {
        Self {
            meta_data,
            petri_net,
            start: start_place,
            end: current_place,
            process_type: TypeId::of::<P>(),
            input_type: TypeId::of::<P::Input>(),
            dynamic_api,
        }
    }

    /// Create a new simulation for this process with the given input. The simulation will be initialized with the input token at the start place of the process.
    pub fn instantiate<A: ExtendedExecutor, V: Value>(
        &self,
        executor: A,
        input: V,
        shared_storage: SharedHistory,
    ) -> Simulation<A, FirstCompetingStrategy, Step, State> {
        assert_eq!(
            self.input_type,
            TypeId::of::<V>(),
            "The input type does not match the expected type for this process"
        );
        let token = Token::new(shared_storage).set_output(super::START_NAME, input);
        let mut simulation =
            Simulation::new(executor, self.petri_net.clone(), FirstCompetingStrategy);
        simulation[self.start] = State::Completed(token);
        simulation
    }

    /// Get the end place of the process, which is used for querying the final state of the process instance.
    pub fn end_place(&self) -> &crate::petri_net::Place<State> {
        &self.petri_net[self.end]
    }

    /// Start the process by its name.
    pub fn run_dynamic(
        &self,
        runtime: &Runtime<E>,
        input: JsonValue,
    ) -> Result<InstanceId, RuntimeApiError> {
        (self.dynamic_api.start)(runtime, input)
    }

    /// Send a dynamic message.
    pub fn send_message_dynamic(
        &self,
        runtime: &Runtime<E>,
        correlation_key: super::CorrelationKey,
        payload: JsonValue,
    ) -> Result<(), RuntimeApiError> {
        (self.dynamic_api.send_message)(runtime, correlation_key, payload)
    }
}

impl<P, V, E> TryFrom<(ProcessBuilder<P, V>, DynamicCaller<E>)> for RawProcess<E>
where
    P: Process,
    V: Value,
    E: ExtendedExecutor,
{
    type Error = ProcessError;
    fn try_from(
        (mut builder, dynamic_api): (ProcessBuilder<P, V>, DynamicCaller<E>),
    ) -> Result<Self, Self::Error> {
        let mut petri_net = PetriNet::default();
        match Arc::get_mut(&mut builder.petri_net) {
            Some(inner_petri_net) => std::mem::swap(&mut petri_net, inner_petri_net.get_mut()),
            None => return Err(ProcessError::DanglingProcessPart),
        }

        Ok(Self::new::<P>(
            builder.meta_data,
            Arc::new(petri_net),
            builder.start_place,
            builder.current_place,
            dynamic_api,
        ))
    }
}

/// The ID of a BPMN process instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct InstanceId(Uuid);

impl InstanceId {
    /// Create a new instance ID.
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for InstanceId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Status of a runtime-tracked process instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    /// Instance is currently executing.
    Running,
    /// Instance finished successfully.
    Completed,
    /// Instance was explicitly stopped.
    Stopped,
}

/// Public snapshot of a process instance tracked by the runtime.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeInstance {
    /// Instance identifier.
    pub id: InstanceId,
    /// BPMN process name.
    pub process: String,
    /// Current execution status.
    pub status: InstanceStatus,
    #[serde(skip_serializing)]
    #[allow(unused)]
    history: SharedHistory,
    #[serde(skip_serializing)]
    last_tasks: HashMap<TokenId, String>,
}

pub trait Observer {
    fn report_start(&self, instance_id: InstanceId, process: String, history: SharedHistory);
    fn report_task_completed(&self, instance_id: InstanceId, token_id: TokenId, task: &str);
    fn report_end(&self, instance_id: InstanceId);
    fn report_stop(&self, instance_id: InstanceId);
}

#[derive(Debug, Clone)]
pub struct Instances(Arc<DashMap<InstanceId, RuntimeInstance>>);

impl Instances {
    /// Create a new instance object.
    pub fn new() -> Self {
        Instances(Arc::new(DashMap::new()))
    }

    /// Returns an iterator over tracked process instances.
    pub fn iter(&self) -> impl Iterator<Item = impl std::ops::Deref<Target = RuntimeInstance>> {
        self.0.iter()
    }

    /// Returns one tracked instance by id.
    pub fn get(&self, id: InstanceId) -> Option<impl std::ops::Deref<Target = RuntimeInstance>> {
        self.0.get(&id)
    }
}

impl Observer for Instances {
    fn report_start(&self, instance_id: InstanceId, process: String, history: SharedHistory) {
        self.0.insert(
            instance_id,
            RuntimeInstance {
                id: instance_id,
                process: process.to_string(),
                status: InstanceStatus::Running,
                history,
                last_tasks: HashMap::new(),
            },
        );
    }

    fn report_task_completed(&self, instance_id: InstanceId, token_id: TokenId, task: &str) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => entry
                .get_mut()
                .last_tasks
                .insert(token_id, task.to_string()),
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received task completion report for unknown instance {instance_id:?}");
                return;
            }
        };
    }

    fn report_end(&self, instance_id: InstanceId) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().status = InstanceStatus::Completed
            }
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received end report for unknown instance {instance_id:?}");
                return;
            }
        };
    }

    fn report_stop(&self, instance_id: InstanceId) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => entry.get_mut().status = InstanceStatus::Stopped,
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received stop report for unknown instance {instance_id:?}");
                return;
            }
        };
    }
}

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor> {
    pub(crate) executor: E,
    registered_processes: HashMap<String, RawProcess<E>>,
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
        let dynamic_api = DynamicCaller {
            start: Arc::new(move |runtime: &Runtime<E>, input: JsonValue| {
                let value: P::Input = serde_json::from_value(input)
                    .map_err(|err| RuntimeApiError::InvalidPayload(err.to_string()))?;
                runtime
                    .run(process_for_start.clone(), value)
                    .map(|instance| instance.id)
                    .map_err(RuntimeApiError::Instance)
            }),
            send_message: Arc::new(
                move |runtime: &Runtime<E>,
                      correlation_key: super::CorrelationKey,
                      payload: JsonValue| {
                    let value: P::Input = serde_json::from_value(payload)
                        .map_err(|err| RuntimeApiError::InvalidPayload(err.to_string()))?;
                    runtime
                        .send_message(Message {
                            process: process_for_message.clone(),
                            payload: value,
                            correlation_key,
                        })
                        .map_err(RuntimeApiError::Send)
                },
            ),
        };

        let raw_process = {
            let builder = ProcessBuilder::new(metadata, self.message_manager.clone());
            RawProcess::try_from((process.define(builder), dynamic_api))?
        };

        self.registered_processes.insert(process_name, raw_process);
        Ok(())
    }

    /// Return names of all registered processes.
    pub fn registered_processes(&self) -> Vec<String> {
        self.registered_processes.keys().cloned().collect()
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

type ApiStart<E> =
    Arc<dyn Fn(&Runtime<E>, JsonValue) -> Result<InstanceId, RuntimeApiError> + Send + Sync>;
type ApiSendMessage<E> = Arc<
    dyn Fn(&Runtime<E>, super::CorrelationKey, JsonValue) -> Result<(), RuntimeApiError>
        + Send
        + Sync,
>;

struct DynamicCaller<E: ExtendedExecutor> {
    start: ApiStart<E>,
    send_message: ApiSendMessage<E>,
}

/// Errors emitted by runtime API adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeApiError {
    /// Process is unknown.
    Unregistered,
    /// JSON payload could not be deserialized.
    InvalidPayload(String),
    /// Instance startup failed.
    Instance(InstanceError),
    /// Message dispatch failed.
    Send(SendError),
}

/// A instance of a process running in the background.
pub struct Instance<'a, A: ExtendedExecutor> {
    /// The unique identifier of this process instance.
    pub id: InstanceId,
    engine: &'a Runtime<A>,
    process: &'a RawProcess<A>,
    simulation: <A as super::Executor<crate::petri_net::Marking<State>>>::TaskHandle,
}

impl<'a, A: ExtendedExecutor> std::fmt::Debug for Instance<'a, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("uuid", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a, A: ExtendedExecutor + 'static> Instance<'a, A> {
    fn new<V: Value>(
        uuid: InstanceId,
        engine: &'a Runtime<A>,
        process_raw: &'a RawProcess<A>,
        input: V,
        shared_history: SharedHistory,
    ) -> Self {
        // Create the process simulation and spawn it in the executor.
        let simulation = engine.executor.spawn_task(
            process_raw
                .instantiate(engine.executor.clone(), input, shared_history)
                .run(),
        );

        Self {
            engine,
            process: process_raw,
            id: uuid,
            simulation,
        }
    }

    /// Wait for the process instance to complete and return the final context. The context can be used to query the final state of the process.
    pub async fn wait_for_completion(self) -> Token {
        let instance_id = self.id;
        let mut marking: crate::petri_net::Marking<State> =
            <A as super::Executor<crate::petri_net::Marking<State>>>::join(
                &self.engine.executor,
                self.simulation,
            )
            .await
            .expect("process simulation failed to complete");
        match std::mem::take(&mut marking[self.process.end]) {
            State::Completed(token) => {
                self.engine.instances.report_end(instance_id);
                token
            }
            _ => panic!("process completed without token at the end place"),
        }
    }

    /// Stop the process instance and return the context for resuming later.
    pub fn stop(self) {
        self.engine.instances.report_stop(self.id);
        futures::executor::block_on(async {
            <A as super::Executor<crate::petri_net::Marking<State>>>::stop(
                &self.engine.executor,
                self.simulation,
            )
            .await;
        });
    }
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
