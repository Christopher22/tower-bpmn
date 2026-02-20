use dashmap::DashMap;
use std::{any::TypeId, collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::{
    ExtendedExecutor, FirstCompetingStrategy, Process, State, Step, Value,
    token::{Token, TokenObserver},
};
use crate::{
    Instance, InstanceError, Message, MessageManager, ProcessBuilder, SendError,
    petri_net::{PetriNet, Simulation},
};

/// A raw process definition, which is used internally for storing the process definition and creating process instances.
/// It contains the PetriNet representation of the process, the start and end places of the process, and the type information of the process and its input.
pub(crate) struct RawProcess {
    pub(crate) name: String,
    pub(crate) petri_net: Arc<PetriNet<Step, State>>,
    pub(crate) start: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) end: crate::petri_net::Id<crate::petri_net::Place<State>>,
    process_type: TypeId,
    input_type: TypeId,
}

impl RawProcess {
    /// Create a new raw process from a PetriNet representation and type information.
    pub fn new<P: Process>(
        name: String,
        petri_net: Arc<PetriNet<Step, State>>,
        start_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
        current_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
    ) -> Self {
        Self {
            name,
            petri_net,
            start: start_place,
            end: current_place,
            process_type: TypeId::of::<P>(),
            input_type: TypeId::of::<P::Input>(),
        }
    }

    /// Create a new simulation for this process with the given input. The simulation will be initialized with the input token at the start place of the process.
    pub fn instantiate<E: ExtendedExecutor, V: Value>(
        &self,
        executor: E,
        input: V,
        observer: Option<TokenObserver>,
    ) -> Simulation<E, FirstCompetingStrategy, Step, State> {
        assert_eq!(
            self.input_type,
            TypeId::of::<V>(),
            "The input type does not match the expected type for this process"
        );
        let token = match observer {
            Some(observer) => Token::new()
                .with_observer(observer)
                .set_output("Start", input),
            None => Token::new().set_output("Start", input),
        };
        let mut simulation =
            Simulation::new(executor, self.petri_net.clone(), FirstCompetingStrategy);
        simulation[self.start] = State::Completed(token);
        simulation
    }
}

impl<P: Process, E: Value> TryFrom<ProcessBuilder<P, E>> for RawProcess {
    type Error = ProcessError;
    fn try_from(mut builder: ProcessBuilder<P, E>) -> Result<Self, Self::Error> {
        let mut petri_net = PetriNet::default();
        match Arc::get_mut(&mut builder.petri_net) {
            Some(inner_petri_net) => std::mem::swap(&mut petri_net, inner_petri_net.get_mut()),
            None => return Err(ProcessError::DanglingProcessPart),
        }

        Ok(Self::new::<P>(
            std::any::type_name::<P>().to_string(),
            Arc::new(petri_net),
            builder.start_place,
            builder.current_place,
        ))
    }
}

/// Status of a runtime-tracked process instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeInstanceStatus {
    /// Instance is currently executing.
    Running,
    /// Instance finished successfully.
    Completed,
    /// Instance was explicitly stopped.
    Stopped,
}

/// Public snapshot of a process instance tracked by the runtime.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RuntimeInstance {
    /// Instance identifier.
    pub id: Uuid,
    /// BPMN process name.
    pub process: String,
    /// Current execution status.
    pub status: RuntimeInstanceStatus,
    /// Active place names for this instance.
    pub current_places: Vec<String>,
    /// Most recent task names observed in active tokens.
    pub current_tasks: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeInstanceEntry {
    pub(crate) process: String,
    pub(crate) status: RuntimeInstanceStatus,
    pub(crate) current_places: Vec<String>,
    pub(crate) current_tasks: Vec<String>,
}

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor> {
    pub(crate) executor: E,
    registered_processes: HashMap<String, RawProcess>,
    message_manager: MessageManager,
    pub(crate) instances: Arc<DashMap<Uuid, RuntimeInstanceEntry>>,
}

impl<E: ExtendedExecutor> std::fmt::Debug for Runtime<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("registered_processes", &self.registered_processes.len())
            .finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor + 'static> Runtime<E> {
    /// Creates a new runtime with the provided executor backend.
    pub fn new(executor: E) -> Self {
        Runtime {
            executor,
            registered_processes: HashMap::new(),
            message_manager: MessageManager::new(),
            instances: Arc::new(DashMap::new()),
        }
    }

    /// Registers a process definition in the runtime.
    pub fn register_process<P: Process>(&mut self, process: P) -> Result<(), ProcessError> {
        let name = process.name();
        let mut raw_process = {
            let builder = ProcessBuilder::new(self.message_manager.clone());
            RawProcess::try_from(process.define(builder))?
        };
        raw_process.name = name.to_string();
        self.registered_processes
            .insert(name.to_string(), raw_process);
        Ok(())
    }

    /// Return names of all registered processes.
    pub fn registered_processes(&self) -> Vec<String> {
        self.registered_processes.keys().cloned().collect()
    }

    /// Return all currently tracked instances.
    pub fn instances(&self) -> Vec<RuntimeInstance> {
        self.instances
            .iter()
            .map(|entry| RuntimeInstance {
                id: *entry.key(),
                process: entry.value().process.clone(),
                status: entry.value().status,
                current_places: entry.value().current_places.clone(),
                current_tasks: entry.value().current_tasks.clone(),
            })
            .collect()
    }

    /// Return all instances that are still running.
    pub fn running_instances(&self) -> Vec<RuntimeInstance> {
        self.instances()
            .into_iter()
            .filter(|instance| instance.status == RuntimeInstanceStatus::Running)
            .collect()
    }

    pub(crate) fn upsert_instance(&self, id: Uuid, entry: RuntimeInstanceEntry) {
        self.instances.insert(id, entry);
    }

    pub(crate) fn update_instance(
        &self,
        id: Uuid,
        status: RuntimeInstanceStatus,
        current_places: Vec<String>,
        current_tasks: Vec<String>,
    ) {
        if let Some(mut instance) = self.instances.get_mut(&id) {
            instance.status = status;
            instance.current_places = current_places;
            instance.current_tasks = current_tasks;
        }
    }

    /// Run a process with the given input. The process will be executed in the background, and an instance handle will be returned for waiting for completion or resuming later.
    /// Actually, this is just a shorthand for sending a message to the process with the input as payload and without message correlation.
    pub fn run<P: Process>(
        &self,
        process: P,
        input: P::Input,
    ) -> Result<Instance<'_, E>, InstanceError> {
        match self.registered_processes.get(process.name()) {
            Some(raw_process) if raw_process.process_type == TypeId::of::<P>() => {
                Ok(Instance::new(self, raw_process, input))
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
