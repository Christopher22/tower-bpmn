use std::{any::TypeId, collections::HashMap, sync::Arc};

use super::{ExtendedExecutor, FirstCompetingStrategy, Process, State, Step, Value, token::Token};
use crate::{
    Instance, InstanceError, Message, MessageManager, ProcessBuilder, SendError,
    petri_net::{PetriNet, Simulation},
};

/// A raw process definition, which is used internally for storing the process definition and creating process instances.
/// It contains the PetriNet representation of the process, the start and end places of the process, and the type information of the process and its input.
pub(crate) struct RawProcess {
    pub(crate) petri_net: Arc<PetriNet<Step, State>>,
    pub(crate) start: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) end: crate::petri_net::Id<crate::petri_net::Place<State>>,
    process_type: TypeId,
    input_type: TypeId,
}

impl RawProcess {
    /// Create a new raw process from a PetriNet representation and type information.
    pub fn new<P: Process>(
        petri_net: Arc<PetriNet<Step, State>>,
        start_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
        current_place: crate::petri_net::Id<crate::petri_net::Place<State>>,
    ) -> Self {
        Self {
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
    ) -> Simulation<E, FirstCompetingStrategy, Step, State> {
        assert_eq!(
            self.input_type,
            TypeId::of::<V>(),
            "The input type does not match the expected type for this process"
        );
        let token = Token::new().set_output("Start", input);
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
            Arc::new(petri_net),
            builder.start_place,
            builder.current_place,
        ))
    }
}

/// Runtime that stores process definitions and starts process instances.
pub struct Runtime<E: ExtendedExecutor> {
    pub(crate) executor: E,
    registered_processes: HashMap<String, RawProcess>,
    message_manager: MessageManager,
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
        }
    }

    /// Registers a process definition in the runtime.
    pub fn register_process<P: Process>(&mut self, process: P) -> Result<(), ProcessError> {
        let name = process.name();
        let raw_process = {
            let builder = ProcessBuilder::new(self.message_manager.clone());
            RawProcess::try_from(process.define(builder))?
        };
        self.registered_processes
            .insert(name.to_string(), raw_process);
        Ok(())
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
