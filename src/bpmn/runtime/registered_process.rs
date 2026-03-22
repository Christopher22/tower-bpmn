use std::{any::TypeId, sync::Arc};

use serde::Serialize;

use crate::{
    BpmnStep, ExtendedExecutor, MetaData, Process, ProcessBuilder, ProcessError, ProcessName,
    State, Steps, StorageBackend, Token, Value,
    bpmn::runtime::DynamicInput,
    petri_net::{FirstCompetingStrategy, Id, PetriNet, Place, Simulation},
};

type PetriNetRef<S> = Arc<PetriNet<BpmnStep<S>, State<S>>>;

/// A registered process definition.
#[derive(Debug, Serialize)]
pub struct RegisteredProcess<B: StorageBackend> {
    /// Meta data of the registered process.
    pub meta_data: MetaData,
    /// The steps of the process.
    pub steps: Steps,
    /// The (dynamic) types which could be used to start this process.
    #[serde(skip)]
    pub input: DynamicInput,
    #[serde(skip)]
    pub(crate) start: Id<Place<State<B::Storage>>>,
    #[serde(skip)]
    pub(crate) end: Id<Place<State<B::Storage>>>,
    #[serde(skip)]
    pub(crate) process_type: TypeId,
    #[serde(skip)]
    petri_net: PetriNetRef<B::Storage>,
}

impl<B: StorageBackend> RegisteredProcess<B> {
    /// Create a new raw process from a PetriNet representation and type information.
    fn new<P: Process>(
        meta_data: MetaData,
        petri_net: PetriNetRef<B::Storage>,
        start_place: Id<Place<State<B::Storage>>>,
        current_place: Id<Place<State<B::Storage>>>,
        steps: Steps,
    ) -> Self {
        Self {
            meta_data,
            petri_net,
            start: start_place,
            end: current_place,
            process_type: TypeId::of::<P>(),
            input: DynamicInput::for_process::<P>(),
            steps,
        }
    }

    /// Checks if the process matches the given type.
    pub fn matches<P: Process>(&self, process: &P) -> bool {
        self.process_type == TypeId::of::<P>() && &self.meta_data == process.metadata()
    }

    /// Create a new simulation for this process with the given input.
    /// The simulation will be initialized with the input token at the start place of the process.
    pub(crate) fn start<A: ExtendedExecutor<B::Storage>, V: Value>(
        &self,
        executor: A,
        input: V,
        shared_storage: B::Storage,
    ) -> Simulation<A, FirstCompetingStrategy, BpmnStep<B::Storage>, State<B::Storage>> {
        assert!(
            self.input.matches::<V>(),
            "The input type does not match the expected type for this process"
        );
        let token = Token::new(shared_storage).set_output(self.steps.start(), input);
        let mut simulation =
            Simulation::new(executor, self.petri_net.clone(), FirstCompetingStrategy);
        simulation[self.start] = State::Completed(token);
        simulation
    }

    /// Resume a simulation.
    pub(crate) fn resume<A: ExtendedExecutor<B::Storage>>(
        &self,
        executor: A,
        serialized_storage: super::storage::SerializedMarking<B::Storage>,
    ) -> Simulation<A, FirstCompetingStrategy, BpmnStep<B::Storage>, State<B::Storage>> {
        let mut simulation =
            Simulation::new(executor, self.petri_net.clone(), FirstCompetingStrategy);
        for entry in serialized_storage {
            simulation[entry.0] = State::Completed(entry.1);
        }
        simulation
    }
}

impl<P, V, B> TryFrom<ProcessBuilder<P, V, B::Storage>> for RegisteredProcess<B>
where
    P: Process,
    V: Value,
    B: StorageBackend,
{
    type Error = ProcessError;
    fn try_from(builder: ProcessBuilder<P, V, B::Storage>) -> Result<Self, Self::Error> {
        // Finalize the process by adding an end place.
        let mut builder = builder.add_end();

        let steps = builder.steps.build().unwrap();
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
            steps,
        ))
    }
}

impl<B: StorageBackend> std::fmt::Display for RegisteredProcess<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", ProcessName::from(&self.meta_data))
    }
}
