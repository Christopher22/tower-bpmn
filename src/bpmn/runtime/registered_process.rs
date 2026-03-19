use std::{any::TypeId, sync::Arc};

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::{
    BpmnStep, ExtendedExecutor, InstanceId, InstanceSpawnError, MetaData, Process, ProcessBuilder,
    ProcessError, ProcessName, Runtime, State, Steps, StorageBackend, Token, Value,
    petri_net::{FirstCompetingStrategy, Id, PetriNet, Place, Simulation},
};

type PetriNetRef<S> = Arc<PetriNet<BpmnStep<S>, State<S>>>;

/// A registered process definition.
#[derive(Debug, Serialize)]
pub struct RegisteredProcess<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    /// Meta data of the registered process.
    pub meta_data: MetaData,
    /// Schema of the value which could be passed to this process to start it.
    pub input_schema: JsonValue,
    /// The steps of the process.
    pub steps: Steps,
    #[serde(skip)]
    pub(crate) start: Id<Place<State<B::Storage>>>,
    #[serde(skip)]
    pub(crate) end: Id<Place<State<B::Storage>>>,
    #[serde(skip)]
    pub(crate) process_type: TypeId,
    #[serde(skip)]
    pub(crate) input_type: TypeId,
    #[serde(skip)]
    dynamic_api: DynamicCaller<E, B>,
    #[serde(skip)]
    petri_net: PetriNetRef<B::Storage>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> RegisteredProcess<E, B> {
    /// Create a new raw process from a PetriNet representation and type information.
    fn new<P: Process>(
        meta_data: MetaData,
        petri_net: PetriNetRef<B::Storage>,
        start_place: Id<Place<State<B::Storage>>>,
        current_place: Id<Place<State<B::Storage>>>,
        dynamic_api: DynamicCaller<E, B>,
        steps: Steps,
    ) -> Self {
        Self {
            meta_data,
            petri_net,
            start: start_place,
            end: current_place,
            process_type: TypeId::of::<P>(),
            input_type: TypeId::of::<P::Input>(),
            input_schema: serde_json::to_value(schemars::schema_for!(P::Input))
                .expect("failed to serialize process input schema"),
            dynamic_api,
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
        assert_eq!(
            self.input_type,
            TypeId::of::<V>(),
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

    /// Start the process by its name.
    pub(crate) fn run_dynamic(
        &self,
        runtime: &Runtime<E, B>,
        input: JsonValue,
    ) -> Result<InstanceId, InstanceSpawnError> {
        (self.dynamic_api.0)(runtime, input)
    }
}

impl<P, V, E, B> TryFrom<(ProcessBuilder<P, V, B::Storage>, DynamicCaller<E, B>)>
    for RegisteredProcess<E, B>
where
    P: Process,
    V: Value,
    E: ExtendedExecutor<B::Storage>,
    B: StorageBackend,
{
    type Error = ProcessError;
    fn try_from(
        (builder, dynamic_api): (ProcessBuilder<P, V, B::Storage>, DynamicCaller<E, B>),
    ) -> Result<Self, Self::Error> {
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
            dynamic_api,
            steps,
        ))
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Display
    for RegisteredProcess<E, B>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", ProcessName::from(&self.meta_data))
    }
}

type ApiStart<E, B> =
    Box<dyn Fn(&Runtime<E, B>, JsonValue) -> Result<InstanceId, InstanceSpawnError> + Send + Sync>;

pub(super) struct DynamicCaller<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(ApiStart<E, B>);

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> DynamicCaller<E, B> {
    pub fn new<P: Process + Clone + Send + Sync>(process_for_start: P) -> Self {
        Self(Box::new(
            move |runtime: &Runtime<E, B>, input: JsonValue| {
                let value: P::Input = serde_json::from_value(input)
                    .map_err(|err| InstanceSpawnError::InvalidInput(err.to_string()))?;
                runtime.run(process_for_start.clone(), value)
            },
        ))
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for DynamicCaller<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCaller").finish_non_exhaustive()
    }
}
