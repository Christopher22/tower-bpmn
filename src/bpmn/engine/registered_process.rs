use std::{any::TypeId, sync::Arc};

use serde_json::Value as JsonValue;

use crate::{
    CorrelationKey, ExtendedExecutor, InstanceError, InstanceId, Message, MetaData, Process,
    ProcessBuilder, ProcessError, Runtime, SendError, SharedHistory, State, Step, Token, Value,
    petri_net::{FirstCompetingStrategy, PetriNet, Simulation},
};

/// A registered process definition.
#[derive(Debug)]
pub struct RegisteredProcess<E: ExtendedExecutor> {
    /// Meta data of the registered process.
    pub meta_data: MetaData,
    /// Schema of the value which could be passed to this process to start it.
    pub input_schema: JsonValue,
    pub(crate) petri_net: Arc<PetriNet<Step, State>>,
    pub(crate) start: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) end: crate::petri_net::Id<crate::petri_net::Place<State>>,
    pub(crate) process_type: TypeId,
    pub(crate) input_type: TypeId,
    dynamic_api: DynamicCaller<E>,
}

impl<E: ExtendedExecutor> RegisteredProcess<E> {
    /// Create a new raw process from a PetriNet representation and type information.
    fn new<P: Process>(
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
            input_schema: serde_json::to_value(schemars::schema_for!(P::Input))
                .expect("failed to serialize process input schema"),
            dynamic_api,
        }
    }

    /// Create a new simulation for this process with the given input. The simulation will be initialized with the input token at the start place of the process.
    pub(crate) fn instantiate<A: ExtendedExecutor, V: Value>(
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
        let token = Token::new(shared_storage).set_output(crate::bpmn::START_NAME, input);
        let mut simulation =
            Simulation::new(executor, self.petri_net.clone(), FirstCompetingStrategy);
        simulation[self.start] = State::Completed(token);
        simulation
    }

    /// Get the end place of the process, which is used for querying the final state of the process instance.
    pub(crate) fn end_place(&self) -> &crate::petri_net::Place<State> {
        &self.petri_net[self.end]
    }

    /// Start the process by its name.
    pub(crate) fn run_dynamic(
        &self,
        runtime: &Runtime<E>,
        input: JsonValue,
    ) -> Result<InstanceId, RuntimeApiError> {
        (self.dynamic_api.start)(runtime, input)
    }

    /// Send a dynamic message.
    pub(crate) fn send_message_dynamic(
        &self,
        runtime: &Runtime<E>,
        correlation_key: CorrelationKey,
        payload: JsonValue,
    ) -> Result<(), RuntimeApiError> {
        (self.dynamic_api.send_message)(runtime, correlation_key, payload)
    }
}

impl<P, V, E> TryFrom<(ProcessBuilder<P, V>, DynamicCaller<E>)> for RegisteredProcess<E>
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

impl<E: ExtendedExecutor> std::fmt::Display for RegisteredProcess<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.meta_data.name)
    }
}

type ApiStart<E> =
    Arc<dyn Fn(&Runtime<E>, JsonValue) -> Result<InstanceId, RuntimeApiError> + Send + Sync>;
type ApiSendMessage<E> = Arc<
    dyn Fn(&Runtime<E>, CorrelationKey, JsonValue) -> Result<(), RuntimeApiError> + Send + Sync,
>;

pub(super) struct DynamicCaller<E: ExtendedExecutor> {
    start: ApiStart<E>,
    send_message: ApiSendMessage<E>,
}

impl<E: ExtendedExecutor> DynamicCaller<E> {
    pub fn new<P: Process + Clone + Send + Sync>(
        process_for_start: P,
        process_for_message: P,
    ) -> Self {
        Self {
            start: Arc::new(move |runtime: &Runtime<E>, input: JsonValue| {
                let value: P::Input = serde_json::from_value(input)
                    .map_err(|err| RuntimeApiError::InvalidPayload(err.to_string()))?;
                runtime
                    .run(process_for_start.clone(), value)
                    .map(|instance| instance.id)
                    .map_err(RuntimeApiError::Instance)
            }),
            send_message: Arc::new(
                move |runtime: &Runtime<E>, correlation_key: CorrelationKey, payload: JsonValue| {
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
        }
    }
}

impl<E: ExtendedExecutor> std::fmt::Debug for DynamicCaller<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCaller").finish_non_exhaustive()
    }
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
