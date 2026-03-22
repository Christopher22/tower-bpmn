//! Message-based process interaction primitives for BPMN processes.

use chrono::DateTime;
use dashmap::DashMap;
use std::{any::Any, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};

use crate::{
    DynamicInput, DynamicValue, ExtendedExecutor, InstanceId, InstanceSpawnError, Instances,
    MetaData, Process, ProcessName, Step, StorageBackend, Value,
};

mod broker;
mod message;
mod participant;

pub use self::broker::{MessageBroker, MessageError};
pub use self::message::{CorrelationKey, Message};
pub use self::participant::{Context, Participant};

#[derive(Debug)]
struct RawMessage {
    _timestamp: DateTime<chrono::Utc>,
    value: Box<dyn Any + Send + Sync>,
}

impl RawMessage {
    pub fn new<T: Value>(value: T) -> Self {
        RawMessage {
            _timestamp: chrono::Utc::now(),
            value: Box::new(value),
        }
    }
}

impl From<DynamicValue> for RawMessage {
    fn from(value: DynamicValue) -> Self {
        RawMessage {
            _timestamp: chrono::Utc::now(),
            value: value.into(),
        }
    }
}

/// Metadata, associated with a message, which can be used for correlation and access control.
#[derive(Debug, Clone)]
pub struct MessageMetaData<C = CorrelationKey> {
    /// Correlation key for matching messages.
    pub correlation_key: C,
    /// Additional context for message-based access control.
    pub context: Context,
}

impl<C> MessageMetaData<C> {
    /// Creates new message metadata with the given correlation key and context.
    pub fn new(correlation_key: C, context: Context) -> Self {
        Self {
            correlation_key,
            context,
        }
    }
}

impl<C: Default> Default for MessageMetaData<C> {
    fn default() -> Self {
        Self::new(C::default(), Context::default())
    }
}

/// The callback for spawn a new process instance with a message, which is used for messages that start new process instances.
type SpawnCallback = dyn Fn(Box<dyn Any + 'static>) -> Result<InstanceId, InstanceSpawnError>
    + 'static
    + Send
    + Sync;

/// Messages for a single process.
#[derive(Clone)]
pub struct Messages {
    sender: Sender<MessageMetaData>,
    data: Arc<DashMap<CorrelationKey, RawMessage>>,
    dynamic_spawn: Option<(DynamicInput, Arc<SpawnCallback>)>,
    dynamic_send: Arc<DashMap<Step, DynamicInput>>,
}

impl Messages {
    /// Creates empty message storage for one process type.
    pub fn new() -> Self {
        Messages {
            sender: tokio::sync::broadcast::channel(100).0,
            data: Arc::new(DashMap::new()),
            dynamic_spawn: None,
            dynamic_send: Arc::new(DashMap::new()),
        }
    }

    /// Send a message without routing.
    pub fn send<S: Sendable>(&self, message: S) -> S::Result {
        message.send(self)
    }

    /// Subscribes to correlation-key notifications for newly sent messages.
    pub fn subscribe(&self) -> Receiver<MessageMetaData> {
        self.sender.subscribe()
    }

    /// Retrieves a typed message by key if present and of matching type.
    pub fn receive<T: Value>(&self, key: CorrelationKey) -> Option<T> {
        self.data
            .get(&key)
            .and_then(|entry| entry.value.downcast_ref::<T>().cloned())
    }

    /// Register a callback for sending dynamic input to waiting processes.
    pub(crate) fn register_input_for_step<V: Value>(&self, step: Step, owner: Participant) {
        self.dynamic_send
            .insert(step, DynamicInput::new::<V>(owner));
    }

    /// Register a callback for spawning new process instances with messages, which is used for messages that start new process instances.
    /// To ensure matching generic parameters, this should be called in the broker.
    pub(super) fn register_spawn<P: Process, E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
        &mut self,
        instances: Arc<Instances<E, B>>,
    ) {
        self.dynamic_spawn = Some((
            DynamicInput::for_process::<P>(),
            Arc::new(move |value| match value.downcast::<P::Input>() {
                Ok(input) => Ok(instances.run(*input)),
                Err(_) => Err(InstanceSpawnError::InvalidInput(
                    "Message payload type does not match process input type".to_string(),
                )),
            }),
        ));
    }

    fn send_raw(&self, meta_data: MessageMetaData, value: RawMessage) {
        self.data.insert(meta_data.correlation_key, value);
        let _ = self.sender.send(meta_data);
    }

    fn try_spawn(
        &self,
        context: &Context,
        callback: impl FnOnce(
            &DynamicInput,
            &Arc<SpawnCallback>,
        ) -> Result<InstanceId, InstanceSpawnError>,
    ) -> Result<InstanceId, InstanceSpawnError> {
        if let Some((input, instance_spawn)) = self.dynamic_spawn.as_ref() {
            if !context.is_suitable_for(&input.responsible) {
                return Err(InstanceSpawnError::InvalidContext);
            }
            callback(input, instance_spawn)
        } else {
            Err(InstanceSpawnError::InvalidContext)
        }
    }
}

impl Default for Messages {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Messages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Messages").finish_non_exhaustive()
    }
}

/// The target of a message.
pub trait Target {
    /// The name of the target process.
    fn target_process(&self) -> ProcessName;
}

impl<P: Process> Target for P {
    fn target_process(&self) -> ProcessName {
        ProcessName::from(self.metadata())
    }
}

impl Target for MetaData {
    fn target_process(&self) -> ProcessName {
        ProcessName::from(self)
    }
}

impl Target for ProcessName {
    fn target_process(&self) -> ProcessName {
        self.clone()
    }
}

impl<T: Target> Target for (T, Step) {
    fn target_process(&self) -> ProcessName {
        self.0.target_process()
    }
}

/// A item which is sendable.
pub trait Sendable {
    /// The result type of sending the message.
    type Result;

    /// Send the message.
    fn send(self, messages: &Messages) -> Self::Result;
}

/// A message which could be send with routing in
pub trait SendableWithTarget: Sendable {
    /// The target process of the message.
    fn target(&self) -> ProcessName;
}

/// A message which could be send with a fixed target process, which is used for messages with a statically known target process type, e.g., messages sent to waiting processes.
/// This can be used to ensure that the message is only sent to the intended process type.
pub trait SendableWithFixedTarget<P: Process>: SendableWithTarget {}

// ------------------
// Everything for spawning new processes
// ------------------

impl Sendable for DynamicValue {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn send(self, messages: &Messages) -> Self::Result {
        messages.try_spawn(&Context::default(), |_, spawner| spawner(self.to_box()))
    }
}

impl Sendable for serde_json::Value {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn send(self, messages: &Messages) -> Self::Result {
        messages.try_spawn(&Context::default(), |dynamic_input, spawner| {
            let value = dynamic_input.cast(self).map_err(|_| {
                InstanceSpawnError::InvalidInput(
                    "Failed to cast JSON value to process input".to_string(),
                )
            })?;
            spawner(value.to_box())
        })
    }
}

impl<P: Process> Sendable for Message<P, P::Input, ()> {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn send(self, messages: &Messages) -> Self::Result {
        messages.try_spawn(&self.context, |_, spawner| spawner(Box::new(self.payload)))
    }
}

impl Sendable for Message<ProcessName, DynamicValue, ()> {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn send(self, messages: &Messages) -> Self::Result {
        messages.try_spawn(&self.context, |_, spawner| spawner(self.payload.to_box()))
    }
}

impl Sendable for Message<ProcessName, serde_json::Value, ()> {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn send(self, messages: &Messages) -> Self::Result {
        messages.try_spawn(&self.context, |dynamic_input, spawner| {
            let value = dynamic_input.cast(self.payload).map_err(|_| {
                InstanceSpawnError::InvalidInput(
                    "Failed to cast JSON value to process input".to_string(),
                )
            })?;
            spawner(value.to_box())
        })
    }
}

// -------------
// Everything to send messages to waiting processes.
// -------------
impl<P: Process, V: Value> Sendable for Message<P, V, CorrelationKey> {
    type Result = ();

    fn send(self, messages: &Messages) -> Self::Result {
        let (meta_data, value) = self.split();
        messages.send_raw(meta_data, RawMessage::new(value));
    }
}

impl Sendable for Message<Step, serde_json::Value, CorrelationKey> {
    type Result = Result<(), MessageError>;

    fn send(self, messages: &Messages) -> Self::Result {
        let dynamic_input = messages
            .dynamic_send
            .get(&self.target)
            .ok_or(MessageError::NoTarget)?;

        if self.context.is_suitable_for(&dynamic_input.responsible) {
            return Err(MessageError::Forbidden);
        }

        let (meta_data, value) = self.split();
        let input = dynamic_input
            .cast(value)
            .map_err(|_| MessageError::InvalidType)?;

        messages.send_raw(meta_data, input.into());
        Ok(())
    }
}

impl<T: Target> Sendable for Message<(T, Step), serde_json::Value, CorrelationKey> {
    type Result = Result<(), MessageError>;

    fn send(self, messages: &Messages) -> Self::Result {
        self.map(|target| target.1).send(messages)
    }
}

impl<P: Target, V, C> SendableWithTarget for Message<P, V, C>
where
    Message<P, V, C>: Sendable,
{
    fn target(&self) -> ProcessName {
        self.target.target_process()
    }
}

impl<P: Process, V, C> SendableWithFixedTarget<P> for Message<P, V, C> where
    Message<P, V, C>: SendableWithTarget
{
}
