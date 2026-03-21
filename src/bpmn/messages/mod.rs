//! Message-based process interaction primitives for BPMN processes.

use chrono::DateTime;
use dashmap::DashMap;
use std::{any::Any, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};

use crate::{
    ExtendedExecutor, InstanceId, InstanceSpawnError, Instances, Process, StorageBackend, Value,
};

mod broker;
mod message;
mod participant;

pub use self::broker::{MessageBroker, MessageError};
pub use self::message::{CorrelationKey, GuardedCorrelationKey, Message};
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
    instance_spawn: Option<Arc<SpawnCallback>>,
}

impl Messages {
    /// Creates empty message storage for one process type.
    pub fn new() -> Self {
        Messages {
            sender: tokio::sync::broadcast::channel(100).0,
            data: Arc::new(DashMap::new()),
            instance_spawn: None,
        }
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

    /// Register a callback for spawning new process instances with messages, which is used for messages that start new process instances.
    /// To ensure matching generic parameters, this should be called in the broker.
    pub(super) fn register_spawn<P: Process, E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
        &mut self,
        instances: Arc<Instances<E, B>>,
    ) {
        self.instance_spawn = Some(Arc::new(move |value| match value.downcast::<P::Input>() {
            Ok(input) => Ok(instances.run(*input)),
            Err(_) => Err(InstanceSpawnError::InvalidInput(
                "Message payload type does not match process input type".to_string(),
            )),
        }));
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

/// A message which could be send.
pub trait SendableMessage<P: Process> {
    /// The result of trying to send the message.
    type Result;

    /// The target process of the message.
    fn process(&self) -> &P;

    /// Send the message.
    fn send(self, messages: &Messages) -> Self::Result;
}

impl<P: Process, V: Value> SendableMessage<P> for Message<P, V, CorrelationKey> {
    type Result = ();

    fn process(&self) -> &P {
        &self.process
    }

    fn send(self, messages: &Messages) -> Self::Result {
        let (meta_data, value) = self.split();
        messages
            .data
            .insert(meta_data.correlation_key, RawMessage::new(value));
        let _ = messages.sender.send(meta_data);
    }
}

impl<P: Process> SendableMessage<P> for Message<P, P::Input, ()> {
    type Result = Result<InstanceId, InstanceSpawnError>;

    fn process(&self) -> &P {
        &self.process
    }

    fn send(self, messages: &Messages) -> Self::Result {
        if let Some(value) = messages.instance_spawn.as_ref()
            && self.context.is_suitable_for(&P::INITIAL_OWNER)
        {
            (value)(Box::new(self.payload))
        } else {
            Err(InstanceSpawnError::InvalidContext)
        }
    }
}
