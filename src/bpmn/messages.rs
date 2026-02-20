use chrono::DateTime;
use dashmap::DashMap;
use std::{
    any::{Any, TypeId},
    sync::Arc,
};
use tokio::sync::broadcast::{Receiver, Sender};
use uuid::Uuid;

use crate::Process;

use super::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
/// Correlation identifier for message-based process interaction.
pub struct CorrelationKey(Uuid);

impl CorrelationKey {
    /// Creates a new random correlation key.
    pub fn new() -> Self {
        CorrelationKey(Uuid::new_v4())
    }
}

impl Default for CorrelationKey {
    fn default() -> Self {
        Self::new()
    }
}

/// A message to be sent to a process, which can be used for both sending messages to a waiting process and starting a new process instance with the message as input.
#[derive(Debug)]
pub struct Message<P: Process, V: Value> {
    /// Target process type.
    pub process: P,
    /// Typed payload.
    pub payload: V,
    /// Correlation key used for message matching.
    pub correlation_key: CorrelationKey,
}

impl<P: Process, V: Value> Message<P, V> {
    /// Creates a new message and returns it with its generated correlation key.
    pub fn new(process: P, payload: V) -> (Self, CorrelationKey) {
        let correlation_key = CorrelationKey::new();
        (
            Message {
                process,
                payload,
                correlation_key,
            },
            correlation_key,
        )
    }
}

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

/// Messages for a single process.
#[derive(Debug, Clone)]
pub struct ProcessMessages {
    sender: Sender<CorrelationKey>,
    data: Arc<DashMap<CorrelationKey, RawMessage>>,
}

impl ProcessMessages {
    /// Creates empty message storage for one process type.
    pub fn new() -> Self {
        ProcessMessages {
            sender: tokio::sync::broadcast::channel(100).0,
            data: Arc::new(DashMap::new()),
        }
    }

    /// Subscribes to correlation-key notifications for newly sent messages.
    pub fn subscribe(&self) -> Receiver<CorrelationKey> {
        self.sender.subscribe()
    }

    /// Clones the underlying broadcast sender.
    pub fn create_sender(&self) -> Sender<CorrelationKey> {
        self.sender.clone()
    }

    /// Stores and broadcasts a typed message for the given key.
    pub fn send<T: Value>(&self, key: CorrelationKey, value: T) {
        self.data.insert(key, RawMessage::new(value));
        let _ = self.sender.send(key);
    }

    /// Retrieves a typed message by key if present and of matching type.
    pub fn receive<T: Value>(&self, key: CorrelationKey) -> Option<T> {
        self.data
            .get(&key)
            .and_then(|entry| entry.value.downcast_ref::<T>().cloned())
    }
}

impl Default for ProcessMessages {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
/// Registry that maps process types to process-local message stores.
pub struct MessageManager(Arc<DashMap<TypeId, ProcessMessages>>);

impl MessageManager {
    /// Creates an empty message manager.
    pub fn new() -> Self {
        MessageManager(Arc::new(DashMap::new()))
    }

    /// Sends a message to the target process store.
    pub fn send_message<P: Process, V: Value>(
        &self,
        message: Message<P, V>,
    ) -> Result<(), SendError> {
        match self.0.get(&TypeId::of::<P>()) {
            Some(messages) => {
                messages.send(message.correlation_key, message.payload);
                Ok(())
            }
            None => Err(SendError::NoTarget),
        }
    }

    /// Gets (or lazily creates) the message store for one process type.
    pub fn get_messages_for_process<P: Process>(&self) -> ProcessMessages {
        let type_id = TypeId::of::<P>();
        match self.0.entry(type_id) {
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::Entry::Vacant(entry) => {
                let messages = ProcessMessages::new();
                entry.insert(messages.clone());
                messages
            }
        }
    }
}

impl Default for MessageManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors produced while sending process messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SendError {
    /// The message type does not match the expected type.
    InvalidType,
    /// There is currently target for the message.
    NoTarget,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::InvalidType => {
                write!(f, "The message type does not match the expected type.")
            }
            SendError::NoTarget => write!(f, "There is currently target for the message."),
        }
    }
}

impl std::error::Error for SendError {}
