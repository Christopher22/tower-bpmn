use chrono::DateTime;
use dashmap::DashMap;
use std::{
    any::{Any, TypeId},
    ops::Deref,
    sync::Arc,
};
use tokio::sync::broadcast::{Receiver, Sender};
use uuid::Uuid;

use crate::Process;

use super::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct CorrelationKey(Uuid);

impl CorrelationKey {
    pub fn new() -> Self {
        CorrelationKey(Uuid::new_v4())
    }
}

/// A message to be sent to a process, which can be used for both sending messages to a waiting process and starting a new process instance with the message as input.
pub struct Message<P: Process, V: Value> {
    pub process: P,
    pub payload: V,
    pub correlation_key: CorrelationKey,
}

impl<P: Process, V: Value> Message<P, V> {
    pub fn new(process: P, payload: V) -> (Self, CorrelationKey) {
        let correlation_key = CorrelationKey::new();
        (
            Message {
                process,
                payload,
                correlation_key: correlation_key.clone(),
            },
            correlation_key,
        )
    }
}

#[derive(Debug)]
struct RawMessage {
    timestamp: DateTime<chrono::Utc>,
    value: Box<dyn Any + Send + Sync>,
}

impl RawMessage {
    pub fn new<T: Value>(value: T) -> Self {
        RawMessage {
            timestamp: chrono::Utc::now(),
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

struct Ref<'a, T: 'static>(
    dashmap::mapref::one::Ref<'a, CorrelationKey, RawMessage>,
    std::marker::PhantomData<T>,
);

impl<T: 'static> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
            .value
            .downcast_ref::<T>()
            .expect("Message value has wrong type")
    }
}

impl ProcessMessages {
    pub fn new() -> Self {
        ProcessMessages {
            sender: tokio::sync::broadcast::channel(100).0,
            data: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self) -> Receiver<CorrelationKey> {
        self.sender.subscribe()
    }

    pub fn create_sender(&self) -> Sender<CorrelationKey> {
        self.sender.clone()
    }

    pub fn send<T: Value>(&self, key: CorrelationKey, value: T) {
        self.data.insert(key, RawMessage::new(value));
        let _ = self.sender.send(key);
    }

    pub fn receive<T: Value>(&self, key: CorrelationKey) -> Option<Ref<T>> {
        self.data
            .get(&key)
            .map(|r| Ref(r, std::marker::PhantomData))
    }
}

#[derive(Debug, Clone)]
pub struct MessageManager(Arc<DashMap<TypeId, ProcessMessages>>);

impl MessageManager {
    pub fn new() -> Self {
        MessageManager(Arc::new(DashMap::new()))
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
