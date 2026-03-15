//! Message-based process interaction primitives for BPMN processes.

use chrono::DateTime;
use dashmap::DashMap;
use std::{any::Any, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};

use crate::Value;

mod broker;
mod message;
mod participant;

pub use self::broker::{MessageBroker, MessageError};
pub use self::message::{CorrelationKey, Message};
pub use self::participant::Participant;

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
pub struct Messages {
    sender: Sender<CorrelationKey>,
    data: Arc<DashMap<CorrelationKey, RawMessage>>,
}

impl Messages {
    /// Creates empty message storage for one process type.
    pub fn new() -> Self {
        Messages {
            sender: tokio::sync::broadcast::channel(100).0,
            data: Arc::new(DashMap::new()),
        }
    }

    /// Subscribes to correlation-key notifications for newly sent messages.
    pub fn subscribe(&self) -> Receiver<CorrelationKey> {
        self.sender.subscribe()
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

impl Default for Messages {
    fn default() -> Self {
        Self::new()
    }
}
