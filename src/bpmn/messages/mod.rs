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
pub struct MessageMetaData {
    /// Correlation key for matching messages.
    pub correlation_key: CorrelationKey,
    /// Additional context for message-based access control.
    pub context: Context,
}

impl MessageMetaData {
    /// Creates new message metadata with the given correlation key and context.
    pub fn new(correlation_key: CorrelationKey, context: Context) -> Self {
        Self {
            correlation_key,
            context,
        }
    }
}

impl Default for MessageMetaData {
    fn default() -> Self {
        Self::new(CorrelationKey::new(), Context::default())
    }
}

/// Messages for a single process.
#[derive(Debug, Clone)]
pub struct Messages {
    sender: Sender<MessageMetaData>,
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
    pub fn subscribe(&self) -> Receiver<MessageMetaData> {
        self.sender.subscribe()
    }

    /// Stores and broadcasts a typed message for the given key.
    pub fn send<T: Value>(&self, meta_data: MessageMetaData, value: T) {
        self.data
            .insert(meta_data.correlation_key, RawMessage::new(value));
        let _ = self.sender.send(meta_data);
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
