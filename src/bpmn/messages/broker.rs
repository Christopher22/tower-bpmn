use dashmap::DashMap;
use std::sync::Arc;

use crate::{ExtendedExecutor, Instances, Process, ProcessName, StorageBackend};

use super::{Messages, SendableWithTarget};

#[derive(Debug, Clone)]
/// Registry that maps process types to process-local message stores.
pub struct MessageBroker(Arc<DashMap<ProcessName, Messages>>);

impl MessageBroker {
    /// Creates an empty message manager.
    pub fn new() -> Self {
        MessageBroker(Arc::new(DashMap::new()))
    }

    /// Sends a message to the target process store.
    pub fn send<S: SendableWithTarget>(&self, message: S) -> Result<S::Result, MessageError> {
        match self.0.get(&message.target()) {
            Some(messages) => Ok(message.send(&messages)),
            None => Err(MessageError::NoTarget),
        }
    }

    /// Gets (or lazily creates) the message store for one process type.
    pub fn get_messages_for_process<P: Process>(&self, process: P) -> Messages {
        let process_name = ProcessName::from(process.metadata());
        match self.0.entry(process_name) {
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::Entry::Vacant(entry) => {
                let messages = Messages::new();
                entry.insert(messages.clone());
                messages
            }
        }
    }

    /// Registers a process to be spawned.
    pub fn register_spawn<P: Process, E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
        &self,
        process: &P,
        instances: Arc<Instances<E, B>>,
    ) {
        let process_name = ProcessName::from(process.metadata());
        match self.0.entry(process_name) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().register_spawn::<P, E, B>(instances);
            }
            dashmap::Entry::Vacant(entry) => {
                let mut messages = Messages::new();
                messages.register_spawn::<P, E, B>(instances);
                entry.insert(messages);
            }
        }
    }
}

impl Default for MessageBroker {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors produced while sending process messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageError {
    /// The message type does not match the expected type. This could only happen when sending messages dynamically with JSON payload, and the payload cannot be deserialized into the expected type.
    InvalidType,
    /// There is currently target for the message.
    NoTarget,
    /// The context does not match.
    Forbidden,
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageError::InvalidType => {
                write!(f, "the message type does not match the expected type")
            }
            MessageError::NoTarget => write!(f, "there is currently target for the message"),
            MessageError::Forbidden => write!(f, "the context does not match."),
        }
    }
}

impl std::error::Error for MessageError {}
