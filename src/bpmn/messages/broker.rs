use dashmap::DashMap;
use std::sync::Arc;

use crate::{Process, ProcessName, Value};

use super::{Context, CorrelationKey, Message, Messages};

type Callback = dyn Fn(&MessageBroker, CorrelationKey, serde_json::Value, Context) -> Result<(), MessageError>
    + Send
    + Sync;
struct InnerRouter {
    messages: Messages,
    dynamic_send: Box<Callback>,
}

impl InnerRouter {
    fn new<P: Process>(process: P) -> Self {
        InnerRouter {
            messages: Messages::new(),
            dynamic_send: Box::new(move |broker, correlation_key, payload, context| {
                let value: P::Input =
                    serde_json::from_value(payload).map_err(|_| MessageError::InvalidType)?;
                broker.send_message(Message {
                    process: process.clone(),
                    payload: value,
                    correlation_key,
                    context,
                })
            }),
        }
    }
}

impl std::fmt::Debug for InnerRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerRouter")
            .field("messages", &self.messages)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
/// Registry that maps process types to process-local message stores.
pub struct MessageBroker(Arc<DashMap<ProcessName, InnerRouter>>);

impl MessageBroker {
    /// Creates an empty message manager.
    pub fn new() -> Self {
        MessageBroker(Arc::new(DashMap::new()))
    }

    /// Sends a message to the target process store.
    pub fn send_message<P: Process, V: Value>(
        &self,
        message: Message<P, V>,
    ) -> Result<(), MessageError> {
        match self.0.get(&ProcessName::from(message.process.metadata())) {
            Some(messages) => {
                messages.messages.send(message.metadata(), message.payload);
                Ok(())
            }
            None => Err(MessageError::NoTarget),
        }
    }

    /// Sends a message to the target process serialized as JSON.
    /// This is used for dynamic message sending where the process type and payload type are not known at compile time.
    pub fn send_message_dynamic(
        &self,
        process_name: ProcessName,
        correlation_key: CorrelationKey,
        payload: serde_json::Value,
        context: Context,
    ) -> Result<(), MessageError> {
        match self.0.get(&process_name) {
            Some(messages) => {
                messages.dynamic_send.as_ref()(self, correlation_key, payload, context)
            }
            None => Err(MessageError::NoTarget),
        }
    }

    /// Gets (or lazily creates) the message store for one process type.
    pub fn get_messages_for_process<P: Process>(&self, process: P) -> Messages {
        let process_name = ProcessName::from(process.metadata());
        match self.0.entry(process_name) {
            dashmap::Entry::Occupied(entry) => entry.get().messages.clone(),
            dashmap::Entry::Vacant(entry) => {
                let inner_router = InnerRouter::new(process);
                let messages = inner_router.messages.clone();
                entry.insert(inner_router);
                messages
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
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageError::InvalidType => {
                write!(f, "The message type does not match the expected type.")
            }
            MessageError::NoTarget => write!(f, "There is currently target for the message."),
        }
    }
}

impl std::error::Error for MessageError {}
