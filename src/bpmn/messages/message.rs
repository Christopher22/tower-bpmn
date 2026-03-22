use uuid::Uuid;

use super::Context;
use crate::{Process, ProcessName, Step, Value, messages::MessageMetaData};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
)]
/// Correlation identifier for message-based process interaction.
pub struct CorrelationKey(Uuid);

impl CorrelationKey {
    /// Creates a new random correlation key.
    pub fn new() -> Self {
        CorrelationKey(Uuid::new_v4())
    }
}

impl std::fmt::Display for CorrelationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for CorrelationKey {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl Default for CorrelationKey {
    fn default() -> Self {
        Self::new()
    }
}

/// A message to be sent to a process, which can be used for both sending messages to a waiting process and starting a new process instance with the message as input.
#[derive(Debug)]
pub struct Message<T, V, C = CorrelationKey> {
    /// The target of the process.
    pub target: T,
    /// Typed payload.
    pub payload: V,
    /// Correlation key used for message matching.
    pub correlation_key: C,
    /// The context of the message.
    pub context: Context,
}

impl<T, V, C> Message<T, V, C> {
    /// Split metadata and payload of the message.
    pub fn split(self) -> (MessageMetaData<C>, V) {
        (
            MessageMetaData {
                correlation_key: self.correlation_key,
                context: self.context,
            },
            self.payload,
        )
    }

    /// Change the target of the message, while keeping the payload and metadata unchanged.
    pub fn map<T2>(self, callback: impl FnOnce(T) -> T2) -> Message<T2, V, C> {
        Message {
            target: callback(self.target),
            payload: self.payload,
            correlation_key: self.correlation_key,
            context: self.context,
        }
    }
}

impl<P: Process> Message<P, P::Input, ()> {
    /// Creates a new message for starting a process instance with the given payload.
    pub fn for_starting(process: P, payload: P::Input) -> Option<Self> {
        let context = Context::new_matching(P::INITIAL_OWNER)?;
        Some(Message {
            target: process,
            payload,
            correlation_key: (),
            context,
        })
    }
}

impl Message<ProcessName, serde_json::Value, ()> {
    /// Creates a new message for starting a process instance with the given payload.
    pub fn for_dynamic_starting(process_name: ProcessName, payload: serde_json::Value) -> Self {
        Message {
            target: process_name,
            payload,
            correlation_key: (),
            context: Context::default(),
        }
    }
}

impl Message<(ProcessName, Step), serde_json::Value, CorrelationKey> {
    /// Create a message for a waiting step.
    pub fn for_waiting_step(
        process_name: ProcessName,
        step: Step,
        payload: serde_json::Value,
        correlation_key: CorrelationKey,
    ) -> Self {
        Self {
            target: (process_name, step),
            payload,
            correlation_key,
            context: Context::default(),
        }
    }
}

impl<T, V: Value> Message<T, V, CorrelationKey> {
    /// Creates a new message with a random correlation key and default context.
    pub fn new(target: T, payload: V) -> Self {
        Message {
            target,
            payload,
            correlation_key: CorrelationKey::new(),
            context: Context::default(),
        }
    }

    /// Creates a new message with the given correlation key and default context.
    pub fn with_key(target: T, payload: V, correlation_key: CorrelationKey) -> Self {
        Message {
            target,
            payload,
            correlation_key,
            context: Context::default(),
        }
    }
}
