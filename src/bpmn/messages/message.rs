use uuid::Uuid;

use super::Context;
use crate::{
    Process, Value,
    messages::{MessageMetaData, Participant},
};

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

/// A correlation key with an associated guard, which accepts messages only if the guard matches the message context.
/// This can be used to implement message-based access control, e.g., for assigning tasks to specific participants and only accepting messages from those participants.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct GuardedCorrelationKey {
    /// Correlation key for matching messages.
    pub key: CorrelationKey,
    /// Expected sender of the message, used for access control.
    pub expected_sender: Participant,
}

/// A message to be sent to a process, which can be used for both sending messages to a waiting process and starting a new process instance with the message as input.
#[derive(Debug)]
pub struct Message<P: Process, V: Value, C = CorrelationKey> {
    /// Target process type.
    pub process: P,
    /// Typed payload.
    pub payload: V,
    /// Correlation key used for message matching.
    pub correlation_key: C,
    /// The context of the message.
    pub context: Context,
}

impl<P: Process, V: Value, C> Message<P, V, C> {
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
}

impl<P: Process> Message<P, P::Input, ()> {
    /// Creates a new message for starting a process instance with the given payload.
    pub fn for_starting(process: P, payload: P::Input) -> Option<Self> {
        let context = Context::new_matching(P::INITIAL_OWNER)?;
        Some(Message {
            process,
            payload,
            correlation_key: (),
            context,
        })
    }
}

impl<P: Process, V: Value> Message<P, V, CorrelationKey> {
    /// Creates a new message with a random correlation key and default context.
    pub fn new(process: P, payload: V) -> Self {
        Message {
            process,
            payload,
            correlation_key: CorrelationKey::new(),
            context: Context::default(),
        }
    }

    /// Creates a new message with the given correlation key and default context.
    pub fn with_key(process: P, payload: V, correlation_key: CorrelationKey) -> Self {
        Message {
            process,
            payload,
            correlation_key,
            context: Context::default(),
        }
    }
}
