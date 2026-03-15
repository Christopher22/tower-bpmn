use uuid::Uuid;

use crate::{Process, Value};

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
