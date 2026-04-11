mod in_memory;
mod sqlite;

use crate::{
    InstanceId, ProcessName, RegisteredProcess, State, Step, Token, TokenId, Value,
    petri_net::{Id, Place},
};

pub use self::in_memory::{InMemory, InMemoryStorage};
pub use self::sqlite::{Sqlite, SqliteError, SqliteStorage};

/// A backend suitable for storing the data of instances.
pub trait StorageBackend: 'static + Clone + Sized + Send + Sync {
    /// The type of storage used for instances.
    type Storage: Storage;

    /// Register a new instance.
    fn new_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Self::Storage;

    /// Resume an instance by its ID, returning the values of the PetriNet if the instance is found and belongs to the given process.
    fn resume_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Result<ResumableProcess<Self>, ResumeError>;

    /// Yield a list of all unfinished instances which could be resumed, along with the name of the process they belong to.
    fn unfinished_instances(&self) -> Vec<(ProcessName, InstanceId)>;
}

/// A serialized marking which could be used to resume an instance.
pub(super) type SerializedMarking<B> = Vec<(Id<Place<State<B>>>, Token<B>)>;

/// A process which could be resumed.
#[derive(Debug)]
pub struct ResumableProcess<B: StorageBackend> {
    pub(super) id: InstanceId,
    pub(super) current_state: SerializedMarking<B::Storage>,
    pub(super) storage: B::Storage,
}

/// A storage for a process instance, which can be used to store token values and other data related to the instance. This is shared between all tokens in the same process instance, allowing them to see each other's values according to their branching history.
pub trait Storage: 'static + std::fmt::Debug + Clone + Send + Sync + Eq {
    /// Add a new entry to the storage for the given token ID, place, and value.
    fn add<V: Value>(&self, token_id: TokenId, place: Step, value: V);

    /// Return all the states currently active.
    fn current_places(&self) -> Vec<Step>;

    /// Returns the most recent value of type `T`.
    fn get_last<T: Value>(&self, token_ids: &[TokenId]) -> Option<T>;

    /// Returns the name of the last step finished by any of the given token.
    fn last_step(&self, token_ids: &[TokenId]) -> Option<Step>;
}

/// Errors that can occur when trying to resume a paused instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeError {
    /// No instance with the given ID was found.
    NotFound,
    /// An instance with the given ID was found, but it belongs to a different process.
    ProcessMismatch,
}

impl std::fmt::Display for ResumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResumeError::NotFound => write!(f, "Instance not found"),
            ResumeError::ProcessMismatch => write!(f, "Instance belongs to a different process"),
        }
    }
}

impl std::error::Error for ResumeError {}
